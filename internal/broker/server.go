package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"telemetry-pipeline/internal/config"
	"telemetry-pipeline/internal/log"
	"telemetry-pipeline/internal/metadata"
	"telemetry-pipeline/internal/replication"
	"telemetry-pipeline/proto"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type Partition struct {
	logger *zap.Logger
	Log    *log.PartitionLog

	// NextOffset  int64
	LeaderEpoch int64
	ISR         map[string]string // brokerID->addr
	Rep         *replication.Replicator
	mu          sync.RWMutex
}

type Server struct {
	proto.UnimplementedMQServer
	proto.UnimplementedReplicatorServer

	cfg config.Config
	// store   *metadata.Store
	store   metadata.StoreAPI
	brokers map[string]string
	parts   map[string]*Partition // key: topic-part
	partsMu sync.RWMutex          // protect parts map (create-once)
	logger  *zap.Logger

	GRPCLis net.Listener
	GRPCSrv *grpc.Server
	httpSrv *http.Server
}

func NewServer(cfg config.Config, z *zap.Logger, etcd *clientv3.Client) *Server {
	return &Server{cfg: cfg, store: metadata.NewStore(etcd), brokers: map[string]string{}, parts: map[string]*Partition{}, logger: z}
}

func (s *Server) Start(ctx context.Context) error {
	// Register to etcd with TTL
	if err := s.store.RegisterBroker(ctx, s.cfg.Server.NodeID, s.cfg.Server.GRPCAdvertisedAddr, 10); err != nil {
		s.logger.Error("error in register broker", zap.Any("error", err))
		return err
	}
	s.logger.Debug("broker registered successfully", zap.Any("nodeid:", s.cfg.Server.NodeID), zap.Any("address", s.cfg.Server.GRPCAdvertisedAddr))
	bs, err := s.store.ListBrokers(ctx)
	if err != nil {
		s.logger.Error("error in List broker", zap.Any("error", err))
		return err
	}
	s.brokers = bs
	// Build gRPC server
	var opts []grpc.ServerOption
	if tlsCfg, _ := s.cfg.Etcd.TLS.GRPCCredentials(); tlsCfg != nil {
		creds := credentials.NewTLS(tlsCfg)
		opts = append(opts, grpc.Creds(creds))
	}
	grpcSrv := grpc.NewServer(opts...)

	s.logger.Info("gRPC RegisterMQServer Successful")

	proto.RegisterMQServer(grpcSrv, s)
	proto.RegisterReplicatorServer(grpcSrv, s)

	reflection.Register(grpcSrv)

	lis, err := net.Listen("tcp", s.cfg.Server.GRPCAddr)
	if err != nil {
		return err
	}
	s.logger.Info("gRPC server listener created",
		zap.String("addr", s.cfg.Server.GRPCAddr),
	)

	// HTTP for health + metrics
	r := mux.NewRouter()
	r.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	r.HandleFunc("/health/ready", s.ready)
	r.Handle("/metrics", promhttp.Handler())
	s.httpSrv = &http.Server{Addr: s.cfg.Server.HTTPAddr, Handler: r}
	s.logger.Info("HTTP server listener created",
		zap.String("addr", s.cfg.Server.HTTPAddr),
	)
	s.GRPCSrv = grpcSrv
	s.GRPCLis = lis

	errCh := make(chan error, 2)

	// Start HTTP
	go func() {
		s.logger.Info("starting HTTP server", zap.String("addr", s.cfg.Server.HTTPAddr))
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server exited unexpectedly", zap.Error(err))
			errCh <- fmt.Errorf("http server error: %w", err)
		}
	}()

	// Start gRPC
	go func() {
		s.logger.Info("starting gRPC server", zap.String("addr", s.cfg.Server.GRPCAddr))
		if err := s.GRPCSrv.Serve(s.GRPCLis); err != nil {
			s.logger.Error("gRPC server exited unexpectedly", zap.Error(err))
			errCh <- fmt.Errorf("grpc server error: %w", err)

		}
	}()

	s.partsMu.Lock()
	for k, v := range s.parts {
		s.logger.Info("initial part", zap.String("key", k), zap.Any("part", v != nil))
	}
	s.partsMu.Unlock()

	// Block until one of them fails or context is canceled
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}

}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }

func partKey(t string, p int32) string { return fmt.Sprintf("%s-%d", t, p) }

// Load/ensure partition leadership and logs
func (s *Server) ensurePartition(ctx context.Context, t string, p int32, requireLeader bool) (*Partition, error) {

	key := partKey(t, p)

	s.partsMu.Lock()
	pr, ok := s.parts[key]
	if ok {
		s.partsMu.Unlock()
		return pr, nil
	}

	st, err := s.store.GetPartitionState(ctx, t, int(p))
	if err != nil {
		s.partsMu.Unlock()
		return nil, err
	}
	if requireLeader && st.Leader != s.cfg.Server.NodeID {
		s.partsMu.Unlock()
		return nil, fmt.Errorf("not leader: leader=%s", st.Leader)
	}
	// Open log storage
	dir := filepath.Join(s.cfg.Storage.DataDir, t, fmt.Sprintf("%d", p))
	pl, err := log.OpenPartition(dir, s.cfg.Storage.SegmentBytes, s.logger)
	if err != nil {
		s.partsMu.Unlock()
		return nil, err
	}

	var rep *replication.Replicator
	followers := map[string]string{}
	if st.Leader == s.cfg.Server.NodeID {
		// Build Replicator to followers in ISR (excluding self)
		for _, id := range st.ISR {
			if id != s.cfg.Server.NodeID {
				addr, ok := s.brokers[id]
				if !ok || addr == "" {
					s.logger.Error("missing broker advertised addr", zap.String("brokerID", id))
					continue
				}
				followers[id] = addr
			}
		}
		var dialOpt grpc.DialOption = grpc.WithInsecure()
		if tlsCfg, _ := s.cfg.Etcd.TLS.GRPCCredentials(); tlsCfg != nil {
			dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))
		}
		rep, err = replication.NewReplicator(followers, dialOpt, s.cfg.Replication.QuorumAcks)
		if err != nil {
			s.partsMu.Unlock()
			s.logger.Info("error creting replicator:", zap.Any("error", err))
			return nil, err
		}
	}
	newPr := &Partition{Log: pl, LeaderEpoch: st.LeaderEpoch, ISR: followers, Rep: rep, logger: s.logger}
	s.parts[key] = newPr
	s.partsMu.Unlock()

	return newPr, nil
}

// --- MQ gRPC implementation ---

func (s *Server) Produce(ctx context.Context, req *proto.ProduceRequest) (*proto.ProduceResponse, error) {
	s.logger.Debug("Got Produce request", zap.Any("topic", req.Topic))

	// If partition not specified (-1), pick based on key hash
	partition := req.Partition
	if partition == -1 {
		meta, err := s.Metadata(ctx, &proto.MetadataRequest{Topics: []string{req.Topic}})
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "topic not found: %v", err)
		}

		// If key exists → consistent hashing
		if len(req.Messages) > 0 && len(req.Messages[0].Key) > 0 {
			h := fnv.New32a()
			h.Write(req.Messages[0].Key)
			partition = int32(int(h.Sum32()) % len(meta.Partitions))
		} else {
			// fallback → round robin
			partition = s.pickNextPartition(req.Topic, len(meta.Partitions))
		}
	}

	// partition selection (hashing omitted; require partition present)
	pr, err := s.ensurePartition(ctx, req.Topic, partition, true)
	if err != nil {
		s.logger.Error("error getting partition", zap.Any("error", err))
		return nil, err
	}
	s.logger.Debug("Got partition", zap.Any("partition", partition), zap.Any("NextOffset", pr.Log.NextOffset()), zap.Any("SegmentBytes", pr.Log.SegmentBytes))

	// Lock the partition for concurrent produce
	pr.mu.Lock()
	defer pr.mu.Unlock()

	// convert messages to wire bytes (frame: key|value|ts|headers JSON)
	batch := make([][]byte, 0, len(req.Messages))
	for _, m := range req.Messages {
		b, _ := json.Marshal(m)
		batch = append(batch, b)
	}
	first, last, err := pr.Log.AppendBatch(batch)
	if err != nil {
		s.logger.Error("error in appendbatch", zap.Any("error", err))
		return nil, err
	}
	s.logger.Debug("debug:=====", zap.Any("first", first), zap.Any("last", last))

	// replicate to ISR
	if pr.Rep != nil && len(pr.ISR) > 0 {
		s.logger.Debug("sending replicate request", zap.Any("topic", req.Topic), zap.Any("partition", partition))
		r := &proto.ReplicateRequest{
			Topic:       req.Topic,
			Partition:   partition,
			BaseOffset:  first,
			Messages:    req.Messages,
			LeaderEpoch: pr.LeaderEpoch,
		}
		if err := pr.Rep.Append(ctx, r); err != nil {
			s.logger.Error("error in replication Append", zap.Any("error", err))
			return nil, err
		}
	}
	// acks: already enforced by Replicator when quorum required
	// return offsets
	offsets := make([]int64, 0, len(req.Messages))
	for off := first; off <= last; off++ {
		offsets = append(offsets, off)
	}
	s.logger.Debug("Produce request sucessfull", zap.Any("topic", req.Topic), zap.Any("partition", partition))
	return &proto.ProduceResponse{Partition: partition, Offsets: offsets}, nil
}

func (s *Server) Fetch(ctx context.Context, req *proto.FetchRequest) (*proto.FetchResponse, error) {

	// If client didn’t specify partition, pick one (simple default: 0)
	partition := req.Partition
	if partition == -1 {
		partition = 0
	}
	s.logger.Debug("Got Fetch request", zap.Any("topic", req.Topic), zap.Any("partition", partition), zap.Any("offset", req.Offset))

	pr, err := s.ensurePartition(ctx, req.Topic, partition, true)
	if err != nil {
		return nil, err
	}

	// Lock partition while reading
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	recs, hw, err := pr.Log.ReadFrom(req.Offset, int(req.MaxMessages))
	if err != nil && !errors.Is(err, io.EOF) {
		s.logger.Error("error in reading data", zap.Any("error", err))
		return nil, err
	}
	res := &proto.FetchResponse{HighWatermark: hw}

	for _, r := range recs {
		var m proto.Message
		if err := json.Unmarshal(r.Data, &m); err != nil {
			s.logger.Warn("json marshal error", zap.Any("error", err), zap.Any("r", string(r.Data)), zap.Any("off", r.Off))
			continue
		}
		res.Records = append(res.Records, &proto.FetchedMessage{
			Offset:  r.Off,
			Message: &m,
		})
	}
	s.logger.Debug("Fetch request successful",
		zap.Any("topic", req.Topic),
		zap.Any("partition", partition),
		zap.Any("fetched_count", len(res.Records)),
		zap.Any("high_watermark", hw),
	)

	return res, nil
}

func (s *Server) Metadata(ctx context.Context, req *proto.MetadataRequest) (*proto.MetadataResponse, error) {
	s.logger.Debug("Got Metadata request", zap.Any("topic", req.Topics))

	bs, err := s.store.ListBrokers(ctx)
	if err != nil {
		return nil, err
	}
	s.logger.Debug("broker list: ", zap.Any("brokers", bs))
	parts := []*proto.TopicPartitionMeta{}
	for _, t := range req.Topics {
		td, err := s.store.GetTopic(ctx, t)
		if err != nil {
			return nil, err
		}
		for p := 0; p < td.Partitions; p++ {
			st, _ := s.store.GetPartitionState(ctx, t, p)
			if st != nil {
				parts = append(parts, &proto.TopicPartitionMeta{Topic: t, Partition: int32(p), Leader: st.Leader, Isr: st.ISR})
			}
		}
	}
	brokerIDs := make([]*proto.Broker, 0, len(bs))
	for id, addr := range bs {
		parts := strings.Split(addr, ":")
		host := parts[0]
		port, _ := strconv.Atoi(parts[1])
		brokerIDs = append(brokerIDs, &proto.Broker{
			Id:   id,
			Host: host,
			Port: int32(port),
		})
	}
	s.logger.Debug("MetadataResponse ", zap.Any("Partitions", parts), zap.Any("brokers", brokerIDs))

	return &proto.MetadataResponse{Partitions: parts, Brokers: brokerIDs}, nil
}

func (s *Server) CreateTopic(ctx context.Context, req *proto.CreateTopicRequest) (*proto.CreateTopicResponse, error) {
	if err := s.store.CreateTopic(ctx, metadata.TopicDesc{Name: req.Topic, Partitions: int(req.Partitions), RF: int(req.Rf)}); err != nil {
		return nil, err
	}
	if err := s.store.AssignInitialLeaders(ctx, req.Topic, int(req.Partitions), int(req.Rf)); err != nil {
		return nil, err
	}
	return &proto.CreateTopicResponse{}, nil
}

func (s *Server) DeleteTopic(ctx context.Context, req *proto.DeleteTopicRequest) (*proto.DeleteTopicResponse, error) {
	// left as exercise: delete keys and local data
	return &proto.DeleteTopicResponse{}, nil
}

func (s *Server) CommitOffsets(ctx context.Context, req *proto.CommitOffsetsRequest) (*proto.CommitOffsetsResponse, error) {
	if err := s.store.CommitOffset(ctx, req.GroupId, req.Topic, int(req.Partition), req.Offset); err != nil {
		return nil, err
	}
	return &proto.CommitOffsetsResponse{}, nil
}

func (s *Server) FetchOffsets(ctx context.Context, req *proto.FetchOffsetsRequest) (*proto.FetchOffsetsResponse, error) {
	offs, err := s.store.FetchGroupOffsets(ctx, req.GroupId, req.Topic)
	if err != nil {
		return nil, err
	}
	res := &proto.FetchOffsetsResponse{}
	for _, o := range offs {
		res.Offsets = append(res.Offsets, &proto.GroupOffset{Partition: int32(o.Partition), Offset: o.Offset})
	}
	return res, nil
}

// --- Replicator RPC ---
func (s *Server) Append(ctx context.Context, req *proto.ReplicateRequest) (*proto.ReplicateResponse, error) {
	s.logger.Debug("received replication request",
		zap.Any("nodeId", s.cfg.Server.NodeID), zap.Any("Topic", req.Topic), zap.Any("partition", req.Partition), zap.Any("len", len(req.Messages)))
	pr, err := s.ensurePartition(ctx, req.Topic, req.Partition, false)
	if err != nil {
		return nil, err
	}
	if req.LeaderEpoch < pr.LeaderEpoch {
		return nil, fmt.Errorf("stale leader epoch")
	}
	batch := make([][]byte, 0, len(req.Messages))
	for _, m := range req.Messages {
		b, _ := json.Marshal(m)
		batch = append(batch, b)
	}
	_, last, err := pr.Log.AppendBatch(batch)
	if err != nil {
		return nil, err
	}
	return &proto.ReplicateResponse{LastOffset: last}, nil
}

var rrCounter = sync.Map{} // topic → counter

func (s *Server) pickNextPartition(topic string, numParts int) int32 {
	val, _ := rrCounter.LoadOrStore(topic, int32(0))
	c := val.(int32)
	next := (c + 1) % int32(numParts)
	rrCounter.Store(topic, next)
	return next
}

func (s *Server) Shutdown(ctx context.Context) error {
	// Close partition logs first so they flush to disk
	s.partsMu.Lock()
	for _, pr := range s.parts {
		if pr != nil && pr.Log != nil {
			_ = pr.Log.Close()
		}
	}
	s.partsMu.Unlock()

	if s.GRPCSrv != nil {
		s.GRPCSrv.GracefulStop()
	}
	if s.httpSrv != nil {
		ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = s.httpSrv.Shutdown(ctx2)
	}
	return nil
}
