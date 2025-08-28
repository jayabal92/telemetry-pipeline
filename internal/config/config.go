package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

type StorageConfig struct {
	DataDir       string `yaml:"dataDir"`
	SegmentBytes  int64  `yaml:"segmentBytes"`
	RetentionHrs  int    `yaml:"retentionHours"`
	RetentionSize int64  `yaml:"retentionBytes"`
}

type ServerConfig struct {
	GRPCAddr           string `yaml:"grpcAddr"`
	HTTPAddr           string `yaml:"httpAddr"`
	NodeID             string `yaml:"nodeId"`
	GRPCAdvertisedAddr string `yaml:"grpcAdvertisedAddr"`
}

type EtcdConfig struct {
	Endpoints []string  `yaml:"endpoints"`
	Username  string    `yaml:"username"`
	Password  string    `yaml:"password"`
	TLS       TLSConfig `yaml:"tls"`
}

type TLSConfig struct {
	Enable   bool   `yaml:"enable"`
	CAFile   string `yaml:"caFile"`
	CertFile string `yaml:"certFile"`
	KeyFile  string `yaml:"keyFile"`
}

type ReplicationConfig struct {
	ReplicationFactor int `yaml:"rf"`
	QuorumAcks        int `yaml:"quorumAcks"`
}

type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Storage     StorageConfig     `yaml:"storage"`
	Etcd        EtcdConfig        `yaml:"etcd"`
	Replication ReplicationConfig `yaml:"replication"`
}

func (t TLSConfig) GRPCCredentials() (*tls.Config, error) {
	if !t.Enable {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, err
	}
	ca, err := ioutil.ReadFile(t.CAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: pool, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}, nil
}

func (c Config) String() string {
	return fmt.Sprintf("node=%s grpc=%s grpcsAdvrAddr=%s dataDir=%s", c.Server.NodeID, c.Server.GRPCAddr, c.Server.GRPCAdvertisedAddr, c.Storage.DataDir)
}
