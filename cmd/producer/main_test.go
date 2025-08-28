package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"telemetry-pipeline/internal/model"
	pb "telemetry-pipeline/proto"
)

// mockProducer captures produced messages
type mockProducer struct {
	sent [][]*pb.Message
	err  error
}

func (m *mockProducer) Produce(topic string, partition int, msgs []*pb.Message) error {
	copied := make([]*pb.Message, len(msgs))
	for i, msg := range msgs {
		v := *msg      // copy the struct
		copied[i] = &v // take address of the copy
	}
	m.sent = append(m.sent, copied)
	return m.err
}

func (m *mockProducer) Close() { return }

func writeTempCSV(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.csv")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp csv: %v", err)
	}
	return path
}

func TestProduceFromCSV_Batching(t *testing.T) {
	csv := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
2025-08-28T08:46:58Z,utilization,1,dev,uuid1,RTX,host1,c1,p1,ns1,75.5,"k1=v1,k2=v2"
2025-08-28T08:47:58Z,utilization,2,dev,uuid2,RTX,host2,c2,p2,ns2,88.0,"x=1,y=2"
2025-08-28T08:47:58Z,utilization,2,dev,uuid2,RTX,host2,c2,p2,ns2,88.0,"x=1,y=2"
2025-08-28T08:47:58Z,utilization,2,dev,uuid2,RTX,host2,c2,p2,ns2,88.0,"x=1,y=2"

`
	path := writeTempCSV(t, csv)

	mp := &mockProducer{}
	err := produceFromCSV(mp, path, "topicA", 0, 2) // batch size = 1, so 2 flushes
	assert.NoError(t, err)
	assert.Len(t, mp.sent, 2)

	hosts := []string{}
	values := []float64{}
	uuids := []string{}

	for _, batch := range mp.sent { // batch is []*pb.Message
		for _, m := range batch {
			tel := model.Telemetry{}
			err := json.Unmarshal(m.Value, &tel)
			require.NoError(t, err)

			hosts = append(hosts, tel.Hostname)
			values = append(values, tel.Value)
			uuids = append(uuids, tel.UUID)
		}
	}

	require.ElementsMatch(t, []string{"host1", "host2", "host2", "host2"}, hosts)
	require.ElementsMatch(t, []float64{75.5, 88, 88, 88}, values)
	require.ElementsMatch(t, []string{"uuid1", "uuid2", "uuid2", "uuid2"}, uuids)
}

func TestProduceFromCSV_FlushLeftover(t *testing.T) {
	csv := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
2025-08-28T08:46:58Z,utilization,3,dev,uuid3,RTX,host3,c3,p3,ns3,99.9,"a=b"
`
	path := writeTempCSV(t, csv)

	mp := &mockProducer{}
	// batchSize larger than row count → leftover flush path
	err := produceFromCSV(mp, path, "topicB", 1, 10)
	assert.NoError(t, err)
	assert.Len(t, mp.sent, 1)
}

func TestSafeGet(t *testing.T) {
	record := []string{"a", "b"}
	idx := map[string]int{"f1": 0, "f2": 1}
	assert.Equal(t, "a", safeGet(record, idx, "f1"))
	assert.Equal(t, "b", safeGet(record, idx, "f2"))
	assert.Equal(t, "", safeGet(record, idx, "f3"))
}

func TestParseHelpers(t *testing.T) {
	// parseInt
	assert.Equal(t, 42, parseInt("42"))
	assert.Equal(t, 0, parseInt("bad"))

	// parseFloat
	assert.Equal(t, 3.14, parseFloat("3.14"))
	assert.Equal(t, 0.0, parseFloat("bad"))

	// parseTime from millis
	ts := parseTime("1693212345678")
	assert.WithinDuration(t, time.UnixMilli(1693212345678), ts, time.Millisecond)

	// parseTime from RFC3339
	ts2 := parseTime("2025-08-28T08:46:58Z")
	assert.Equal(t, 2025, ts2.Year())

	// fallback → now
	now := time.Now()
	ts3 := parseTime("not-a-time")
	assert.WithinDuration(t, now, ts3, time.Second)

	// parseLabels
	labels := parseLabels("a=1,b=2")
	assert.Equal(t, "1", labels["a"])
	assert.Equal(t, "2", labels["b"])
	assert.Equal(t, 0, len(parseLabels("")))
}

// Negative test: missing file
func TestProduceFromCSV_FileNotFound(t *testing.T) {
	mp := &mockProducer{}
	err := produceFromCSV(mp, "/nonexistent/path.csv", "topicA", 0, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to open csv file")
}

// Negative test: malformed CSV row (bad float value)
func TestProduceFromCSV_BadRow(t *testing.T) {
	// invalid float in value column
	csv := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
2025-08-28T08:46:58Z,utilization,1,dev,uuid1,RTX,host1,c1,p1,ns1,notafloat,"k1=v1"
`
	path := writeTempCSV(t, csv)

	mp := &mockProducer{}
	err := produceFromCSV(mp, path, "topicA", 0, 10)
	require.NoError(t, err) // should not crash

	// still produced message, but Value should be zero (default float64)
	require.Len(t, mp.sent, 1)
	var tel model.Telemetry
	err = json.Unmarshal(mp.sent[0][0].Value, &tel)
	require.NoError(t, err)
	require.Equal(t, float64(0), tel.Value)
}

// Negative test: producer returns error
func TestProduceFromCSV_ProducerError(t *testing.T) {
	csv := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
2025-08-28T08:46:58Z,utilization,1,dev,uuid1,RTX,host1,c1,p1,ns1,42.0,"k1=v1"
`
	path := writeTempCSV(t, csv)

	mp := &mockProducer{err: errors.New("mock send error")}
	err := produceFromCSV(mp, path, "topicA", 0, 10)
	require.Error(t, err) // since function only logs
	require.Contains(t, err.Error(), "mock send error")
}
