package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConstructUUIDAndIsValidUUID(t *testing.T) {
	uuid := ConstructUUID("gpu0", "host1")
	assert.Equal(t, "gpu0:host1", uuid)

	valid, dev, host := IsValidUUID(uuid)
	assert.True(t, valid)
	assert.Equal(t, "gpu0", dev)
	assert.Equal(t, "host1", host)

	// invalid case
	valid, dev, host = IsValidUUID("bad-uuid")
	assert.False(t, valid)
	assert.Empty(t, dev)
	assert.Empty(t, host)
}

func TestGPU_SetUUIDAndGetUUID(t *testing.T) {
	g := &GPU{Device: "gpu1", Hostname: "node1"}
	g.SetUUID()
	assert.Equal(t, "gpu1:node1", g.getUUID())
}

func TestAggregation_IsValid(t *testing.T) {
	assert.True(t, AggregationMin.IsValid())
	assert.True(t, AggregationMax.IsValid())
	assert.True(t, AggregationAvg.IsValid())
	assert.True(t, AggregationSum.IsValid())

	assert.False(t, Aggregation("bad").IsValid())
}

func TestMetricName_IsValid(t *testing.T) {
	validMetrics := []MetricName{
		MetricDecUtil, MetricEncUtil, MetricFbFree, MetricFbUsed,
		MetricGpuTemp, MetricGpuUtil, MetricMemClock, MetricMemCopyUtil,
		MetricPowerUsage, MetricSmClock,
	}
	for _, m := range validMetrics {
		assert.True(t, m.IsValid(), "expected %s to be valid", m)
	}

	assert.False(t, MetricName("UNKNOWN").IsValid())
}

func TestJSONBMap_ScanAndValue(t *testing.T) {
	var m JSONBMap

	// nil value → empty map
	err := m.Scan(nil)
	require.NoError(t, err)
	assert.NotNil(t, m)
	assert.Equal(t, 0, len(m))

	// valid JSON
	raw := []byte(`{"k1":"v1","k2":"v2"}`)
	err = m.Scan(raw)
	require.NoError(t, err)
	assert.Equal(t, "v1", m["k1"])
	assert.Equal(t, "v2", m["k2"])

	// invalid type
	err = m.Scan("not-bytes")
	assert.Error(t, err)

	// Value should marshal back to JSON
	v, err := m.Value()
	require.NoError(t, err)

	// round-trip check
	var out map[string]string
	err = json.Unmarshal(v.([]byte), &out)
	require.NoError(t, err)
	assert.Equal(t, "v1", out["k1"])
}

func TestPaginatedResponse(t *testing.T) {
	resp := PaginatedResponse[int]{
		Total:   100,
		Limit:   10,
		Offset:  20,
		Count:   10,
		Results: []int{1, 2, 3},
	}

	assert.Equal(t, 100, resp.Total)
	assert.Equal(t, []int{1, 2, 3}, resp.Results)
}
