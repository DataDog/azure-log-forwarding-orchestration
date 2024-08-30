package metrics

import (
	"bufio"
	"bytes"
	"encoding/json"
)

const MetricsBucket = "forwarder-metrics"

type MetricEntry struct {
	Timestamp          int64            `json:"timestamp"`
	RuntimeSeconds     float64          `json:"runtime_seconds"`
	ResourceLogVolumes map[string]int64 `json:"resource_log_volume"`
}

func FromBytes(data []byte) ([]MetricEntry, error) {
	var metrics []MetricEntry
	reader := bufio.NewScanner(bytes.NewReader(data))
	for reader.Scan() {
		currLine := reader.Text()
		if currLine == "" {
			continue
		}
		var metric MetricEntry
		err := json.Unmarshal([]byte(currLine), &metric)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, metric)
	}

	return metrics, nil
}

func (m MetricEntry) ToBytes() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}
