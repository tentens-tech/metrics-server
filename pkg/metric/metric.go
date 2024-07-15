package metric

import (
	"time"
)

type Metric struct {
	Name          string            `json:"name"`
	Labels        map[string]string `json:"labels,omitempty"`
	Operation     string            `json:"op,omitempty"`
	Help          string            `json:"help,omitempty"`
	IndexTemplate string            `json:"index,omitempty"`
	IndexName     string            `json:"-"`
	Buckets       int               `json:"buckets,omitempty"`
	CustomBuckets []float64         `json:"bucket_custom,omitempty"`
	Width         float64           `json:"bucket_width,omitempty"`
	Factor        float64           `json:"bucket_factor,omitempty"`
	Min           float64           `json:"bucket_min,omitempty"`
	Max           float64           `json:"bucket_max,omitempty"`
	Start         float64           `json:"bucket_start,omitempty"`
	Value         float64           `json:"value"`
	Timestamp     *time.Time        `json:"timestamp,omitempty"`
}
