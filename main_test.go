package main

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"metrics-server/pkg/metric"
	"metrics-server/pkg/processor"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestJsonExtractor(t *testing.T) {
	// Arrange
	testTable := []struct {
		data     string
		expected [][]int
	}{
		{
			`{"name":"metric","labels":{"label1":"labelvalue1"},"op":"inc","value":1.0}`,
			[][]int{
				{0, 74},
			},
		},
		{
			`{"name":"metric","labels":{"label1":"labelvalue1"},"op":"inc","value":1.0}{"name":"jobs_runned","labels":{"app":"job-counter","instance":"job-counter-a32q35as"},"op":"inc","value":1.0}`,
			[][]int{
				{0, 74},
				{74, 184},
			},
		},
	}

	// Act
	for _, testCase := range testTable {
		app := processor.App{}
		result := app.JsonExtractor(testCase.data)
		// Assert
		if !reflect.DeepEqual(result, testCase.expected) {
			t.Errorf("Incorrect result. Expect %v, got %v", testCase.expected, result)
		}
	}
}

func TestRawDataParser(t *testing.T) {
	// Arrange
	testTable := []struct {
		data     string
		expected []string
	}{
		{
			`{"name":"metric","labels":{"label1":"labelvalue1"},"op":"inc","value":1.0}datadatagrabagedata`,
			[]string{
				`{"name":"metric","labels":{"label1":"labelvalue1"},"op":"inc","value":1.0}`,
			},
		},
		{
			`{"name":"metric","labels":{"label1":"labelvalue1"},"op":"inc","value":1.0}{"name":"jobs_runned","labels":{"app":"job-counter","instance":"job-counter-a32q35as"},"op":"inc","value":1.0}`,
			[]string{
				`{"name":"metric","labels":{"label1":"labelvalue1"},"op":"inc","value":1.0}`,
				`{"name":"jobs_runned","labels":{"app":"job-counter","instance":"job-counter-a32q35as"},"op":"inc","value":1.0}`,
			},
		},
	}

	// Act
	for _, testCase := range testTable {
		app := processor.App{}
		app.RawData = make(chan processor.RawDataMessage, 512)
		app.MetricBuffer = make(chan processor.MetricBufferMessage, 1)
		var result []string
		go app.RawDataParser()
		t.Logf("Run RawDataParser worker")

		app.RawData <- processor.RawDataMessage{
			Data:   testCase.data,
			Remote: "",
		}
		t.Logf("Write raw data")
		for i := 0; i < len(testCase.expected); i++ {
			mb := <-app.MetricBuffer
			result = append(result, mb.Data)
		}
		// Assert

		if !reflect.DeepEqual(result, testCase.expected) {
			t.Errorf("Incorrect result. Expect %v, got %v", testCase.expected, result)
		}
	}
}

func TestMetricParser(t *testing.T) {
	// Arrange
	testTable := []struct {
		data     string
		expected metric.Metric
	}{
		{
			`{"name":"metric","labels":{"label1":"labelvalue1"},"op":"inc","value":1.0}`,
			metric.Metric{
				Name: "metric",
				Labels: map[string]string{
					"label1":      "labelvalue1",
					"source_app":  "",
					"data_source": "",
				},
				Operation: "inc",
				Value:     1.0,
			},
		},
	}

	// Act
	for _, testCase := range testTable {
		app := processor.App{}
		app.MetricBuffer = make(chan processor.MetricBufferMessage, 1)
		app.LocalMetric = make(chan metric.Metric, 1)
		var result metric.Metric
		t.Logf("Run MetricParser worker")
		go app.MetricParser()
		t.Logf("Write raw data")

		app.MetricBuffer <- processor.MetricBufferMessage{
			Data:   testCase.data,
			Remote: "",
		}
		t.Logf("Read parsed metric")
		result = <-app.LocalMetric
		// Assert
		if !reflect.DeepEqual(result, testCase.expected) {
			t.Errorf("Incorrect result. Expect %v, got %v", testCase.expected, result)
		}
	}
}

func TestLocalMetricProcessor(t *testing.T) {
	// Arrange
	testTable := []struct {
		data     metric.Metric
		expected float64
	}{
		{
			metric.Metric{
				Name: "metric_inc",
				Labels: map[string]string{
					"label1": "labelvalue1",
				},
				Operation: "inc",
				Value:     1.0,
			},
			1.0,
		},
		{
			metric.Metric{
				Name: "metric_gauge",
				Labels: map[string]string{
					"label1": "labelvalue1",
				},
				Operation: "set",
				Value:     100.0,
			},
			100.0,
		},
	}

	// Act
	app := processor.App{}
	app.InitMetrics()
	for _, testCase := range testTable {
		var result float64
		//app := App{}
		app.LocalMetric = make(chan metric.Metric, 1)
		app.MetricRegistry.Counter = make(map[string]*prometheus.CounterVec, 100)
		app.MetricRegistry.Gauge = make(map[string]*prometheus.GaugeVec, 100)
		app.MetricRegistry.Histogram = make(map[string]*prometheus.HistogramVec, 100)
		t.Logf("Write metric data")
		app.LocalMetric <- testCase.data
		t.Logf("Run LocalMetricProcessor worker")
		go app.LocalMetricProcessor()
		time.Sleep(time.Second * 1)

		t.Logf("Generate labels slice")

		labels := make([]string, 0, len(testCase.data.Labels))
		for l := range testCase.data.Labels {
			labels = append(labels, l)
		}
		sort.Strings(labels)
		var lv processor.LabelKV
		for _, l := range labels {
			lv = append(lv, l, testCase.data.Labels[l])
		}

		sIndex := lv.ToLI(testCase.data.Name)

		t.Logf("Read metric value")
		switch {
		case testCase.data.Operation == "inc":
			t.Logf("DUMP %v", app.MetricRegistry.Counter)
			result = testutil.ToFloat64(app.MetricRegistry.Counter[sIndex])
		case testCase.data.Operation == "set":
			t.Logf("DUMP %v", app.MetricRegistry.Gauge)
			result = testutil.ToFloat64(app.MetricRegistry.Gauge[sIndex])
			// Assert
			if !reflect.DeepEqual(result, testCase.expected) {
				t.Errorf("Incorrect result. Expect %v, got %v", testCase.expected, result)
			}
		}
	}
}

func BenchmarkMetricTTL(b *testing.B) {
	metricTTL := time.Second * 1
	app := processor.App{}
	app.InitMetrics()
	go app.LocalMetricProcessor()
	app.RawData = make(chan processor.RawDataMessage, 512)
	app.MetricBuffer = make(chan processor.MetricBufferMessage, 1)
	app.LocalMetric = make(chan metric.Metric, 0)
	app.RemoteMetric = make(chan metric.Metric, 0)
	app.MetricRegistry.Counter = make(map[string]*prometheus.CounterVec, 100)
	app.MetricRegistry.Gauge = make(map[string]*prometheus.GaugeVec, 100)
	app.MetricRegistry.Histogram = make(map[string]*prometheus.HistogramVec, 100)
	app.MetricRegistry.Deadline = make(map[string]*processor.MetricTTL, processor.StaledMetricsBuffer)
	app.MetricRegistry.CleanUpPeriod = time.NewTicker(metricTTL + (metricTTL / 100 * 1))
	time.Sleep(time.Second * 1)
	for i := 0; i < b.N; i++ {
		for l := 0; l < 10; l++ {
			app.LocalMetric <- metric.Metric{
				Name: fmt.Sprintf("metric_%v", l),
				Labels: map[string]string{
					fmt.Sprintf("k1_%v", l): fmt.Sprintf("v1_%v", l),
					fmt.Sprintf("k2_%v", l): fmt.Sprintf("v2_%v", l),
				},
				Operation: "inc",
				Value:     10,
			}
		}

	}
}
