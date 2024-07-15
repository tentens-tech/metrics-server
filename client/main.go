package main

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

type Metric struct {
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels"`
	Operation string            `json:"op"`
	Help      string            `json:"help"`
	Buckets   int               `json:"buckets"`
	Width     float64           `json:"bucket_width"`
	Value     float64           `json:"value"`
}

func PushIncMetrics(wg *sync.WaitGroup, c net.Conn, group string) {
	defer wg.Done()
	var metrics [][]byte
	for i := 0; i < 1; i++ {
		m := Metric{
			Name:      fmt.Sprintf("job_%v", group),
			Operation: "inc",
			Labels: map[string]string{
				"application": "metrics-server",
			},
			Value: 1.0,
		}
		data, err := json.Marshal(m)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
		metrics = append(metrics, data)
	}

	for _, metric := range metrics {
		fmt.Printf("%v\n", string(metric))
		_, err := c.Write(metric)
		if err != nil {
			fmt.Printf("%v", err)
		}
	}
}

func PushEvents(wg *sync.WaitGroup, c net.Conn, group string) {
	defer wg.Done()
	var metrics [][]byte
	for i := 0; i < 10; i++ {
		m := Metric{
			Name:      fmt.Sprintf("test_app"),
			Operation: "event",
			Labels: map[string]string{
				"app":        "test_app",
				"event_type": "test_event",
			},
			Value: 1.0,
		}
		data, err := json.Marshal(m)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
		metrics = append(metrics, data)
	}

	for _, metric := range metrics {
		fmt.Printf("%v\n", string(metric))
		_, err := c.Write(metric)
		if err != nil {
			fmt.Printf("%v", err)
		}
	}
}

func main() {
	wg := sync.WaitGroup{}
	c, err := net.Dial("unix", "metric-server.sock")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		//go LoadMetrics(&wg, c)
		go PushIncMetrics(&wg, c, fmt.Sprintf("%v", i))
	}
	wg.Wait()
}
