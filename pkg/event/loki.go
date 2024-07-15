package event

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"metrics-server/pkg/metric"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	BufferSize        = 10000
	BulkPeriodSeconds = 5
	MaxStreams        = 500
)

type Loki struct {
	Client     *http.Client
	Address    string
	BulkBuffer chan metric.Metric
}

var loki *Loki

func NewLoki(caPath, certPath, keyPath, address string) (*Loki, error) {
	if loki != nil {
		return loki, nil
	}
	log.Printf("Create loki client")
	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("cannot load CA certificate, %v", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// load client cert
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("cannot load client certificate and key, %v", err)
	}

	// setup HTTPS client
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}

	loki = &Loki{
		Client:     client,
		Address:    address,
		BulkBuffer: make(chan metric.Metric, BufferSize),
	}
	go loki.BulkWorker()
	return loki, nil
}

type LokiRequest struct {
	Streams []Stream `json:"streams"`
}

type Stream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

func sanitizeLabels(labels map[string]string) map[string]string {
	sanitized := make(map[string]string, len(labels))
	for k, v := range labels {
		sanitized[strings.ReplaceAll(k, "-", "_")] = v
	}
	return sanitized
}

func (loki *Loki) BulkWorker() {
	var err error
	events := LokiRequest{}
	t := time.NewTicker(time.Second * BulkPeriodSeconds)
	for {
		select {
		case m := <-loki.BulkBuffer:
			var docData []byte
			docData, err = json.Marshal(m)
			if err != nil {
				log.Errorf("Cannot marshal metric, %v", err)
			}
			events.Streams = append(events.Streams, Stream{
				Stream: sanitizeLabels(m.Labels),
				Values: [][]string{
					{generateEpoch(), string(docData)},
				},
			})
			if len(events.Streams) >= MaxStreams {
				log.Warnf("Max loki streams are excited")
				err = loki.Push(&events)
				if err != nil {
					log.Errorf("Cannot push events to loki %v", err)
				}
				events.Streams = nil
				events = LokiRequest{}
				runtime.GC()
			}
		case <-t.C:
			if len(events.Streams) == 0 {
				continue
			}
			log.Debugf("Make tick loki push")
			err = loki.Push(&events)
			if err != nil {
				log.Errorf("Cannot push events to loki %v", err)
			}
			log.Debugf("Stream slice len: %v, cap: %v", len(events.Streams), cap(events.Streams))
			events.Streams = nil
			events = LokiRequest{}
			runtime.GC()
		}
	}
}

func (loki *Loki) Push(data *LokiRequest) error {
	log.Printf("Push %v loki events", len(data.Streams))
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("cannot marshal loki data, %v", err)
	}

	resp, err := loki.Client.Post(loki.Address, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("cannot push events to loki api, %v", err)
	}
	_, err = io.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("%v", err)
	}

	//err = resp.Body.Close()
	defer resp.Body.Close()

	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		log.Errorf("%v", err)
	}
	if resp.StatusCode > 299 {
		log.Printf("Loki push response status: %v, body: %+v", resp.Status, data)
	}
	return nil
}

// Function to generate Unix epoch time in nanoseconds
func generateEpoch() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
