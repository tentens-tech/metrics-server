package index

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"io"
	"metrics-server/pkg/metric"
	"net/http"
	"os"
	"strings"
	"time"
	//"time"
)

const (
	// https://opensearch.org/docs/latest/opensearch/index-templates/
	BufferSize        = 10000
	BulkSizeBytes     = 20000
	BulkPeriodSeconds = 5
	IndexSettings     = `{
  "settings": {
    "index": {
      "number_of_shards": 9,
      "number_of_replicas": 2
    }
  },
  "mappings": {
    "properties": {
      "timestamp": {
        "type": "date",
        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis||strict_date_optional_time_nanos"
      },
      "value": {
        "type": "double"
      }
    }
  }
}`
)

var (
	bulkInsertCounter *prometheus.CounterVec
)

func init() {
	var err error
	bulkInsertCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "metrics_server_index_bulk_insert_total",
	}, []string{"type", "status"})
	err = prometheus.Register(bulkInsertCounter)
	if err != nil {
		log.Warnf("%v", err)
	}
}

type OpenSearch struct {
	Client     *opensearch.Client
	BulkBuffer chan metric.Metric
	Ctx        context.Context
	cancel     context.CancelFunc
	bulkPeriod time.Duration
	bulkSize   int
}

func NewOpenSearch(ctx context.Context, cancel context.CancelFunc, bulkPeriod time.Duration, bulkSize int) (*OpenSearch, error) {
	var err error
	search := &OpenSearch{
		BulkBuffer: make(chan metric.Metric, BufferSize),
		Ctx:        ctx,
		cancel:     cancel,
		bulkPeriod: bulkPeriod,
		bulkSize:   bulkSize,
	}
	search.Client, err = opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: []string{os.Getenv("METRICS_SERVER_SEARCH_URL")},
		Username:  os.Getenv("METRICS_SERVER_SEARCH_USER"),
		Password:  os.Getenv("METRICS_SERVER_SEARCH_PASSWORD"),
	})
	if err != nil {
		return nil, err
	}
	return search, nil
}

func (search *OpenSearch) Init(indexName string) error {
	var err error
	go search.Bulk()
	settings := strings.NewReader(IndexSettings)
	res := opensearchapi.IndicesCreateRequest{
		Index: indexName,
		Body:  settings,
	}

	indexResponse, err := res.Do(context.Background(), search.Client)
	if err != nil {
		log.Errorf("Cannot create index, %v", err)
		return err
	}
	indexData, err := io.ReadAll(indexResponse.Body)
	if err != nil {
		log.Errorf("Cannot read index response data, %v", err)
		return err
	}
	log.Debugf("Index response: %v", string(indexData))
	return nil
}

func (search *OpenSearch) Bulk() {
	var err error
	var body strings.Builder
	t := time.NewTicker(search.bulkPeriod)
	for {
		select {
		case <-search.Ctx.Done():
			log.Printf("Shutdown bulk worker")
			return
		case <-t.C:
			if body.Len() > 0 {
				log.Warnf("Make tick push %v, body len %v", search.bulkPeriod, body.Len())
				err = search.BulkInsert(search.Client, &body)
				if err != nil {
					log.Errorf("Bulk insert failed, %v", err)
					bulkInsertCounter.WithLabelValues("time", "failed").Inc()
				}
				bulkInsertCounter.WithLabelValues("time", "success").Inc()
			}
		case m := <-search.BulkBuffer:
			var docData []byte
			docData, err = json.Marshal(m)
			if err != nil {
				log.Errorf("Cannot marshal metric, %v", err)
			}
			body.WriteString(fmt.Sprintf(`{"index" : { "_index" : "%v" }}`,
				m.IndexName))
			body.WriteString("\n")
			body.WriteString(string(docData) + "\n")
			log.Debugf("Add %v to bulk buffer. Bulk body len: %v", m.Name, body.Len())
			if body.Len() > search.bulkSize {
				log.Warnf("Make sized push, body len %v > %v", body.Len(), search.bulkSize)
				err = search.BulkInsert(search.Client, &body)
				if err != nil {
					log.Errorf("Bulk insert failed, %v", err)
					bulkInsertCounter.WithLabelValues("size", "failed").Inc()
				}
				bulkInsertCounter.WithLabelValues("size", "success").Inc()
			}
		}
	}
}

func (search OpenSearch) BulkInsert(client *opensearch.Client, body *strings.Builder) error {
	var err error
	var insertResponse *opensearchapi.Response
	var insertData []byte
	log.Debugf("Bulk insert body: %v", body.String())
	bulk := opensearchapi.BulkRequest{
		Body:    strings.NewReader(body.String()),
		Timeout: time.Second * 60,
	}
	insertResponse, err = bulk.Do(context.Background(), client)
	if err != nil {
		log.Errorf("Cannot insert bulk data, %v", err)
		return err
	}
	insertData, err = io.ReadAll(insertResponse.Body)
	if err != nil {
		log.Errorf("Cannot read bulk insert response data, %v", err)
		return err
	}
	log.Debugf("Response: %v", string(insertData))
	body.Reset()
	return nil
}
