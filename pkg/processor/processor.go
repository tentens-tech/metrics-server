package processor

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/pyroscope-io/client/pyroscope"
	log "github.com/sirupsen/logrus"
	"io"
	"metrics-server/pkg/event"
	"metrics-server/pkg/index"
	"metrics-server/pkg/metric"
	"metrics-server/pkg/raw"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	UDS                        = "./metric-server.sock"
	MetricPort                 = ":9099"
	CollectorUDPSocket         = ":9999"
	ListenerBuffer             = 65565
	HistogramBucketWidth       = 10
	HistogramBucketCount       = 20
	StaledMetricsBuffer        = 5000
	DefaultMetricIndexTemplate = "metric-events"
)

var (
	debug                   bool
	ready                   bool
	metricCleanUp           bool
	socket                  string
	uds                     string
	udsPool                 string
	udp                     string
	remote                  string
	labelsFilePath          string
	lokiCaPath              string
	lokiCertPath            string
	lokiKeyPath             string
	lokiAddress             string
	pyroscopeUrl            string
	scylladbTable           string
	scylladbHosts           []string
	extraLabels             map[string]string
	shutdownDelay           time.Duration
	metricTTL               time.Duration
	opensearchBulkPeriod    time.Duration
	opensearchBulkSizeBytes int
)

func InitFlags() {
	var extraLabelsflag string
	var hosts string
	extraLabels = make(map[string]string, 10)
	flag.BoolVar(&debug, "debug", false, "enable debug log level")
	flag.BoolVar(&metricCleanUp, "metric-clean-up", false, "enable clean up metric via metric-ttl time after creation")
	flag.StringVar(&socket, "socket", MetricPort, "prometheus exporter tcp socket")
	flag.StringVar(&uds, "uds", UDS, "path to collector unix domain socket file")
	flag.StringVar(&udsPool, "uds-pool", "", "comma separated list of unix domain sockets")
	flag.StringVar(&udp, "udp", CollectorUDPSocket, "collector udp socket")
	flag.StringVar(&remote, "remote", "", "send metrics to global metrics-server via UDP (remote=127.0.0.1:9999)")
	flag.StringVar(&extraLabelsflag, "extra-labels", "", "coma separated metrics extra labels (extra-labels=label1:value2,label2:value2)")
	flag.StringVar(&labelsFilePath, "extra-labels-file-path", "", "read file with key=value labels (same as the java properties format)")
	flag.StringVar(&scylladbTable, "scylladb-table", "tracking.metrics", "scylladb table to write raw data")
	flag.StringVar(&hosts, "scylladb-hosts", "", "comma separated scylladb hosts")
	flag.StringVar(&lokiCaPath, "loki-ca-path", "", "path to loki ca certificate")
	flag.StringVar(&lokiCertPath, "loki-cert-path", "", "path to loki certificate")
	flag.StringVar(&lokiKeyPath, "loki-key-path", "", "path to loki key")
	flag.StringVar(&lokiAddress, "loki-address", "", "loki https endpoint")
	flag.StringVar(&pyroscopeUrl, "pyroscope-url", "", "pyroscope profiler address, e.g http://localhost:4040")
	flag.IntVar(&opensearchBulkSizeBytes, "opensearch-bulk-size-bytes", index.BulkSizeBytes, "bulk buffer size in bytes")
	flag.DurationVar(&shutdownDelay, "shutdown-delay", time.Second*0, "delay for graceful shutdown in case of kill signal or shutdown metric was received")
	flag.DurationVar(&metricTTL, "metric-ttl", time.Hour*24, "enable metric clean up via specified amount of time from http endpoint")
	flag.DurationVar(&opensearchBulkPeriod, "opensearch-bulk-period", time.Second*index.BulkPeriodSeconds, "bulk push period")
	flag.Parse()

	scylladbHosts = strings.Split(hosts, ",")

	// Loads labels from file
	if labelsFilePath != "" {
		labelsFile, err := os.Open(labelsFilePath)
		if err != nil {
			log.Errorf("Cannot read the labels file %v", labelsFilePath)
		}
		defer labelsFile.Close()
		reader := bufio.NewReader(labelsFile)
		for {
			var line string
			line, err = reader.ReadString('\n')
			// check if the line has = sign
			// and process the line. Ignore the rest.
			if equal := strings.Index(line, "="); equal >= 0 {
				if key := strings.TrimSpace(line[:equal]); len(key) > 0 {
					value := ""
					if len(line) > equal {
						value = strings.TrimSpace(line[equal+1:])
					}
					// assign the config map
					extraLabels[key] = strings.Replace(value, "\"", "", -1)
				}
			}
			if err == io.EOF {
				break
			}
		}
	}

	if len(extraLabelsflag) > 0 {
		extraLabelsKV := strings.Split(extraLabelsflag, ",")
		for _, kv := range extraLabelsKV {
			v := strings.Split(kv, ":")
			extraLabels[v[0]] = v[1]
		}
		log.Debugf("[LABELS] %v\n", extraLabels)
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
}

type App struct {
	RawData          chan RawDataMessage
	MetricBuffer     chan MetricBufferMessage
	LocalMetric      chan metric.Metric
	RemoteMetric     chan metric.Metric
	MetricRegexp     *regexp.Regexp
	RemoteServer     net.Conn
	MetricRegistry   MetricRegistry
	forwardedMetrics *prometheus.CounterVec
	processedMetrics *prometheus.CounterVec
	deletedMetrics   *prometheus.CounterVec
	indexConnections *prometheus.GaugeVec
	staledStorage    *prometheus.GaugeVec
	staledDuration   *prometheus.HistogramVec
}

type RawDataMessage struct {
	Data   string
	Remote string
}

type MetricBufferMessage struct {
	Data   string
	Remote string
}

type MetricRegistry struct {
	Counter       map[string]*prometheus.CounterVec
	Gauge         map[string]*prometheus.GaugeVec
	Histogram     map[string]*prometheus.HistogramVec
	Index         map[string]*index.OpenSearch
	Deadline      map[string]*MetricTTL
	CleanUpPeriod *time.Ticker
}

type MetricTTL struct {
	Name    string
	Type    string
	LabelKV LabelKV
	Time    time.Time
}

type LabelKV []string

func (kv *LabelKV) ToMap() map[string]string {
	if kv == nil {
		return nil
	}
	m := make(map[string]string, len(*kv)/2)
	for i := 0; i < len(*kv)-1; i += 2 {
		k := (*kv)[i]
		v := (*kv)[i+1]
		m[k] = v
	}
	return m
}

func (kv *LabelKV) ToLVI(name string) string {
	if kv == nil {
		return ""
	}
	return fmt.Sprintf("%v-%v", name, *kv)
}

func (kv *LabelKV) ToLI(name string) string {
	if kv == nil {
		return ""
	}
	var labelIndex []string
	for i := 0; i < len(*kv)-1; i += 2 {
		k := (*kv)[i]
		labelIndex = append(labelIndex, k)
	}
	return fmt.Sprintf("%v-%v", name, labelIndex)
}

func (r *MetricRegistry) AddDeadline(mTTL *MetricTTL) {
	if !metricCleanUp {
		return
	}
	log.Debugf("Add deadline for %+v", mTTL)
	r.Deadline[mTTL.LabelKV.ToLVI(mTTL.Name)] = mTTL
}

func (app *App) InitMetrics() {
	var err error
	app.forwardedMetrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "metrics_server_forwarded_metrics_total",
	}, []string{"type"})
	err = prometheus.Register(app.forwardedMetrics)
	if err != nil {
		log.Warnf("%v", err)
	}

	app.processedMetrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "metrics_server_processed_metrics_total",
	}, []string{"type"})
	err = prometheus.Register(app.processedMetrics)
	if err != nil {
		log.Warnf("%v", err)
	}

	app.deletedMetrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "metrics_server_deleted_metrics_total",
	}, []string{"type"})
	err = prometheus.Register(app.deletedMetrics)
	if err != nil {
		log.Warnf("%v", err)
	}

	app.indexConnections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metrics_server_index_connections",
	}, []string{})
	err = prometheus.Register(app.indexConnections)
	if err != nil {
		log.Warnf("%v", err)
	}

	app.staledStorage = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metrics_server_staled_storage_length",
	}, []string{})
	err = prometheus.Register(app.staledStorage)
	if err != nil {
		log.Warnf("%v", err)
	}

	app.staledDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metrics_server_staled_clean_up_duration_seconds",
		Buckets: prometheus.ExponentialBuckets(0.1, 10.0, 15),
	}, []string{})
	err = prometheus.Register(app.staledDuration)
	if err != nil {
		log.Warnf("%v", err)
	}
}

func (app *App) JsonExtractor(data string) [][]int {
	var openBracket, closeBracket int
	var startPosition, endPosition int
	var positions [][]int
	for i := 0; i < len(data); i++ {
		switch {
		case data[i] == '{':
			if openBracket == 0 {
				startPosition = i
			}
			openBracket += 1
		case data[i] == '}':
			closeBracket += 1
			if openBracket == closeBracket {
				openBracket = 0
				closeBracket = 0
				endPosition = i + 1
				positions = append(positions, []int{startPosition, endPosition})
			}
		case data[i] == '\n':
			openBracket = 0
			closeBracket = 0
			startPosition = 0
			endPosition = 0
		}
	}
	return positions
}

func (app *App) RemoteMetricProcessor() {
	for {
		m := <-app.RemoteMetric
		if m.Operation == "shutdown" {
			GracefulShutdown(shutdownDelay)
		} else {
			hostname, err := os.Hostname()
			if err != nil {
				log.Errorf("Cannot get server hostname, %v", err)
			}
			m.Labels["instance"] = hostname
			data, err := json.Marshal(m)
			if err != nil {
				log.Warnf("Cannot marshal json, %v", err)
			}
			_, err = app.RemoteServer.Write(data)
			if err != nil {
				log.Warnf("Send metric error, %v", err)
			}
		}
		app.forwardedMetrics.WithLabelValues(m.Operation).Inc()
	}
}

func (app *App) LocalMetricProcessor() {
	var err error
	var session *gocql.Session
	var loki *event.Loki

	for {
		m := <-app.LocalMetric
		labels := make([]string, 0, len(m.Labels))
		for l := range m.Labels {
			labels = append(labels, l)
		}
		sort.Strings(labels)
		var lv LabelKV
		for _, l := range labels {
			lv = append(lv, l, m.Labels[l])
		}

		sIndex := lv.ToLI(m.Name)
		log.Debugf("[STORAGE INDEX]: %v\n", sIndex)
		switch {
		case m.Operation == "shutdown":
			go GracefulShutdown(shutdownDelay)
		case m.Operation == "raw" && len(scylladbHosts) > 0:
			if session == nil {
				session, err = raw.ScyllaInit(scylladbHosts, scylladbTable, labels)
				if err != nil {
					log.Errorf("Scylla init error, %v", err)
				}
			}
			err = raw.ScyllaInsert(session, scylladbTable, m)
			if err != nil {
				log.Errorf("Scylla insert error, %v", err)
			}
		case m.Operation == "event":
			loki, err = event.NewLoki(lokiCaPath, lokiCertPath, lokiKeyPath, lokiAddress)
			if err != nil {
				log.Errorf("%v", err)
				break
			}
			loki.BulkBuffer <- m
		case m.Operation == "index":
			*m.Timestamp = time.Now().UTC()
			if m.IndexTemplate == "" {
				m.IndexName = fmt.Sprintf("%v-%v", DefaultMetricIndexTemplate, time.Now().UTC().Format("2006-01-02"))
			} else {
				m.IndexName = fmt.Sprintf("%v-%v", m.IndexTemplate, time.Now().UTC().Format("2006-01-02"))
			}
			if app.MetricRegistry.Index[m.IndexName] == nil {
				ctx, cancel := context.WithCancel(context.Background())
				app.MetricRegistry.Index[m.IndexName], err = index.NewOpenSearch(ctx, cancel, opensearchBulkPeriod, opensearchBulkSizeBytes)
				if err != nil {
					log.Errorf("Skip metric %+v, Cannot create connection to index storage backend, %v", m, err)
					continue
				}
				err = app.MetricRegistry.Index[m.IndexName].Init(m.IndexName)
				if err != nil {
					log.Errorf("Failed to init index, %v", err)
					continue
				}
				app.indexConnections.With(prometheus.Labels{}).Set(float64(len(app.MetricRegistry.Index)))
			}
			app.MetricRegistry.Index[m.IndexName].BulkBuffer <- m
		case m.Operation == "inc":
			app.ProcessCounter(sIndex, lv, labels, m)
		case m.Operation == "set":
			app.ProcessGauge(sIndex, lv, labels, m)
		case m.Operation == "obs":
			app.ProcessHistogram(sIndex, lv, labels, m)

		default:
			log.Warnf("Unknown metric format")
		}
		app.processedMetrics.WithLabelValues(m.Operation).Inc()
		app.CleanUpStaledMetrics()
	}
}

func (app *App) ProcessCounter(sIndex string, lv LabelKV, labels []string, m metric.Metric) {
	var err error
	if app.MetricRegistry.Counter[sIndex] == nil {
		log.Printf("Init new counter metric %v %v", m.Name, labels)
		app.MetricRegistry.Counter[sIndex] = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: m.Name,
				Help: m.Help,
			}, labels)
		if err = prometheus.Register(app.MetricRegistry.Counter[sIndex]); err != nil {
			log.Errorf("Counter metric %v %v is already registered, %v", m.Name, m.Labels, err)
		} else {
			app.MetricRegistry.Counter[sIndex].With(m.Labels).Add(m.Value)
			app.MetricRegistry.AddDeadline(&MetricTTL{
				Name:    m.Name,
				Type:    "inc",
				LabelKV: lv,
				Time:    time.Now().Add(metricTTL),
			})
		}
	} else {
		log.Debugf("Increment existing counter metric %v %v", m.Name, labels)
		app.MetricRegistry.Counter[sIndex].With(m.Labels).Add(m.Value)
		app.MetricRegistry.AddDeadline(&MetricTTL{
			Name:    m.Name,
			Type:    "inc",
			LabelKV: lv,
			Time:    time.Now().Add(metricTTL),
		})
	}
}

func (app *App) ProcessGauge(sIndex string, lv LabelKV, labels []string, m metric.Metric) {
	var err error
	if app.MetricRegistry.Gauge[sIndex] == nil {
		log.Printf("Init new gauge metric %v %v", m.Name, labels)
		app.MetricRegistry.Gauge[sIndex] = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: m.Name,
				Help: m.Help,
			}, labels)
		if err = prometheus.Register(app.MetricRegistry.Gauge[sIndex]); err != nil {
			log.Errorf("Gauge metric %v %v is already registered, %v", m.Name, m.Labels, err)
		} else {
			app.MetricRegistry.Gauge[sIndex].With(m.Labels).Set(m.Value)
			app.MetricRegistry.AddDeadline(&MetricTTL{
				Name:    m.Name,
				Type:    "set",
				LabelKV: lv,
				Time:    time.Now().Add(metricTTL),
			})
		}
	} else {
		log.Debugf("Set existing gauge metric %v %v", m.Name, labels)
		app.MetricRegistry.Gauge[sIndex].With(m.Labels).Set(m.Value)
		app.MetricRegistry.AddDeadline(&MetricTTL{
			Name:    m.Name,
			Type:    "set",
			LabelKV: lv,
			Time:    time.Now().Add(metricTTL),
		})
	}
}

func (app *App) ProcessHistogram(sIndex string, lv LabelKV, labels []string, m metric.Metric) {
	var err error
	if app.MetricRegistry.Histogram[sIndex] == nil {
		log.Printf("Init new histogram metric %v %v", m.Name, labels)
		width := m.Width
		if width == 0 {
			width = HistogramBucketWidth
		}
		count := m.Buckets
		if count == 0 {
			count = HistogramBucketCount
		}

		var buckets []float64
		switch {
		case m.Min > 0 && m.Max > 0:
			buckets = prometheus.ExponentialBucketsRange(m.Min, m.Max, count)
			log.Debugf("Min max buckets")
		case m.Factor > 0:
			buckets = prometheus.ExponentialBuckets(0.1, m.Factor, count)
			log.Debugf("Factor")
		case len(m.CustomBuckets) > 0:
			buckets = m.CustomBuckets
			log.Debugf("Custom buckets")
		default:
			buckets = prometheus.LinearBuckets(m.Start, width, count)
			log.Debugf("Default")
		}

		app.MetricRegistry.Histogram[sIndex] = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    m.Name,
				Help:    m.Help,
				Buckets: buckets,
			}, labels)
		if err = prometheus.Register(app.MetricRegistry.Histogram[sIndex]); err != nil {
			log.Errorf("Histogram metric %v %v is already registered, %v", m.Name, m.Labels, err)
		} else {
			app.MetricRegistry.Histogram[sIndex].With(m.Labels).Observe(m.Value)
			app.MetricRegistry.AddDeadline(&MetricTTL{
				Name:    m.Name,
				Type:    "obs",
				LabelKV: lv,
				Time:    time.Now().Add(metricTTL),
			})
		}
	} else {
		log.Debugf("Observe existing metric %v %v", m.Name, labels)
		app.MetricRegistry.Histogram[sIndex].With(m.Labels).Observe(m.Value)
		app.MetricRegistry.AddDeadline(&MetricTTL{
			Name:    m.Name,
			Type:    "obs",
			LabelKV: lv,
			Time:    time.Now().Add(metricTTL),
		})
	}
}

func (app *App) CleanUpStaledMetrics() {
	if !metricCleanUp {
		return
	}
	select {
	case <-app.MetricRegistry.CleanUpPeriod.C:
		t := time.Now()
		log.Printf("Start metric clean up, mem: %v", memAlloc())
		for _, mTTl := range app.MetricRegistry.Deadline {
			log.Debugf("Process %+v", mTTl)
			if time.Now().After(mTTl.Time) {
				switch mTTl.Type {
				case "inc":
					index := mTTl.LabelKV.ToLI(mTTl.Name)
					app.MetricRegistry.Counter[index].DeletePartialMatch(mTTl.LabelKV.ToMap())
					log.Warnf("Deleted staled counter metric %v, with labels %v", mTTl.Name, mTTl.LabelKV.ToLVI(mTTl.Name))
					delete(app.MetricRegistry.Deadline, mTTl.LabelKV.ToLVI(mTTl.Name))
					app.deletedMetrics.WithLabelValues(mTTl.Type).Inc()
				case "set":
					app.MetricRegistry.Gauge[mTTl.LabelKV.ToLI(mTTl.Name)].DeletePartialMatch(mTTl.LabelKV.ToMap())
					log.Warnf("Deleted staled gauge metric %v, with labels %v", mTTl.Name, mTTl.LabelKV.ToLVI(mTTl.Name))
					delete(app.MetricRegistry.Deadline, mTTl.LabelKV.ToLVI(mTTl.Name))
					app.deletedMetrics.WithLabelValues(mTTl.Type).Inc()
				case "obs":
					app.MetricRegistry.Histogram[mTTl.LabelKV.ToLI(mTTl.Name)].DeletePartialMatch(mTTl.LabelKV.ToMap())
					log.Warnf("Deleted staled histogram metric %v, with labels %v", mTTl.Name, mTTl.LabelKV.ToLVI(mTTl.Name))
					delete(app.MetricRegistry.Deadline, mTTl.LabelKV.ToLVI(mTTl.Name))
					app.deletedMetrics.WithLabelValues(mTTl.Type).Inc()
				default:
					log.Warnf("Staled unknown type of metric %v %+v", mTTl.LabelKV.ToLI(mTTl.Name), mTTl)
					delete(app.MetricRegistry.Deadline, mTTl.LabelKV.ToLVI(mTTl.Name))
					app.deletedMetrics.WithLabelValues(mTTl.Type).Inc()
				}
			}
		}
		//log.Debugf("Shrink deadline map")
		//app.ShrinkDeadlineMap()
		//app.ShrinkCounterMap()
		//app.ShrinkGaugeMap()
		//app.ShrinkHistogramMap()
		runtime.GC()
		eTime := time.Since(t)

		staledStorageLen := len(app.MetricRegistry.Deadline)
		log.Printf("Clean up exec time: %v, storage: %v, cregistry: %v, gregistry: %v, hregistry: %v, mem: %v",
			eTime.Seconds(), staledStorageLen, len(app.MetricRegistry.Counter),
			len(app.MetricRegistry.Gauge), len(app.MetricRegistry.Histogram), memAlloc())
		app.staledDuration.WithLabelValues().Observe(eTime.Seconds())
		app.staledStorage.WithLabelValues().Set(float64(staledStorageLen))

	default:
		return
	}
}

func memAlloc() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return fmt.Sprintf("%d KB", m.Alloc/1024)
}

func (app *App) ShrinkDeadlineMap() {
	newMap := make(map[string]*MetricTTL, StaledMetricsBuffer)
	for k, v := range app.MetricRegistry.Deadline {
		newMap[k] = v
	}
	app.MetricRegistry.Deadline = newMap
}

func (app *App) ShrinkCounterMap() {
	newMap := make(map[string]*prometheus.CounterVec, 100)
	for k, v := range app.MetricRegistry.Counter {
		newMap[k] = v
	}
	app.MetricRegistry.Counter = newMap
}

func (app *App) ShrinkGaugeMap() {
	newMap := make(map[string]*prometheus.GaugeVec, 100)
	for k, v := range app.MetricRegistry.Gauge {
		newMap[k] = v
	}
	app.MetricRegistry.Gauge = newMap
}

func (app *App) ShrinkHistogramMap() {
	newMap := make(map[string]*prometheus.HistogramVec, 100)
	for k, v := range app.MetricRegistry.Histogram {
		newMap[k] = v
	}
	app.MetricRegistry.Histogram = newMap
}

func (app *App) CollectorUDSServer() {
	_ = os.Remove(uds)
	log.Printf("Listen uds %v for metric collection", uds)
	l, err := net.Listen("unix", uds)
	if err != nil {
		log.Printf("Listen uds error. %v", err)
		return
	}
	err = os.Chmod(uds, 0777)
	if err != nil {
		log.Warnf("Set uds file attributes error. %v", err)
		//return
	}
	for {
		fd, err := l.Accept()
		if err != nil {
			log.Printf("Accept uds message error. %v", err)
			return
		}
		app.MetricCollector(fd, uds)
	}
}

func (app *App) CollectorUDSServerPool() {
	if udsPool == "" {
		log.Debugf("UDS pool is empty")
		return
	}
	unixSocketsPath := strings.Split(udsPool, ",")
	for _, udsPath := range unixSocketsPath {
		go func(udsPath string) {
			_ = os.Remove(udsPath)
			log.Printf("Listen uds %v for metric collection", udsPath)
			l, err := net.Listen("unix", udsPath)
			if err != nil {
				log.Printf("Listen uds error. %v", err)
				return
			}
			err = os.Chmod(udsPath, 0777)
			if err != nil {
				log.Warnf("Set uds file attributes error. %v", err)
			}
			for {
				fd, err := l.Accept()
				if err != nil {
					log.Printf("Accept uds message error. %v", err)
					return
				}
				app.MetricCollector(fd, udsPath)
			}
		}(udsPath)
	}

}

func (app *App) MetricCollector(c net.Conn, source string) {
	for {
		buf := make([]byte, ListenerBuffer)
		//c.RemoteAddr()
		nr, err := c.Read(buf)
		if err != nil {
			if err.Error() != "EOF" {
				log.Warnf("Unable to read new message. %v", err)
			}
			return
		}
		data := buf[0:nr]
		dataString := string(data)

		app.RawData <- RawDataMessage{
			Data:   dataString,
			Remote: source,
		}
	}
}

func (app *App) RawDataParser() {
	var trailer string
	for {
		select {
		case dataMessage := <-app.RawData:
			dataMessage.Data = trailer + dataMessage.Data
			log.Debugf("[RAW DATA]: %v\n", dataMessage.Data)
			log.Debugf("[RAW DATA LENGTH]: %v\n", len(dataMessage.Data))
			dataIndex := app.JsonExtractor(dataMessage.Data)
			log.Debugf("[METRIC INDEX]: %v\n", dataIndex)

			for i := 0; i < len(dataIndex); i++ {
				log.Debugf("FROM %v TO %v\n", dataIndex[i][0], dataIndex[i][1])

				app.MetricBuffer <- MetricBufferMessage{
					Data:   dataMessage.Data[dataIndex[i][0]:dataIndex[i][1]],
					Remote: dataMessage.Remote,
				}
				// If the last element can not be parsed try to save the rest of the message as a trailer for the second one
				if i == len(dataIndex)-1 && dataIndex[i][1] != len(dataMessage.Data) && dataMessage.Data[dataIndex[i][1]:] != "\n" {
					trailer = dataMessage.Data[dataIndex[i][1]:]
					log.Warnf("Append trailer to the next message, trailer size: %v, body: %v", len(trailer), trailer)
				}
			}
		}
	}
}

func extractSourceAppName(str string) string {
	re := regexp.MustCompile(`(?:^|\/|\.)?(\w+(?:-\w+)*)\.sock$`)
	matches := re.FindStringSubmatch(str)
	if len(matches) < 2 {
		return str
	}
	return matches[1]
}

func (app *App) MetricParser() {
	for {
		select {
		case dataMessage := <-app.MetricBuffer:
			m := metric.Metric{Labels: make(map[string]string, 20)}
			log.Debugf("[RAW PARSED METRIC]: %v\n", dataMessage.Data)
			err := json.Unmarshal([]byte(dataMessage.Data), &m)
			if err != nil {
				log.Warnf("Metric %v unmarshal error, %v", dataMessage.Data, err)
			}
			log.Debugf("[RECEIVED]: %+v\n", m)
			for k, v := range extraLabels {
				m.Labels[k] = v
			}
			m.Labels["data_source"] = dataMessage.Remote
			m.Labels["source_app"] = extractSourceAppName(dataMessage.Remote)

			switch {
			case remote != "":
				app.RemoteMetric <- m
			default:
				app.LocalMetric <- m
			}
		}
	}
}

func (app *App) CollectorUDPServer() {
	log.Printf("Listen udp %v for metric collection", udp)

	//Build the address
	udpAddr, err := net.ResolveUDPAddr("udp", udp)
	if err != nil {
		log.Errorf("Wrong Address")
		return
	}

	//Create the connection
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fmt.Println(err)
	}

	//Keep calling this function
	for {
		var buf [ListenerBuffer]byte
		n, err := udpConn.Read(buf[0:])
		if err != nil {
			log.Errorf("Error Reading")
			return
		} else {
			log.Debugf("[UDP DATAGRAM] %v", string(buf[0:n]))
		}

		app.RawData <- RawDataMessage{
			Data: string(buf[0:n]), Remote: udp,
		}
	}
}

func (app *App) MetricServer() {
	log.Printf("Expose metrics on http://0.0.0.0%v/metrics", socket)
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if ready {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	err := http.ListenAndServe(socket, nil)
	if err != nil {
		log.Errorf("%v", err)
	}
}

func (app *App) SignalHandler() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	sig := <-signals
	//close(signals)
	log.Printf("Received signal: %v", sig)
	GracefulShutdown(shutdownDelay)
}

func GracefulShutdown(shutdownDelay time.Duration) {
	ready = false
	log.Printf("Shutting down")
	time.Sleep(shutdownDelay)
	var udsList []string
	if udsPool != "" {
		udsList = strings.Split(udsPool, ",")
	}
	udsList = append(udsList, uds)
	for _, udsPath := range udsList {
		err := os.Remove(udsPath)
		if err != nil {
			log.Errorf("Cannot delete uds file %v", err)
		}
	}
	os.Exit(0)
}

func StartApp() {
	InitFlags()
	if pyroscopeUrl != "" {
		Pyroscope()
	}
	app := App{}
	app.RawData = make(chan RawDataMessage, 512)
	app.MetricBuffer = make(chan MetricBufferMessage, 1)
	app.LocalMetric = make(chan metric.Metric, 0)
	app.RemoteMetric = make(chan metric.Metric, 0)
	lock := make(chan struct{}, 0)
	app.MetricRegistry.Counter = make(map[string]*prometheus.CounterVec, 100)
	app.MetricRegistry.Gauge = make(map[string]*prometheus.GaugeVec, 100)
	app.MetricRegistry.Histogram = make(map[string]*prometheus.HistogramVec, 100)
	app.MetricRegistry.Index = make(map[string]*index.OpenSearch)
	if metricCleanUp {
		app.MetricRegistry.Deadline = make(map[string]*MetricTTL, StaledMetricsBuffer)
		app.MetricRegistry.CleanUpPeriod = time.NewTicker(metricTTL + (metricTTL / 100 * 1))
	}

	if len(remote) > 0 {
		var err error
		app.RemoteServer, err = net.Dial("udp", remote)
		if err != nil {
			log.Errorln(err)
			return
		}
		defer app.RemoteServer.Close()
	}

	app.InitMetrics()
	go app.SignalHandler()
	go app.MetricServer()
	go app.RawDataParser()
	go app.MetricParser()
	go app.LocalMetricProcessor()
	go app.RemoteMetricProcessor()
	go app.CollectorUDPServer()
	go app.CollectorUDSServerPool()
	go app.CollectorUDSServer()

	ready = true
	lock <- struct{}{}
}

func Pyroscope() {
	// These 2 lines are only required if you're using mutex or block profiling
	// Read the explanation below for how to set these rates:
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)

	pyroscope.Start(pyroscope.Config{
		ApplicationName: "metrics-server",

		// replace this with the address of pyroscope server
		ServerAddress: pyroscopeUrl,

		// you can disable logging by setting this to nil
		Logger: pyroscope.StandardLogger,

		// optionally, if authentication is enabled, specify the API key:
		// AuthToken:    os.Getenv("PYROSCOPE_AUTH_TOKEN"),

		// you can provide static tags via a map:
		Tags: map[string]string{"hostname": os.Getenv("HOSTNAME")},

		ProfileTypes: []pyroscope.ProfileType{
			// these profile types are enabled by default:
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,

			// these profile types are optional:
			pyroscope.ProfileGoroutines,
			pyroscope.ProfileMutexCount,
			pyroscope.ProfileMutexDuration,
			pyroscope.ProfileBlockCount,
			pyroscope.ProfileBlockDuration,
		},
	})
}
