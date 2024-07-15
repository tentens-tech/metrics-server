# metrics-server
The server that collects metrics from short-lived applications or legacy applications like PHP, which cannot keep the state.

This tool supports all types of metrics, including counters, gauges, and histograms. It is similar to Pushgateway but with broader support for metric types.

## Run server
```sh
go run main.go
```

## Server options
```sh
go run main.go -h
  -debug
    	enable debug log level
  -extra-labels string
    	coma separated metrics extra labels (extra-labels=label1:value2,label2:value2)
  -extra-labels-file-path string
    	read file with key=value labels (same as the java properties format)
  -loki-address string
    	loki https endpoint
  -loki-ca-path string
    	path to loki ca certificate
  -loki-cert-path string
    	path to loki certificate
  -loki-key-path string
    	path to loki key
  -metric-clean-up
    	enable clean up metric via metric-ttl time after creation
  -metric-ttl duration
    	enable metric clean up via specified amount of time from http endpoint (default 24h0m0s)
  -opensearch-bulk-period duration
    	bulk push period (default 5s)
  -opensearch-bulk-size-bytes int
    	bulk buffer size in bytes (default 20000)
  -pyroscope-url string
    	pyroscope profiler address, e.g http://localhost:4040
  -remote string
    	send metrics to global metrics-server via UDP (remote=127.0.0.1:9999)
  -scylladb-hosts string
    	comma separated scylladb hosts
  -scylladb-table string
    	scylladb table to write raw data (default "tracking.metrics")
  -shutdown-delay duration
    	delay for graceful shutdown in case of kill signal or shutdown metric was received
  -socket string
    	prometheus exporter tcp socket (default ":9099")
  -udp string
    	collector udp socket (default ":9999")
  -uds string
    	path to collector unix domain socket file (default "./metric-server.sock")
  -uds-pool string
    	comma separated list of unix domain sockets
```

## Env variables
```shell
METRICS_SERVER_SEARCH_URL=https://10.30.18.4:9200
METRICS_SERVER_SEARCH_USER=admin
METRICS_SERVER_SEARCH_PASSWORD=admin
```

## OpenSearch usage
```shell
go build metrics-server && METRICS_SERVER_SEARCH_URL=https://localhost:9200 METRICS_SERVER_SEARCH_USER=admin METRICS_SERVER_SEARCH_PASSWORD=admin ./metrics-server -opensearch-bulk-period=5s --opensearch-bulk-size-bytes=1000000
```


## Run go client
```sh
cd client
go run main.go
```

## Run php client
```sh
cd client
php client.php
```

## Send metric using netcat via UDS
```sh
cd client
cat metrics.json | nc -U ../metric-server.sock -w 1
```

## Send loki events using netcat via UDS
```sh
cd client
cat loki.json | nc -U ../metric-server.sock -w 1
```

## Send metric using netcat via UDP
```sh
cd client
cat metrics.json | nc -u 127.0.0.1 9999 -w 1
```

# Send metric via socat to metrics-server on Alpine Linux
```sh
apk update
apk add socat
echo '{"name":"metric","labels":{"label1":"labelvalue1"},"op":"event","value":1.0}' | socat - UNIX-CONNECT:/tmp/metrics-server/metrics-server.sock
```

## Graceful shutdown metrics server to use in cronjobs (-shutdown-delay option must be >30s)
```sh
echo '{"name":"shuthdown","op":"shutdown","value":0}' | nc -U ../metric-server.sock -w 1
```

## Collect metrics via metrics router
```sh
go run main.go -shutdown-delay 30s -uds ./metric-server-global.sock &
go run main.go -shutdown-delay 30s -remote 127.0.0.1:9999 -udp ":9998" -socket ":9098" -extra-labels app:marketing-cron,instance:$(hostname) &
cd client
go run main.go
```

## Get metrics
```sh
curl -s localhost:9099/metrics
```

## Metric message format

### For [counters](https://prometheus.io/docs/concepts/metric_types/#counter)
`{"name":"metric_name","help":"metric description","labels":{"label1":"labelvalue1"},"op":"inc","value":1.0}`

### For [gauges](https://prometheus.io/docs/concepts/metric_types/#gauge)
`{"name":"metric_name","help":"metric description","labels":{"label1":"labelvalue1"},"op":"set","value":1.0}`

### For [histograms](https://prometheus.io/docs/concepts/metric_types/#histogram)
`{"name":"metric_request_duration_ms","labels":{"label1":"labelvalue1"},"op":"obs","value":2.0}`

with bucket width:

`{"name":"metric_request_duration_ms","labels":{"label1":"labelvalue1"},"op":"obs","buckets":10,"bucket_width":5,"value":2.0}`

exponential min/max buckets:
`{"name":"metric_request_duration_ms_minmax","labels":{"label1":"labelvalue1"},"op":"obs","buckets":10,"bucket_min":1.0,"bucket_max":70.0,"value":20.0}`

exponential factor buckets:
`{"name":"metric_request_duration_ms_factor","labels":{"label1":"labelvalue1"},"op":"obs","buckets":10,"bucket_factor":10.1,"value":1.0}`

custom buckets:
`{"name":"metric_request_duration_ms_custom","labels":{"label1":"labelvalue1"},"op":"obs","bucket_custom":[1.0, 2.0, 5.5, 7, 10, 50],"value":20.0}`

Bucket params:
- buckets - creates specified number of buckets. Default value is 20
- bucket_width - bucket width where the lowest bucket has an upper bound of 0. Default value is 10

## Restrictions
- Metric can be registered only once and with only one set of labels. For example, sending these will throw an error:
```
{"name":"metric_name","help":"metric description","labels":{"label1":"labelvalue1"},"op":"inc","value":1.0}
{"name":"metric_name","help":"metric description","labels":{"label1":"labelvalue1","label2":"labelvalue2"},"op":"inc","value":1.0}
```


## ScyllaDB

### Docker
```sh
docker run  --rm --name some-scylla -p 7000:7000 -p 7001:7001 -p 9042:9042 -p 9160:9160 -p 10000:10000 --hostname some-scylla -it scylladb/scylla
docker run --rm -p 3000:3000 --name metabase --hostname metabase -it metabase/metabase
```

```sql
DESC SCHEMA;
CREATE KEYSPACE tracking WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor' : 1};
use tracking;
    
CREATE TABLE metrics (
                                name text,
                                labels frozen<map<text, text>>,
                                timestamp timestamp,
                                value double,
                                primary key((name), timestamp))
       WITH CLUSTERING ORDER BY (timestamp DESC)
       AND COMPACTION = {'class': 'TimeWindowCompactionStrategy',
           'base_time_seconds': 3600,
           'max_sstable_age_days': 1};
CREATE INDEX on metrics (FULL(labels));
INSERT INTO tracking.metrics ("name","labels","timestamp","value") VALUES ('metric_raw',{'status':'success'},'2022-09-30 08:07+0000',1.0);
INSERT INTO tracking.metrics ("name","labels","timestamp","value") VALUES ('metric_raw',{'status':'failed'},'2022-09-30 08:08+0000',1.0);
INSERT INTO tracking.metrics ("name","labels","timestamp","value") VALUES ('metric_raw',{'status':'skipped','job':'k8s'},'2022-09-30 08:09+0000',1.0);
SELECT * FROM tracking.metrics where name = 'metric_raw' and labels = {'status': 'success'} and timestamp<'2022-10-14 08:07:00';
```