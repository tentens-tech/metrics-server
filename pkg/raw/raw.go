package raw

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"metrics-server/pkg/metric"
	"strings"
	"time"
)

func ScyllaInit(hosts []string, table string, labels []string) (*gocql.Session, error) {
	scyllaCluster := gocql.NewCluster(hosts...)
	scyllaCluster.Consistency = gocql.One
	scyllaCluster.ProtoVersion = 4
	log.Warnf("LABELS: %v", labels)
	log.Printf("Create session to scylladb")
	session, err := scyllaCluster.CreateSession()
	if err != nil {
		log.Errorf("%v", err)
	}
	//defer session.Close()

	keyspace := strings.Split(table, ".")[0]

	var stmt string

	stmt = fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %v WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor' : 1}", keyspace)
	err = session.Query(stmt).WithContext(context.Background()).Consistency(gocql.One).Exec()
	if err != nil {
		log.Errorf("Create keyspace error, %v", err)
		return nil, err
	}

	scyllaCluster.Keyspace = keyspace

	var (
		stmtHeader, stmtBody, stmtPrimaryKey, stmtTrailer string
	)

	stmtHeader = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %v (
                                name text,
                                labels frozen<map<text, text>>,
                                timestamp timestamp,
                                value double,
		`, table)

	for i, _ := range labels {
		if i == 0 {
			stmtBody = stmtBody + fmt.Sprintf("                %v text,\n", labels[i])
		} else {
			stmtBody = stmtBody + fmt.Sprintf("                                %v text,\n", labels[i])
		}
	}

	stmtPrimaryKey = fmt.Sprintf("                         primary key((name, labels, %v), timestamp))",
		func(l []string) string {
			var pk string
			for i, _ := range l {
				if i == len(labels)-1 {
					pk = pk + l[i]
				} else {
					pk = pk + l[i] + ", "
				}
			}
			return pk
		}(labels))

	stmtTrailer = fmt.Sprintf(`
       WITH CLUSTERING ORDER BY (timestamp DESC)
       AND COMPRESSION = {'sstable_compression': 'LZ4Compressor'}
       AND COMPACTION = {'class': 'TimeWindowCompactionStrategy',
           'base_time_seconds': 3600,
           'max_sstable_age_days': 1}`)

	stmt = stmtHeader + stmtBody + stmtPrimaryKey + stmtTrailer
	log.Warnf("[STMT]: %v", stmt)

	stmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %v (
                                name text,
                                labels frozen<map<text, text>>,
                                timestamp timestamp,
                                value double,
                                primary key((name, labels), timestamp))
       WITH CLUSTERING ORDER BY (timestamp DESC)
       AND COMPRESSION = {'sstable_compression': 'LZ4Compressor'}
       AND COMPACTION = {'class': 'TimeWindowCompactionStrategy',
           'base_time_seconds': 3600,
           'max_sstable_age_days': 1}`, table)
	err = session.Query(stmt).WithContext(context.Background()).Consistency(gocql.One).Exec()
	if err != nil {
		log.Errorf("Create table error, %v", err)
		return nil, err
	}
	stmt = fmt.Sprintf("CREATE INDEX IF NOT EXISTS on %v (FULL(labels))", table)
	err = session.Query(stmt).WithContext(context.Background()).Consistency(gocql.One).Exec()
	if err != nil {
		log.Errorf("Create index error, %v", err)
		return nil, err
	}
	return session, nil
}

func ScyllaInsert(session *gocql.Session, table string, m metric.Metric) error {
	var err error
	var stmt string
	stmt = fmt.Sprintf("INSERT INTO %v (name, labels, timestamp, value) VALUES (?,?,?,?)", table)
	err = session.Query(stmt,
		m.Name, m.Labels, time.Now(), m.Value).WithContext(context.Background()).Consistency(gocql.One).Exec()
	if err != nil {
		log.Errorf("INSERT %v", err)
		return err
	}

	return nil
}
