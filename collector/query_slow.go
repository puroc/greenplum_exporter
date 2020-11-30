package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  获取慢查询SQL的前10条
 */

const (
	slowQuerySql = `SELECT
	datname,
	usename,
	client_addr,
	application_name,
	STATE,
	backend_start,
	xact_start,
	xact_stay,
	query_start,
	query_stay,
	REPLACE ( QUERY, chr( 10 ), ' ' ) AS QUERY 
FROM
	(
	SELECT
		pgsa.datname AS datname,
		pgsa.usename AS usename,
		pgsa.client_addr client_addr,
		pgsa.application_name AS application_name,
		pgsa.STATE AS STATE,
		pgsa.backend_start AS backend_start,
		pgsa.xact_start AS xact_start,
		EXTRACT ( epoch FROM ( NOW( ) - pgsa.xact_start ) ) AS xact_stay,
		pgsa.query_start AS query_start,
		EXTRACT ( epoch FROM ( NOW( ) - pgsa.query_start ) ) AS query_stay,
		pgsa.query AS QUERY 
	FROM
		pg_stat_activity AS pgsa 
	WHERE
		pgsa.STATE != 'idle' 
		AND pgsa.STATE != 'idle in transaction' 
		AND pgsa.STATE != 'idle in transaction (aborted)' 
	) idleconnections 
ORDER BY
	query_stay DESC 
	LIMIT 10;`
)

var (
	slowQueryDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "slow_query"),
		"slow query of GreenPlum cluster at scrape time",
		[]string{"db", "user", "client", "app_name", "query_start", "state", "sql"},
		nil,
	)
)

func NewSlowQueryScraper() Scraper {
	return &slowQueryScraper{}
}

type slowQueryScraper struct{}

func (slowQueryScraper) Name() string {
	return "slowQueryScraper"
}

func (slowQueryScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric) error {
	rows, err := db.Query(slowQuerySql)
	logger.Infof("Query Database: %s", slowQuerySql)

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var db, usename, client_addr, application_name, state, backend_start, xact_start, xact_stay, query_start, query string
		var query_stay float64
		err = rows.Scan(&db, &usename, &client_addr, &application_name, &state, &backend_start, &xact_start, &xact_stay, &query_start, &query_stay, &query)
		if err != nil {
			logger.Errorf("table skew, error:%v", err.Error())
			return err
		}
		ch <- prometheus.MustNewConstMetric(slowQueryDesc, prometheus.GaugeValue, query_stay, db, usename, client_addr, application_name, query_start, state, query)
	}

	return combineErr(err)
}
