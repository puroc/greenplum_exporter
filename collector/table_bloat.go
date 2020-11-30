package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  获取膨胀的表
 */

const (
	tableBloatSql = `select bdinspname,bdirelname,bdirelpages,bdiexppages,bdidiag from gp_toolkit.gp_bloat_diag order by bdirelpages desc, bdidiag`
)

var (
	tableBloatDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "table_bloat"),
		"table bloat of GreenPlum cluster at scrape time",
		[]string{"schema", "table", "expect_size", "desc"},
		nil,
	)
)

func NewTableBloatScraper() Scraper {
	return &tableBloatScraper{}
}

type tableBloatScraper struct{}

func (tableBloatScraper) Name() string {
	return "tableSkewScraper"
}

func (tableBloatScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric) error {
	rows, err := db.Query(tableBloatSql)
	logger.Infof("Query Database: %s", tableBloatSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var schemaName, tableName, expectSize, desc string
		var realSize float64
		err = rows.Scan(&schemaName, &tableName, &realSize, &expectSize, &desc)
		if err != nil {
			logger.Errorf("table skew, error:%v", err.Error())
			return err
		}
		ch <- prometheus.MustNewConstMetric(tableBloatDesc, prometheus.GaugeValue, realSize, schemaName, tableName, expectSize, desc)
	}

	return combineErr(err)
}
