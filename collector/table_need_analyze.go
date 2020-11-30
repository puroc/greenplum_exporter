package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  需要analyze的表。smisize为t的表，analyze才有效果。
 */

const (
	tableNeedAnalyzeSql = `SELECT smischema,smitable FROM gp_toolkit.gp_stats_missing where smisize = 't'`
)

var (
	tableNeedAnalyzeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "table_need_analyze"),
		"table need analyze of GreenPlum cluster at scrape time",
		[]string{"schema", "table"},
		nil,
	)
)

func NewTableNeedAnalyzeScraper() Scraper {
	return &tableNeedAnalyzeScraper{}
}

type tableNeedAnalyzeScraper struct{}

func (tableNeedAnalyzeScraper) Name() string {
	return "tableNeedAnalyzeScraper"
}

func (tableNeedAnalyzeScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric) error {
	rows, err := db.Query(tableNeedAnalyzeSql)
	logger.Infof("Query Database: %s", tableNeedAnalyzeSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var smischema, smitable string
		err = rows.Scan(&smischema, &smitable)
		if err != nil {
			logger.Errorf("table need analyze, error:%v", err.Error())
			return err
		}
		ch <- prometheus.MustNewConstMetric(tableNeedAnalyzeDesc, prometheus.GaugeValue, 1, smischema, smitable)
	}

	return combineErr(err)
}
