package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  获取数据倾斜
 */

const (
	tableSkewSql = `select sifnamespace,sifrelname,siffraction from gp_toolkit.gp_skew_idle_fractions order by siffraction desc`
)

var (
	tableSkewDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "table_skew"),
		"table skew of GreenPlum cluster at scrape time",
		[]string{"schema", "table"},
		nil,
	)
)

func NewTableSkewScraper() Scraper {
	return &tableSkewScraper{}
}

type tableSkewScraper struct{}

func (tableSkewScraper) Name() string {
	return "tableSkewScraper"
}

func (tableSkewScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric) error {
	rows, err := db.Query(tableSkewSql)
	logger.Infof("Query Database: %s", tableSkewSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var schemaName, tableName string
		var tableSkew float64
		err = rows.Scan(&schemaName, &tableName, &tableSkew)
		if err != nil {
			logger.Errorf("table skew, error:%v", err.Error())
			return err
		}
		ch <- prometheus.MustNewConstMetric(tableSkewDesc, prometheus.GaugeValue, tableSkew, schemaName, tableName)
	}

	return combineErr(err)
}
