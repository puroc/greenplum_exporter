package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  获取表和索引占用的自盘空间
 */

const (
	tableAndIndexDiskSql = `select sotaidschemaname,sotaidtablename,sotaidtablesize,sotaididxsize from gp_toolkit.gp_size_of_table_and_indexes_disk`
)

var (
	tableDiskSizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "table_size"),
		"table size of GreenPlum cluster at scrape time",
		[]string{"schema", "table"},
		nil,
	)
	indexDiskSizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "index_size"),
		"index size of GreenPlum cluster at scrape time",
		[]string{"schema", "table"},
		nil,
	)
)

func NewTableIndexDiskScraper() Scraper {
	return &tableIndexDiskScraper{}
}

type tableIndexDiskScraper struct{}

func (tableIndexDiskScraper) Name() string {
	return "tableIndexDiskScraper"
}

func (tableIndexDiskScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric) error {
	rows, err := db.Query(tableAndIndexDiskSql)
	logger.Infof("Query Database: %s", tableAndIndexDiskSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var schemaName, tableName string
		var tableSize, indexSize float64
		err = rows.Scan(&schemaName, &tableName, &tableSize, &indexSize)
		if err != nil {
			logger.Errorf("table index disk, error:%v", err.Error())
			return err
		}
		ch <- prometheus.MustNewConstMetric(tableDiskSizeDesc, prometheus.GaugeValue, tableSize, schemaName, tableName)
		ch <- prometheus.MustNewConstMetric(indexDiskSizeDesc, prometheus.GaugeValue, indexSize, schemaName, tableName)
	}

	return combineErr(err)
}
