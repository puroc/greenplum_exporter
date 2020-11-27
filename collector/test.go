package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  集群状态抓取器
 */

const (
	testCheckStateSql = `SELECT count(1) from gp_dist_random('gp_id')`
)

var (
	testStateDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "testhaha"),
		"Whether the GreenPlum database is accessible",
		[]string{"version", "master", "standby"},
		nil,
	)
)

func NewTestScraper() Scraper {
	return &testScraper{}
}

type testScraper struct{}

func (testScraper) Name() string {
	return "test_scraper"
}

func (testScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric) error {
	rows, err := db.Query(testCheckStateSql)
	logger.Infof("Query Database: %s", testCheckStateSql)

	if err != nil {
		ch <- prometheus.MustNewConstMetric(stateDesc, prometheus.GaugeValue, 0, "", "")
		logger.Errorf("get metrics for scraper, error:%v", err.Error())
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var count int
		err = rows.Scan(&count)
		if err != nil {
			ch <- prometheus.MustNewConstMetric(testStateDesc, prometheus.GaugeValue, 222, "", "")
			logger.Errorf("test, error:%v", err.Error())
			return err
		}
	}

	version, errV := scrapeVersion(db)
	master, errM := scrapeMaster(db)
	standby, errX := scrapeStandby(db)

	ch <- prometheus.MustNewConstMetric(stateDesc, prometheus.GaugeValue, 111, version, master, standby)

	return combineErr(errM, errV, errX)
}
