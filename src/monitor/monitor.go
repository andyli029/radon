/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package monitor

import (
	"net/http"

	"config"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	clientConnectionNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "connection_number_client",
			Help: "client connection Number",
		},
		[]string{"user"},
	)

	backendConnectionNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "connection_number_backend",
			Help: "backend connection Number",
		},
		[]string{"address"},
	)

	queryTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "query_total",
			Help: "Counter of queries.",
		},
		[]string{"command", "result"},
	)
)

func init() {
	prometheus.MustRegister(clientConnectionNum)
	prometheus.MustRegister(backendConnectionNum)
	prometheus.MustRegister(queryTotalCounter)
}

// Start monitor
func Start(log *xlog.Log, monitorConf *config.MonitorConfig) {
	webMonitorAddr := monitorConf.WebMonitorAddr
	webMonitorPort := monitorConf.WebMonitorPort
	webMonitorURL := monitorConf.WebMonitorURL

	log.Info("[prometheus metrics]:\thttp://{%s}:%s%s\n",
		webMonitorAddr, webMonitorPort, webMonitorURL)
	log.Info("[pprof web]:\t\thttp://{%s}:%s/debug/pprof/\n",
		webMonitorAddr, webMonitorPort)

	http.Handle(webMonitorURL, promhttp.Handler())
	go http.ListenAndServe(webMonitorAddr+":"+webMonitorPort, nil)
}

// ClientConnectionInc add 1
func ClientConnectionInc(user string) {
	clientConnectionNum.WithLabelValues(user).Inc()
}

// ClientConnectionDec dec 1
func ClientConnectionDec(user string) {
	clientConnectionNum.WithLabelValues(user).Dec()
}

// BackendConnectionInc add 1
func BackendConnectionInc(address string) {
	backendConnectionNum.WithLabelValues(address).Inc()
}

// BackendConnectionDec dec 1
func BackendConnectionDec(address string) {
	backendConnectionNum.WithLabelValues(address).Dec()
}

//QueryTotalCounterInc add 1
func QueryTotalCounterInc(command string, result string) {
	queryTotalCounter.WithLabelValues(command, result).Inc()
}
