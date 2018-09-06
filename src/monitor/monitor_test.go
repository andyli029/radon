/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package monitor

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func TestClientConnectionIncDec(t *testing.T) {
	user := "andy"
	ClientConnectionInc(user)

	var m dto.Metric
	g, _ := clientConnectionNum.GetMetricWithLabelValues(user)
	g.Write(&m)
	v := m.GetGauge().GetValue()

	assert.EqualValues(t, 1, v)

	ClientConnectionDec(user)

	g, _ = clientConnectionNum.GetMetricWithLabelValues(user)
	g.Write(&m)
	v = m.GetGauge().GetValue()

	assert.EqualValues(t, 0, v)
}

func TestBackendIncDec(t *testing.T) {
	getBackendNum := func(btype string) float64 {
		var m dto.Metric
		g, _ := backendNum.GetMetricWithLabelValues(btype)
		g.Write(&m)
		return m.GetGauge().GetValue()
	}

	backend := "backend"
	backup := "backup"

	BackendInc(backend)
	BackendInc(backup)

	v1 := getBackendNum(backend)
	v2 := getBackendNum(backup)

	assert.EqualValues(t, 1, v1)
	assert.EqualValues(t, 1, v2)

	BackendDec(backend)
	BackendDec(backup)

	v1 = getBackendNum(backend)
	v2 = getBackendNum(backup)

	assert.EqualValues(t, 0, v1)
	assert.EqualValues(t, 0, v2)
}

func TestDiskUsageSet(t *testing.T) {
	v := 0.35

	DiskUsageSet(v)

	var m dto.Metric
	g, _ := diskUsage.GetMetricWithLabelValues("percent")
	g.Write(&m)
	r := m.GetGauge().GetValue()

	assert.EqualValues(t, v, r)
}
