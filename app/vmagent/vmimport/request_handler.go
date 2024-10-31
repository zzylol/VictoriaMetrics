package vmimport

import (
	"net/http"

	"github.com/VictoriaMetrics/metrics"
	"github.com/zzylol/VictoriaMetrics/app/vmagent/common"
	"github.com/zzylol/VictoriaMetrics/app/vmagent/remotewrite"
	"github.com/zzylol/VictoriaMetrics/lib/auth"
	"github.com/zzylol/VictoriaMetrics/lib/bytesutil"
	"github.com/zzylol/VictoriaMetrics/lib/logger"
	"github.com/zzylol/VictoriaMetrics/lib/prompbmarshal"
	parserCommon "github.com/zzylol/VictoriaMetrics/lib/protoparser/common"
	parser "github.com/zzylol/VictoriaMetrics/lib/protoparser/vmimport"
	"github.com/zzylol/VictoriaMetrics/lib/protoparser/vmimport/stream"
	"github.com/zzylol/VictoriaMetrics/lib/tenantmetrics"
)

var (
	rowsInserted       = metrics.NewCounter(`vmagent_rows_inserted_total{type="vmimport"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vmagent_tenant_inserted_rows_total{type="vmimport"}`)
	rowsPerInsert      = metrics.NewHistogram(`vmagent_rows_per_insert{type="vmimport"}`)
)

// InsertHandler processes `/api/v1/import` request.
//
// See https://github.com/zzylol/VictoriaMetrics/issues/6
func InsertHandler(at *auth.Token, req *http.Request) error {
	extraLabels, err := parserCommon.GetExtraLabels(req)
	if err != nil {
		return err
	}
	isGzipped := req.Header.Get("Content-Encoding") == "gzip"
	return stream.Parse(req.Body, isGzipped, func(rows []parser.Row) error {
		return insertRows(at, rows, extraLabels)
	})
}

func insertRows(at *auth.Token, rows []parser.Row, extraLabels []prompbmarshal.Label) error {
	ctx := common.GetPushCtx()
	defer common.PutPushCtx(ctx)

	rowsTotal := 0
	tssDst := ctx.WriteRequest.Timeseries[:0]
	labels := ctx.Labels[:0]
	samples := ctx.Samples[:0]
	for i := range rows {
		r := &rows[i]
		rowsTotal += len(r.Values)
		labelsLen := len(labels)
		for j := range r.Tags {
			tag := &r.Tags[j]
			labels = append(labels, prompbmarshal.Label{
				Name:  bytesutil.ToUnsafeString(tag.Key),
				Value: bytesutil.ToUnsafeString(tag.Value),
			})
		}
		labels = append(labels, extraLabels...)
		values := r.Values
		timestamps := r.Timestamps
		if len(timestamps) != len(values) {
			logger.Panicf("BUG: len(timestamps)=%d must match len(values)=%d", len(timestamps), len(values))
		}
		samplesLen := len(samples)
		for j, value := range values {
			samples = append(samples, prompbmarshal.Sample{
				Value:     value,
				Timestamp: timestamps[j],
			})
		}
		tssDst = append(tssDst, prompbmarshal.TimeSeries{
			Labels:  labels[labelsLen:],
			Samples: samples[samplesLen:],
		})
	}
	ctx.WriteRequest.Timeseries = tssDst
	ctx.Labels = labels
	ctx.Samples = samples
	if !remotewrite.TryPush(at, &ctx.WriteRequest) {
		return remotewrite.ErrQueueFullHTTPRetry
	}
	rowsInserted.Add(rowsTotal)
	if at != nil {
		rowsTenantInserted.Get(at).Add(rowsTotal)
	}
	rowsPerInsert.Update(float64(rowsTotal))
	return nil
}
