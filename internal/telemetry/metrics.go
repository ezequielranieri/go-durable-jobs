package telemetry

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ezequielranieri/go-durable-jobs/internal/application"
)

type Metrics struct {
	jobsEnqueued  prometheus.Counter
	jobsProcessed *prometheus.CounterVec
	jobsInFlight  prometheus.Gauge
	jobProcessing prometheus.Histogram
	handler       http.Handler
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	jobsEnqueued := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "jobs_enqueued_total",
		Help: "Total de jobs nuevos creados (duplicados idempotentes no cuentan).",
	})

	// Definición acordada: el label result=failed incluye tanto fallos de
	// negocio del handler como fallos de persistencia posterior
	// (MarkCompleted/MarkFailed fallando). Ambos son intentos de Execute que
	// no se resolvieron limpio. Un dashboard que mire jobs_processed_total
	// no debe asumir que failed implica un bug de negocio.
	jobsProcessed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_processed_total",
		Help: "Total de ejecuciones de ProcessJob.Execute, con resultado completed o failed. " +
			"failed incluye fallos de negocio del handler y fallos de persistencia posterior.",
	}, []string{"result"})

	jobsInFlight := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "jobs_in_flight",
		Help: "Jobs actualmente siendo procesados por un worker.",
	})

	jobProcessing := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "job_processing_duration_seconds",
		Help:    "Duracion de ProcessJob.Execute, incluyendo el handler y el MarkCompleted/MarkFailed.",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	})

	reg.MustRegister(jobsEnqueued, jobsProcessed, jobsInFlight, jobProcessing)

	return &Metrics{
		jobsEnqueued:  jobsEnqueued,
		jobsProcessed: jobsProcessed,
		jobsInFlight:  jobsInFlight,
		jobProcessing: jobProcessing,
		handler:       promhttp.HandlerFor(reg, promhttp.HandlerOpts{}),
	}
}

func (m *Metrics) IncJobsEnqueued() {
	m.jobsEnqueued.Inc()
}

func (m *Metrics) IncJobsProcessed(result application.JobResult) {
	m.jobsProcessed.WithLabelValues(string(result)).Inc()
}

func (m *Metrics) IncJobsInFlight() {
	m.jobsInFlight.Inc()
}

func (m *Metrics) DecJobsInFlight() {
	m.jobsInFlight.Dec()
}

func (m *Metrics) ObserveJobProcessingDuration(d time.Duration) {
	m.jobProcessing.Observe(d.Seconds())
}

func (m *Metrics) Handler() http.Handler {
	return m.handler
}
