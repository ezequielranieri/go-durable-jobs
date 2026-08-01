package application

import "time"

type JobResult string

const (
	JobCompleted JobResult = "completed"
	JobFailed    JobResult = "failed"
)

type MetricsRecorder interface {
	IncJobsEnqueued()
	IncJobsProcessed(result JobResult)
	IncJobsInFlight()
	DecJobsInFlight()
	ObserveJobProcessingDuration(d time.Duration)
}

type noopMetricsRecorder struct{}

func (noopMetricsRecorder) IncJobsEnqueued()                           {}
func (noopMetricsRecorder) IncJobsProcessed(JobResult)                 {}
func (noopMetricsRecorder) IncJobsInFlight()                           {}
func (noopMetricsRecorder) DecJobsInFlight()                           {}
func (noopMetricsRecorder) ObserveJobProcessingDuration(time.Duration) {}
