package worker

import (
	"strings"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
)

const (
	jobMetaNameKey       = "job.name"
	jobMetaRepoKey       = "job.repo"
	jobMetaTagKey        = "job.tag"
	jobMetaPathKey       = "job.path"
	jobMetaDockerfileKey = "job.dockerfile"
	jobMetaCommandKey    = "job.command"
	jobMetaQueueKey      = "job.queue"
)

func jobDurableTask(job AdminJob) (DurableTask, error) {
	payload, err := JobPayloadFromSpec(job.AdminJobSpec)
	if err != nil {
		return DurableTask{}, err
	}

	task := DurableTask{
		ID:      uuid.New(),
		Handler: JobHandlerName,
		Message: payload,
		Queue:   strings.TrimSpace(job.Queue),
		Retries: job.Retries,
	}

	task.Metadata = jobMetadata(job)

	if task.Handler == "" {
		return DurableTask{}, ewrap.New("job handler is required")
	}

	return task, nil
}

func jobMetadata(job AdminJob) map[string]string {
	meta := map[string]string{
		jobMetaNameKey: job.Name,
		jobMetaRepoKey: job.Repo,
		jobMetaTagKey:  job.Tag,
	}

	if job.Path != "" {
		meta[jobMetaPathKey] = job.Path
	}

	if job.Dockerfile != "" {
		meta[jobMetaDockerfileKey] = job.Dockerfile
	}

	if len(job.Command) > 0 {
		meta[jobMetaCommandKey] = strings.Join(job.Command, " ")
	}

	if job.Queue != "" {
		meta[jobMetaQueueKey] = job.Queue
	}

	return meta
}
