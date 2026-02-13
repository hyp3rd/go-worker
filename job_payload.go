package worker

import (
	"strconv"
	"strings"
	"time"

	"github.com/hyp3rd/ewrap"
	"google.golang.org/protobuf/types/known/structpb"
)

// JobHandlerName is the durable handler name for containerized jobs.
const JobHandlerName = "job_runner"

const (
	jobPayloadNameKey        = "name"
	jobPayloadDescriptionKey = "description"
	jobPayloadRepoKey        = "repo"
	jobPayloadTagKey         = "tag"
	jobPayloadSourceKey      = "source"
	jobPayloadTarballURLKey  = "tarball_url"
	jobPayloadTarballPathKey = "tarball_path"
	jobPayloadTarballSHAKey  = "tarball_sha256"
	jobPayloadPathKey        = "path"
	jobPayloadDockerfileKey  = "dockerfile"
	jobPayloadCommandKey     = "command"
	jobPayloadEnvKey         = "env"
	jobPayloadQueueKey       = "queue"
	jobPayloadRetriesKey     = "retries"
	jobPayloadTimeoutKey     = "timeout_seconds"
)

// JobPayloadFromSpec converts a job spec into a protobuf Struct payload.
func JobPayloadFromSpec(spec AdminJobSpec) (*structpb.Struct, error) {
	payload := map[string]any{
		jobPayloadNameKey:        strings.TrimSpace(spec.Name),
		jobPayloadDescriptionKey: strings.TrimSpace(spec.Description),
		jobPayloadRepoKey:        strings.TrimSpace(spec.Repo),
		jobPayloadTagKey:         strings.TrimSpace(spec.Tag),
		jobPayloadSourceKey:      strings.TrimSpace(spec.Source),
		jobPayloadTarballURLKey:  strings.TrimSpace(spec.TarballURL),
		jobPayloadTarballPathKey: strings.TrimSpace(spec.TarballPath),
		jobPayloadTarballSHAKey:  strings.TrimSpace(spec.TarballSHA),
		jobPayloadPathKey:        strings.TrimSpace(spec.Path),
		jobPayloadDockerfileKey:  strings.TrimSpace(spec.Dockerfile),
		jobPayloadQueueKey:       strings.TrimSpace(spec.Queue),
		jobPayloadRetriesKey:     float64(spec.Retries),
		jobPayloadTimeoutKey:     float64(spec.Timeout.Seconds()),
		jobPayloadCommandKey:     toAnySlice(spec.Command),
		jobPayloadEnvKey:         toAnySlice(spec.Env),
	}

	msg, err := structpb.NewStruct(payload)
	if err != nil {
		return nil, ewrap.Wrap(err, "build job payload")
	}

	return msg, nil
}

// JobSpecFromPayload decodes a protobuf Struct payload into a job spec.
func JobSpecFromPayload(payload *structpb.Struct) (AdminJobSpec, error) {
	if payload == nil {
		return AdminJobSpec{}, ewrap.New("job payload is nil")
	}

	values := payload.AsMap()

	spec := AdminJobSpec{
		Name:        getStringValue(values[jobPayloadNameKey]),
		Description: getStringValue(values[jobPayloadDescriptionKey]),
		Repo:        getStringValue(values[jobPayloadRepoKey]),
		Tag:         getStringValue(values[jobPayloadTagKey]),
		Source:      getStringValue(values[jobPayloadSourceKey]),
		TarballURL:  getStringValue(values[jobPayloadTarballURLKey]),
		TarballPath: getStringValue(values[jobPayloadTarballPathKey]),
		TarballSHA:  getStringValue(values[jobPayloadTarballSHAKey]),
		Path:        getStringValue(values[jobPayloadPathKey]),
		Dockerfile:  getStringValue(values[jobPayloadDockerfileKey]),
		Queue:       getStringValue(values[jobPayloadQueueKey]),
		Command:     getStringSlice(values[jobPayloadCommandKey]),
		Env:         getStringSlice(values[jobPayloadEnvKey]),
		Retries:     int(getNumberValue(values[jobPayloadRetriesKey])),
	}

	timeoutSeconds := int64(getNumberValue(values[jobPayloadTimeoutKey]))
	if timeoutSeconds > 0 {
		spec.Timeout = time.Duration(timeoutSeconds) * time.Second
	}

	if spec.Name == "" {
		return AdminJobSpec{}, ewrap.New("job payload missing name")
	}

	return spec, nil
}

func toAnySlice(values []string) []any {
	if len(values) == 0 {
		return nil
	}

	out := make([]any, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}

		out = append(out, value)
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func getStringValue(value any) string {
	raw, ok := value.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(raw)
}

func getNumberValue(value any) float64 {
	switch raw := value.(type) {
	case float64:
		return raw
	case int:
		return float64(raw)
	case int64:
		return float64(raw)
	case string:
		parsed, err := parseFloat(raw)
		if err == nil {
			return parsed
		}
	default:
		return 0
	}

	return 0
}

func getStringSlice(value any) []string {
	raw, ok := value.([]any)
	if !ok {
		return nil
	}

	out := make([]string, 0, len(raw))
	for _, entry := range raw {
		str, ok := entry.(string)
		if !ok {
			continue
		}

		str = strings.TrimSpace(str)
		if str == "" {
			continue
		}

		out = append(out, str)
	}

	return out
}

const jobPayloadFloatBitSize = 64

func parseFloat(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, ewrap.New("empty number")
	}

	parsed, err := strconv.ParseFloat(value, jobPayloadFloatBitSize)
	if err != nil {
		return 0, ewrap.Wrap(err, "parse float")
	}

	return parsed, nil
}
