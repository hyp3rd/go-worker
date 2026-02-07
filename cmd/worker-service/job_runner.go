package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/hyp3rd/ewrap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	worker "github.com/hyp3rd/go-worker"
)

type jobRunner struct {
	gitBin        string
	dockerBin     string
	network       string
	workDir       string
	outputBytes   int
	repoAllowlist map[string]struct{}
	allowAllRepos bool
}

func newJobRunner(cfg config) *jobRunner {
	allowAll := false
	allowlist := map[string]struct{}{}

	for _, repo := range cfg.jobRepoAllowlist {
		if repo == "*" {
			allowAll = true

			continue
		}

		allowlist[repo] = struct{}{}
	}

	return &jobRunner{
		gitBin:        strings.TrimSpace(cfg.jobGitBin),
		dockerBin:     strings.TrimSpace(cfg.jobDockerBin),
		network:       strings.TrimSpace(cfg.jobNetwork),
		workDir:       strings.TrimSpace(cfg.jobWorkDir),
		outputBytes:   cfg.jobOutputBytes,
		repoAllowlist: allowlist,
		allowAllRepos: allowAll,
	}
}

func jobDurableHandlerSpec(runner *jobRunner) worker.DurableHandlerSpec {
	return worker.DurableHandlerSpec{
		Make: func() proto.Message { return &structpb.Struct{} },
		Fn: func(ctx context.Context, payload proto.Message) (any, error) {
			msg, ok := payload.(*structpb.Struct)
			if !ok {
				return nil, ewrap.New("invalid job payload")
			}

			spec, err := worker.JobSpecFromPayload(msg)
			if err != nil {
				return nil, err
			}

			job := worker.AdminJob{AdminJobSpec: spec}

			return runner.Run(ctx, job)
		},
	}
}

func (r *jobRunner) Run(ctx context.Context, job worker.AdminJob) (string, error) {
	if r == nil {
		return "", ewrap.New("job runner not configured")
	}

	err := r.validate(job)
	if err != nil {
		return "", err
	}

	repoDir, cleanup, err := r.jobWorkspace()
	if err != nil {
		return "", err
	}
	defer cleanup()

	cloneOutput, err := r.cloneRepo(ctx, job, repoDir)
	if err != nil {
		return cloneOutput, ewrap.Wrap(err, "git clone")
	}

	contextDir := jobContextDir(repoDir, job)
	dockerfilePath := jobDockerfilePath(contextDir, job)
	imageTag := dockerImageTag(job.Name, job.Tag)

	buildOutput, err := r.buildImage(ctx, dockerfilePath, imageTag, contextDir)
	if err != nil {
		return buildOutput, ewrap.Wrap(err, "docker build")
	}

	runOutput, err := r.runImage(ctx, job, imageTag)
	if err != nil {
		return runOutput, ewrap.Wrap(err, "docker run")
	}

	if runOutput == "" {
		runOutput = "job complete"
	}

	return runOutput, nil
}

func (r *jobRunner) validate(job worker.AdminJob) error {
	if r.gitBin == "" || r.dockerBin == "" {
		return ewrap.New("job runner binaries are required")
	}

	if strings.TrimSpace(job.Repo) == "" || strings.TrimSpace(job.Tag) == "" {
		return ewrap.New("job repo and tag are required")
	}

	if r.allowAllRepos {
		return nil
	}

	if _, ok := r.repoAllowlist[job.Repo]; !ok {
		return ewrap.New("job repo not allowlisted")
	}

	return nil
}

func (r *jobRunner) jobWorkspace() (string, func(), error) {
	workDir := r.workDir
	if workDir == "" {
		workDir = os.TempDir()
	}

	repoDir, err := os.MkdirTemp(workDir, "worker-job-*")
	if err != nil {
		return "", nil, ewrap.Wrap(err, "create job workspace")
	}

	cleanup := func() {
		err := os.RemoveAll(repoDir)
		if err != nil {
			log.Printf("job workspace cleanup: %v", err)
		}
	}

	return repoDir, cleanup, nil
}

func (r *jobRunner) cloneRepo(ctx context.Context, job worker.AdminJob, repoDir string) (string, error) {
	tagRef := "refs/tags/" + job.Tag
	// #nosec G204 -- repo allowlist + tag-only refs guard against arbitrary input
	cmd := exec.CommandContext(ctx, r.gitBin, "clone", "--depth", "1", "--branch", tagRef, "--single-branch", job.Repo, repoDir)

	return runCommand(cmd, r.outputBytes)
}

func jobContextDir(repoDir string, job worker.AdminJob) string {
	if job.Path == "" {
		return repoDir
	}

	return filepath.Join(repoDir, job.Path)
}

func jobDockerfilePath(contextDir string, job worker.AdminJob) string {
	dockerfile := job.Dockerfile
	if dockerfile == "" {
		dockerfile = "Dockerfile"
	}

	return filepath.Join(contextDir, dockerfile)
}

func (r *jobRunner) buildImage(
	ctx context.Context,
	dockerfilePath string,
	imageTag string,
	contextDir string,
) (string, error) {
	// #nosec G204 -- docker binary/path is configured, inputs are normalized
	cmd := exec.CommandContext(ctx, r.dockerBin, "build", "-f", dockerfilePath, "-t", imageTag, contextDir)

	return runCommand(cmd, r.outputBytes)
}

func (r *jobRunner) runImage(ctx context.Context, job worker.AdminJob, imageTag string) (string, error) {
	runArgs := []string{"run", "--rm"}
	if r.network != "" {
		runArgs = append(runArgs, "--network", r.network)
	}

	for _, envKey := range job.Env {
		value := os.Getenv(envKey)
		if value == "" {
			continue
		}

		runArgs = append(runArgs, "--env", envKey+"="+value)
	}

	runArgs = append(runArgs, imageTag)
	if len(job.Command) > 0 {
		runArgs = append(runArgs, job.Command...)
	}

	// #nosec G204 -- docker binary/path is configured, inputs are normalized
	cmd := exec.CommandContext(ctx, r.dockerBin, runArgs...)

	return runCommand(cmd, r.outputBytes)
}

func runCommand(cmd *exec.Cmd, maxBytes int) (string, error) {
	var buffer limitedBuffer
	if maxBytes > 0 {
		buffer.limit = maxBytes
	}

	cmd.Stdout = &buffer
	cmd.Stderr = &buffer

	err := cmd.Run()

	return buffer.String(), err
}

type limitedBuffer struct {
	limit     int
	buffer    bytes.Buffer
	truncated bool
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	if b.limit <= 0 {
		data, err := b.buffer.Write(data)
		if err != nil {
			return data, ewrap.Wrap(err, "write to buffer")
		}

		return data, nil
	}

	remaining := b.limit - b.buffer.Len()
	if remaining <= 0 {
		b.truncated = true

		return len(data), nil
	}

	if len(data) <= remaining {
		n, err := b.buffer.Write(data)
		if err != nil {
			return n, ewrap.Wrap(err, "write to buffer")
		}

		return n, nil
	}

	n, err := b.buffer.Write(data[:remaining])
	b.truncated = true

	return n + (len(data) - remaining), ewrap.Wrap(err, "write to buffer")
}

func (b *limitedBuffer) String() string {
	if !b.truncated {
		return strings.TrimSpace(b.buffer.String())
	}

	return strings.TrimSpace(b.buffer.String()) + "\n...truncated..."
}

func dockerImageTag(name, tag string) string {
	return "worker-job-" + sanitizeDockerName(name) + ":" + sanitizeDockerTag(tag)
}

func sanitizeDockerName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "job"
	}

	out := strings.Map(func(ch rune) rune {
		switch {
		case ch >= 'a' && ch <= 'z':
			return ch
		case ch >= 'A' && ch <= 'Z':
			return ch
		case ch >= '0' && ch <= '9':
			return ch
		case ch == '.' || ch == '_' || ch == '-':
			return ch
		default:
			return '_'
		}
	}, value)

	return strings.ToLower(out)
}

func sanitizeDockerTag(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "latest"
	}

	out := strings.Map(func(ch rune) rune {
		switch {
		case ch >= 'a' && ch <= 'z':
			return ch
		case ch >= 'A' && ch <= 'Z':
			return ch
		case ch >= '0' && ch <= '9':
			return ch
		case ch == '.' || ch == '_' || ch == '-':
			return ch
		default:
			return '_'
		}
	}, value)

	return strings.ToLower(out)
}
