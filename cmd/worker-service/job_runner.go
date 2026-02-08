package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/hyp3rd/ewrap"
	sectools "github.com/hyp3rd/sectools/pkg/io"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	worker "github.com/hyp3rd/go-worker"
)

const tarballSizeLimitError = "tarball exceeds size limit"

type jobRunner struct {
	gitBin           string
	dockerBin        string
	network          string
	workDir          string
	outputBytes      int
	repoAllowlist    map[string]struct{}
	allowAllRepos    bool
	tarballAllowlist map[string]struct{}
	allowAllTarballs bool
	tarballDir       string
	tarballMaxBytes  int64
	tarballTimeout   time.Duration
}

const (
	tarballDirMode  = 0o750
	tarballFileMode = 0o600
)

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

	tarballAll := false
	tarballAllow := map[string]struct{}{}

	for _, host := range cfg.jobTarballAllowlist {
		host = strings.TrimSpace(strings.ToLower(host))
		if host == "" {
			continue
		}

		if host == "*" {
			tarballAll = true

			continue
		}

		if strings.Contains(host, "://") {
			parsed, err := url.Parse(host)
			if err == nil && parsed.Host != "" {
				host = strings.ToLower(parsed.Host)
			}
		}

		tarballAllow[host] = struct{}{}
	}

	return &jobRunner{
		gitBin:           strings.TrimSpace(cfg.jobGitBin),
		dockerBin:        strings.TrimSpace(cfg.jobDockerBin),
		network:          strings.TrimSpace(cfg.jobNetwork),
		workDir:          strings.TrimSpace(cfg.jobWorkDir),
		outputBytes:      cfg.jobOutputBytes,
		repoAllowlist:    allowlist,
		allowAllRepos:    allowAll,
		tarballAllowlist: tarballAllow,
		allowAllTarballs: tarballAll,
		tarballDir:       strings.TrimSpace(cfg.jobTarballDir),
		tarballMaxBytes:  cfg.jobTarballMaxBytes,
		tarballTimeout:   cfg.jobTarballTimeout,
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

	sourceOutput, err := r.prepareSource(ctx, job, repoDir)
	if err != nil {
		return sourceOutput, err
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
	if r.dockerBin == "" {
		return ewrap.New("job runner docker binary is required")
	}

	source := resolveJobSource(job)

	switch source {
	case worker.JobSourceGitTag:
		return r.validateGitJob(job)
	case worker.JobSourceTarballURL:
		return r.validateTarballURLJob(job)
	case worker.JobSourceTarballPath:
		return r.validateTarballPathJob(job)
	default:
		return ewrap.New("job source is invalid")
	}
}

func resolveJobSource(job worker.AdminJob) string {
	source := worker.NormalizeJobSource(job.Source)
	if source == worker.JobSourceGitTag {
		if strings.TrimSpace(job.TarballURL) != "" {
			return worker.JobSourceTarballURL
		}

		if strings.TrimSpace(job.TarballPath) != "" {
			return worker.JobSourceTarballPath
		}
	}

	return source
}

func (r *jobRunner) validateGitJob(job worker.AdminJob) error {
	if r.gitBin == "" {
		return ewrap.New("job runner git binary is required")
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

func (r *jobRunner) validateTarballURLJob(job worker.AdminJob) error {
	if strings.TrimSpace(job.TarballURL) == "" {
		return ewrap.New("job tarball url is required")
	}

	return r.validateTarballURL(job.TarballURL)
}

func (r *jobRunner) validateTarballPathJob(job worker.AdminJob) error {
	if strings.TrimSpace(job.TarballPath) == "" {
		return ewrap.New("job tarball path is required")
	}

	if r.tarballDir == "" {
		return ewrap.New("job tarball dir is not configured")
	}

	_, err := r.resolveTarballPath(job.TarballPath)
	if err != nil {
		return err
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

func (r *jobRunner) prepareSource(ctx context.Context, job worker.AdminJob, repoDir string) (string, error) {
	source := resolveJobSource(job)

	switch source {
	case worker.JobSourceGitTag:
		cloneOutput, err := r.cloneRepo(ctx, job, repoDir)
		if err != nil {
			return cloneOutput, ewrap.Wrap(err, "git clone")
		}

		return cloneOutput, nil
	case worker.JobSourceTarballURL:
		return r.extractTarballFromURL(ctx, job, repoDir)
	case worker.JobSourceTarballPath:
		return r.extractTarballFromPath(ctx, job, repoDir)
	default:
		return "", ewrap.New("job source is invalid")
	}
}

func (r *jobRunner) cloneRepo(ctx context.Context, job worker.AdminJob, repoDir string) (string, error) {
	tagRef := "refs/tags/" + job.Tag
	// #nosec G204 -- repo allowlist + tag-only refs guard against arbitrary input
	cmd := exec.CommandContext(ctx, r.gitBin, "clone", "--depth", "1", "--branch", tagRef, "--single-branch", job.Repo, repoDir)

	return runCommand(cmd, r.outputBytes)
}

func (r *jobRunner) extractTarballFromURL(ctx context.Context, job worker.AdminJob, repoDir string) (string, error) {
	parsed, err := r.parseTarballURL(job.TarballURL)
	if err != nil {
		return "", err
	}

	client := &http.Client{Timeout: r.tarballTimeout}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", ewrap.Wrap(err, "build tarball request")
	}

	req.Header.Set("User-Agent", "go-worker/job-runner")

	resp, err := client.Do(req)
	if err != nil {
		return "", ewrap.Wrap(err, "download tarball")
	}

	defer func() {
		closeWithLog("tarball response close", resp.Body.Close())
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", ewrap.New("tarball download failed")
	}

	if r.tarballMaxBytes > 0 && resp.ContentLength > r.tarballMaxBytes {
		return "", ewrap.New(tarballSizeLimitError)
	}

	err = r.extractTarball(resp.Body, repoDir, strings.TrimSpace(job.TarballSHA))
	if err != nil {
		return "", err
	}

	return "tarball downloaded", nil
}

func (r *jobRunner) extractTarballFromPath(ctx context.Context, job worker.AdminJob, repoDir string) (string, error) {
	ctxErr := ctx.Err()
	if ctxErr != nil {
		return "", ewrap.Wrap(ctxErr, "tarball context")
	}

	path, err := r.resolveTarballPath(job.TarballPath)
	if err != nil {
		return "", err
	}

	file, err := r.openTarballFile(path)
	if err != nil {
		return "", err
	}

	defer func() {
		closeWithLog("tarball file close", file.Close())
	}()

	info, err := file.Stat()
	if err != nil {
		return "", ewrap.Wrap(err, "stat tarball")
	}

	if r.tarballMaxBytes > 0 && info.Size() > r.tarballMaxBytes {
		return "", ewrap.New(tarballSizeLimitError)
	}

	err = r.extractTarball(file, repoDir, strings.TrimSpace(job.TarballSHA))
	if err != nil {
		return "", err
	}

	return "tarball loaded", nil
}

func (r *jobRunner) parseTarballURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, ewrap.New("job tarball url is required")
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, ewrap.New("job tarball url is invalid")
	}

	if parsed.Host == "" {
		return nil, ewrap.New("job tarball url host is required")
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "https" && scheme != "http" {
		return nil, ewrap.New("job tarball url must be http or https")
	}

	if !r.isTarballHostAllowed(parsed.Host) {
		return nil, ewrap.New("job tarball url not allowlisted")
	}

	return parsed, nil
}

func (r *jobRunner) validateTarballURL(raw string) error {
	_, err := r.parseTarballURL(raw)

	return err
}

func (r *jobRunner) isTarballHostAllowed(host string) bool {
	if r.allowAllTarballs {
		return true
	}

	if len(r.tarballAllowlist) == 0 {
		return false
	}

	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return false
	}

	if _, ok := r.tarballAllowlist[host]; ok {
		return true
	}

	if base, _, ok := strings.Cut(host, ":"); ok {
		if _, found := r.tarballAllowlist[base]; found {
			return true
		}
	}

	return false
}

func (r *jobRunner) resolveTarballPath(value string) (string, error) {
	if r.tarballDir == "" {
		return "", ewrap.New("job tarball dir is not configured")
	}

	clean, err := sanitizeTarballPath(value)
	if err != nil {
		return "", err
	}

	base, err := filepath.Abs(r.tarballDir)
	if err != nil {
		return "", ewrap.Wrap(err, "resolve tarball base")
	}

	info, err := os.Stat(base)
	if err != nil {
		return "", ewrap.Wrap(err, "stat tarball base")
	}

	if !info.IsDir() {
		return "", ewrap.New("job tarball dir is not a directory")
	}

	joined := filepath.Join(base, clean)

	joined = filepath.Clean(joined)
	if joined != base && !strings.HasPrefix(joined, base+string(os.PathSeparator)) {
		return "", ewrap.New("job tarball path escapes base directory")
	}

	return joined, nil
}

func sanitizeTarballPath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", ewrap.New("job tarball path is required")
	}

	if strings.HasPrefix(value, "/") {
		return "", ewrap.New("job tarball path must be relative")
	}

	clean := filepath.Clean(value)
	if strings.HasPrefix(clean, "..") {
		return "", ewrap.New("job tarball path must not escape base directory")
	}

	return clean, nil
}

func (r *jobRunner) tarballClient(baseDir string) (*sectools.Client, error) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		return nil, ewrap.New("tarball base dir is required")
	}

	abs, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, ewrap.Wrap(err, "resolve tarball base")
	}

	opts := []sectools.Option{
		sectools.WithAllowedRoots(abs),
		sectools.WithAllowAbsolute(true),
		sectools.WithDirMode(tarballDirMode),
		sectools.WithWriteFileMode(tarballFileMode),
	}
	if r.tarballMaxBytes > 0 {
		opts = append(opts,
			sectools.WithReadMaxSize(r.tarballMaxBytes),
			sectools.WithWriteMaxSize(r.tarballMaxBytes),
		)
	}

	client, err := sectools.NewWithOptions(opts...)
	if err != nil {
		return nil, ewrap.Wrap(err, "tarball io client")
	}

	return client, nil
}

func (r *jobRunner) openTarballFile(path string) (*os.File, error) {
	client, err := r.tarballClient(r.tarballDir)
	if err != nil {
		return nil, err
	}

	file, err := client.OpenFile(path)
	if err != nil {
		return nil, ewrap.Wrap(err, "open tarball")
	}

	return file, nil
}

func closeWithLog(label string, err error) {
	if err != nil {
		log.Printf("%s: %v", label, err)
	}
}

func (r *jobRunner) extractTarball(reader io.Reader, repoDir, expectedSHA string) error {
	var limited *io.LimitedReader

	stream := reader
	if r.tarballMaxBytes > 0 {
		limited = &io.LimitedReader{R: reader, N: r.tarballMaxBytes + 1}
		stream = limited
	}

	hasher := sha256.New()
	tee := io.TeeReader(stream, hasher)

	client, err := r.tarballClient(repoDir)
	if err != nil {
		return err
	}

	bytesWritten, err := extractTarArchive(tee, repoDir, r.tarballMaxBytes, client)
	if err != nil {
		return err
	}

	if limited != nil && limited.N <= 0 {
		return ewrap.New(tarballSizeLimitError)
	}

	if expectedSHA != "" {
		sum := hex.EncodeToString(hasher.Sum(nil))
		if strings.ToLower(expectedSHA) != sum {
			return ewrap.New("tarball sha256 mismatch")
		}
	}

	_ = bytesWritten

	return nil
}

const tarTypeRegAlt = byte(0)

type tarEntry struct {
	target string
	size   int64
	isDir  bool
}

func extractTarArchive(reader io.Reader, dest string, maxBytes int64, client *sectools.Client) (int64, error) {
	tarReader, closer := openTarReader(reader)
	if closer != nil {
		defer func() {
			closeWithLog("tarball gzip close", closer.Close())
		}()
	}

	var written int64

	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return written, ewrap.Wrap(err, "read tarball")
		}

		entry, skip, err := parseTarEntry(header, dest)
		if err != nil {
			return written, err
		}

		if skip {
			continue
		}

		if entry.isDir {
			err = client.MkdirAll(entry.target)
			if err != nil {
				return written, ewrap.Wrap(err, "create tarball dir")
			}

			continue
		}

		err = writeTarFile(client, tarReader, entry, &written, maxBytes)
		if err != nil {
			return written, err
		}
	}

	return written, nil
}

func openTarReader(reader io.Reader) (*tar.Reader, io.Closer) {
	gzReader, err := gzip.NewReader(reader)
	if err == nil {
		return tar.NewReader(gzReader), gzReader
	}

	return tar.NewReader(reader), nil
}

func parseTarEntry(header *tar.Header, dest string) (tarEntry, bool, error) {
	name := strings.TrimSpace(header.Name)
	if name == "" {
		return tarEntry{}, true, nil
	}

	if strings.HasPrefix(name, "/") {
		return tarEntry{}, false, ewrap.New("tarball contains absolute paths")
	}

	clean := filepath.Clean(name)
	if strings.HasPrefix(clean, "..") {
		return tarEntry{}, false, ewrap.New("tarball path escapes workspace")
	}

	target := filepath.Join(dest, clean)
	if target != dest && !strings.HasPrefix(target, dest+string(os.PathSeparator)) {
		return tarEntry{}, false, ewrap.New("tarball path escapes workspace")
	}

	switch header.Typeflag {
	case tar.TypeDir:
		return tarEntry{target: target, isDir: true}, false, nil
	case tar.TypeReg, tarTypeRegAlt:
		if header.Size < 0 {
			return tarEntry{}, false, ewrap.New("tarball contains invalid file size")
		}

		return tarEntry{target: target, size: header.Size}, false, nil
	default:
		return tarEntry{}, false, ewrap.New("tarball contains unsupported entry")
	}
}

func writeTarFile(
	client *sectools.Client,
	reader *tar.Reader,
	entry tarEntry,
	written *int64,
	maxBytes int64,
) error {
	parent := filepath.Dir(entry.target)

	err := client.MkdirAll(parent)
	if err != nil {
		return ewrap.Wrap(err, "create tarball path")
	}

	err = updateTarWritten(written, entry.size, maxBytes)
	if err != nil {
		return err
	}

	limited := io.LimitReader(reader, entry.size)

	err = client.WriteFromReader(entry.target, limited)
	if err != nil {
		return ewrap.Wrap(err, "extract tarball file")
	}

	return nil
}

func updateTarWritten(written *int64, delta, maxBytes int64) error {
	*written += delta
	if maxBytes > 0 && *written > maxBytes {
		return ewrap.New(tarballSizeLimitError)
	}

	return nil
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
