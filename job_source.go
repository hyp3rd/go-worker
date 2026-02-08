package worker

import "strings"

const (
	// JobSourceGitTag builds a job from a git tag (default).
	JobSourceGitTag = "git_tag"
	// JobSourceTarballURL builds a job from an HTTP(S) tarball URL.
	JobSourceTarballURL = "tarball_url"
	// JobSourceTarballPath builds a job from a local tarball path.
	JobSourceTarballPath = "tarball_path"
)

// NormalizeJobSource returns a valid job source, defaulting to git tag.
func NormalizeJobSource(source string) string {
	source = strings.TrimSpace(strings.ToLower(source))
	if source == "" {
		return JobSourceGitTag
	}

	switch source {
	case JobSourceGitTag, JobSourceTarballURL, JobSourceTarballPath:
		return source
	default:
		return JobSourceGitTag
	}
}

// IsTarballSource returns true when the job source expects a tarball.
func IsTarballSource(source string) bool {
	switch NormalizeJobSource(source) {
	case JobSourceTarballURL, JobSourceTarballPath:
		return true
	default:
		return false
	}
}
