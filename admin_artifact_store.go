package worker

import (
	"path/filepath"
	"strings"

	"github.com/hyp3rd/ewrap"
	sectools "github.com/hyp3rd/sectools/pkg/io"

	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

type adminArtifactStore interface {
	Resolve(job *workerpb.Job) (adminJobArtifactJSON, error)
}

type localAdminArtifactStore struct {
	tarballDir string
}

func newAdminArtifactStore(jobTarballDir string) adminArtifactStore {
	return localAdminArtifactStore{tarballDir: strings.TrimSpace(jobTarballDir)}
}

func (s localAdminArtifactStore) Resolve(job *workerpb.Job) (adminJobArtifactJSON, error) {
	if job == nil || job.GetSpec() == nil {
		return adminJobArtifactJSON{}, ErrAdminJobNotFound
	}

	spec := job.GetSpec()
	artifact := adminJobArtifactJSON{
		Name:   spec.GetName(),
		Source: NormalizeJobSource(spec.GetSource()),
		SHA256: strings.TrimSpace(spec.GetTarballSha256()),
	}

	switch artifact.Source {
	case JobSourceTarballURL:
		target := strings.TrimSpace(spec.GetTarballUrl())
		if target == "" {
			return adminJobArtifactJSON{}, ErrAdminJobTarballURLRequired
		}

		artifact.Downloadable = true
		artifact.RedirectURL = target
		artifact.Filename = filepath.Base(target)

		return artifact, nil
	case JobSourceTarballPath:
		filePath, reader, err := s.resolveLocalArtifact(strings.TrimSpace(spec.GetTarballPath()))
		if err != nil {
			return adminJobArtifactJSON{}, err
		}

		file, err := reader.OpenFile(filePath)
		if err != nil {
			return adminJobArtifactJSON{}, ewrap.Wrap(err, "open artifact file")
		}

		stat, err := file.Stat()
		closeErr := file.Close()

		if err != nil {
			return adminJobArtifactJSON{}, ewrap.Wrap(err, "stat artifact file")
		}

		if closeErr != nil {
			return adminJobArtifactJSON{}, ewrap.Wrap(closeErr, "close artifact file")
		}

		if !stat.Mode().IsRegular() {
			return adminJobArtifactJSON{}, ewrap.New("artifact is not a regular file")
		}

		artifact.Downloadable = true
		artifact.Filename = filepath.Base(filePath)
		artifact.SizeBytes = stat.Size()
		artifact.LocalPath = filePath
		artifact.Reader = reader

		return artifact, nil
	default:
		return adminJobArtifactJSON{}, ewrap.Wrapf(ErrAdminUnsupported, "source %q", artifact.Source)
	}
}

func (s localAdminArtifactStore) resolveLocalArtifact(tarballPath string) (string, *sectools.Client, error) {
	if s.tarballDir == "" {
		return "", nil, ewrap.New("artifact downloads are disabled")
	}

	cleanPath, err := sanitizeJobPath(tarballPath)
	if err != nil || cleanPath == "" {
		return "", nil, ErrAdminJobTarballPathRequired
	}

	absBase, err := filepath.Abs(s.tarballDir)
	if err != nil {
		return "", nil, ewrap.Wrap(err, "resolve tarball root")
	}

	reader, err := sectools.NewWithOptions(
		sectools.WithAllowAbsolute(true),
		sectools.WithAllowedRoots(absBase),
	)
	if err != nil {
		return "", nil, ewrap.Wrap(err, "init artifact reader")
	}

	return filepath.Join(absBase, cleanPath), reader, nil
}
