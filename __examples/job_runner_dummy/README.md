# Job runner crash-test dummy

This minimal project is a tarball-friendly job payload for the worker-service
job runner. It intentionally fails by default to validate error handling.

## Build a tarball

```sh
./create-tarball.sh /tmp/job_runner_dummy.tar.gz
```

The script packages `Dockerfile` and `run.sh` at the tarball root so the job
runner can build without a nested path.

## Configure worker-service

- `WORKER_JOB_TARBALL_DIR=/tmp`
- `WORKER_JOB_TARBALL_ALLOWLIST=*` (or your host allowlist for URL sources)

## Create a job (admin UI)

- **Source**: `Tarball path`
- **Tarball path**: `job_runner_dummy.tar.gz`
- **Dockerfile**: `Dockerfile`
- **Command**: leave empty (uses Dockerfile CMD)

Set `DUMMY_SHOULD_FAIL=0` in the job env to let it succeed.
