import { SectionHeader } from "@/components/section-header";
import { JobsGrid } from "@/components/jobs-grid";
import { JobEvents } from "@/components/job-events";
import { RefreshControls } from "@/components/refresh-controls";
import { HelpPopover } from "@/components/help-popover";
import { getAdminJobs, getJobEvents } from "@/lib/data";

export const dynamic = "force-dynamic";

export default async function JobsPage() {
  const [jobs, jobEvents] = await Promise.all([
    getAdminJobs(),
    getJobEvents({ limit: 25 }),
  ]);

  return (
    <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
      <SectionHeader
        title="Jobs"
        description="Containerized tasks pulled from Git tags or tarballs."
        action={
          <div className="flex flex-wrap items-center gap-2">
            <HelpPopover
              title="Job runner tips"
              items={[
                "Jobs can pull from a Git tag, HTTPS tarball URL, or a local tarball path.",
                "Tarball URLs must be allowlisted; local tarballs resolve under WORKER_JOB_TARBALL_DIR.",
                "Command overrides the image entrypoint; env keys are read from the worker service.",
                "Output is truncated to the configured max bytes for safety.",
              ]}
              href="/docs#jobs"
            />
            <RefreshControls />
          </div>
        }
      />
      <JobsGrid jobs={jobs} events={jobEvents} />
      <JobEvents events={jobEvents} />
    </section>
  );
}
