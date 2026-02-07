import { SectionHeader } from "@/components/section-header";
import { JobsGrid } from "@/components/jobs-grid";
import { RefreshControls } from "@/components/refresh-controls";
import { getAdminJobs } from "@/lib/data";

export const dynamic = "force-dynamic";

export default async function JobsPage() {
  const jobs = await getAdminJobs();

  return (
    <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
      <SectionHeader
        title="Jobs"
        description="Containerized tasks pulled from Git tags."
        action={<RefreshControls />}
      />
      <JobsGrid jobs={jobs} />
    </section>
  );
}
