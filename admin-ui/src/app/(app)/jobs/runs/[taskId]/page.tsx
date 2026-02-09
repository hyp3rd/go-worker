import { JobRunDetail } from "@/components/job-run-detail";
import { getJobEvents } from "@/lib/data";

export const dynamic = "force-dynamic";

type JobRunPageProps = {
  params: Promise<{ taskId: string }>;
};

export default async function JobRunPage({ params }: JobRunPageProps) {
  const resolved = await params;
  const taskId = decodeURIComponent(resolved.taskId);
  const events = await getJobEvents({ limit: 200 });

  return <JobRunDetail taskId={taskId} events={events} />;
}
