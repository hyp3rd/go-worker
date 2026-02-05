import { SectionHeader } from "@/components/section-header";
import { QueuesTable } from "@/components/queues-table";
import { RefreshControls } from "@/components/refresh-controls";
import { getQueueSummaries } from "@/lib/data";

export const dynamic = "force-dynamic";

export default async function QueuesPage() {
  const queues = await getQueueSummaries();

  return (
    <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
      <SectionHeader
        title="Queues"
        description="Live queue balances and scheduling weights."
        action={<RefreshControls />}
      />
      <QueuesTable queues={queues} />
    </section>
  );
}
