import { SectionHeader } from "@/components/section-header";
import { QueuesTable } from "@/components/queues-table";
import { RefreshControls } from "@/components/refresh-controls";
import { HelpPopover } from "@/components/help-popover";
import { getQueueSummaries } from "@/lib/data";

export const dynamic = "force-dynamic";

export default async function QueuesPage() {
  const queues = await getQueueSummaries();

  return (
    <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
      <SectionHeader
        title="Queues"
        description="Live queue balances and scheduling weights."
        action={
          <div className="flex flex-wrap items-center gap-2">
            <HelpPopover
              title="Queue tips"
              items={[
                "Weights control relative scheduling share across queues.",
                "Pausing a queue stops durable dequeue for that queue only.",
                "Creating a queue sets weight and metadata; it does not enqueue tasks.",
              ]}
              href="/docs#queues"
            />
            <RefreshControls />
          </div>
        }
      />
      <QueuesTable queues={queues} />
    </section>
  );
}
