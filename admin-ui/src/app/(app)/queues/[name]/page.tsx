import Link from "next/link";
import { SectionHeader } from "@/components/section-header";
import { StatCard } from "@/components/stat-card";
import { RefreshControls } from "@/components/refresh-controls";
import { formatNumber } from "@/lib/format";
import { getQueueDetail } from "@/lib/data";

export const dynamic = "force-dynamic";

type QueueDetailPageProps = {
  params: Promise<{
    name: string;
  }>;
};

export default async function QueueDetailPage({
  params,
}: QueueDetailPageProps) {
  const { name } = await params;
  const queueName = decodeURIComponent(name ?? "");
  const queue = await getQueueDetail(queueName);

  return (
    <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <SectionHeader
          title={`Queue: ${queue.name}`}
          description="Queue health, throughput, and routing weight."
        />
        <div className="flex items-center gap-3">
          <Link
            href="/queues"
            className="rounded-full border border-soft px-4 py-2 text-xs font-semibold text-muted transition hover:border-black/20 hover:text-black"
          >
            Back to queues
          </Link>
          <RefreshControls />
        </div>
      </div>

      <div className="mt-6 grid gap-4 lg:grid-cols-4">
        <StatCard
          label="Ready"
          value={formatNumber(queue.ready)}
          note="Awaiting workers"
        />
        <StatCard
          label="Processing"
          value={formatNumber(queue.processing)}
          note="Active leases"
        />
        <StatCard
          label="Dead"
          value={formatNumber(queue.dead)}
          note="In DLQ"
        />
        <StatCard
          label="Weight"
          value={formatNumber(queue.weight)}
          note="Scheduler weight"
        />
      </div>
    </section>
  );
}
