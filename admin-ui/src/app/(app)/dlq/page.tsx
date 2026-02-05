import { DlqTable } from "@/components/dlq-table";
import { SectionHeader } from "@/components/section-header";
import { getDlqEntries } from "@/lib/data";

export const dynamic = "force-dynamic";

export default async function DlqPage() {
  const entries = await getDlqEntries();

  return (
    <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
      <SectionHeader
        title="Dead Letter Queue"
        description="Failed tasks awaiting replay or inspection."
      />
      <DlqTable entries={entries} />
    </section>
  );
}
