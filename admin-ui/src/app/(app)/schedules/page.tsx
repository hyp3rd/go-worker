import { SectionHeader } from "@/components/section-header";
import { SchedulesGrid } from "@/components/schedules-grid";
import { ScheduleEvents } from "@/components/schedule-events";
import { RefreshControls } from "@/components/refresh-controls";
import { getJobSchedules, getScheduleEvents, getScheduleFactories } from "@/lib/data";

export const dynamic = "force-dynamic";

export default async function SchedulesPage() {
  const [schedules, factories, events] = await Promise.all([
    getJobSchedules(),
    getScheduleFactories(),
    getScheduleEvents({ limit: 25 }),
  ]);

  return (
    <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
      <SectionHeader
        title="Schedules"
        description="Cron jobs across durable and in‑memory workers."
        action={<RefreshControls />}
      />
      <SchedulesGrid schedules={schedules} factories={factories} />
      <ScheduleEvents events={events} />
    </section>
  );
}
