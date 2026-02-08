import { DlqTable } from "@/components/dlq-table";
import { RefreshControls } from "@/components/refresh-controls";
import { SectionHeader } from "@/components/section-header";
import { HelpPopover } from "@/components/help-popover";
import { getDlqEntries } from "@/lib/data";

export const dynamic = "force-dynamic";

const DEFAULT_PAGE_SIZE = 5;

type DlqPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function DlqPage({ searchParams }: DlqPageProps) {
  const params = (await searchParams) ?? {};
  const pageParam = Array.isArray(params.page) ? params.page[0] : params.page;
  const queryParam = Array.isArray(params.query)
    ? params.query[0]
    : params.query;
  const queueParam = Array.isArray(params.queue)
    ? params.queue[0]
    : params.queue;
  const handlerParam = Array.isArray(params.handler)
    ? params.handler[0]
    : params.handler;
  const limitParam = Array.isArray(params.limit)
    ? params.limit[0]
    : params.limit;

  const page = Math.max(1, Number(pageParam ?? 1));
  const pageSize = Math.max(
    1,
    Number(limitParam ?? DEFAULT_PAGE_SIZE)
  );
  const offset = (page - 1) * pageSize;

  const { entries, total } = await getDlqEntries({
    limit: pageSize,
    offset,
    query: queryParam ?? "",
    queue: queueParam ?? "",
    handler: handlerParam ?? "",
  });

  return (
    <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
      <SectionHeader
        title="Dead Letter Queue"
        description="Failed tasks awaiting replay or inspection."
        action={
          <div className="flex flex-wrap items-center gap-2">
            <HelpPopover
              title="DLQ tips"
              items={[
                "Replay is at-least-once; ensure handlers are idempotent.",
                "Use filters to narrow by queue, handler, or search text.",
                "Open a row to see payload size, metadata, and last error.",
              ]}
              href="/docs#dlq"
            />
            <RefreshControls />
          </div>
        }
      />
      <DlqTable
        entries={entries}
        total={total}
        page={page}
        pageSize={pageSize}
        query={queryParam ?? ""}
        queueFilter={queueParam ?? ""}
        handlerFilter={handlerParam ?? ""}
      />
    </section>
  );
}
