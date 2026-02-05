import { SectionHeader } from "@/components/section-header";
import { getCoordinationStatus, getOverviewStats } from "@/lib/data";
import {
  sessionMaxAgeSeconds,
  sessionRefreshAfterSeconds,
} from "@/lib/auth";

export const dynamic = "force-dynamic";

const formatSeconds = (value: number) => {
  if (value <= 0) {
    return "n/a";
  }
  const hours = Math.floor(value / 3600);
  const minutes = Math.floor((value % 3600) / 60);
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m`;
  }
  return `${value}s`;
};

const envValue = (value?: string) => (value && value.length > 0 ? value : "not set");

export default async function SettingsPage() {
  const [coordination, stats] = await Promise.all([
    getCoordinationStatus(),
    getOverviewStats(),
  ]);

  const adminApiUrl = envValue(process.env.WORKER_ADMIN_API_URL);
  const mtlsCert = envValue(process.env.WORKER_ADMIN_MTLS_CERT);
  const mtlsKey = envValue(process.env.WORKER_ADMIN_MTLS_KEY);
  const mtlsCa = envValue(process.env.WORKER_ADMIN_MTLS_CA);
  const mockEnabled = envValue(process.env.WORKER_ADMIN_ALLOW_MOCK);

  const settings = [
    { label: "Active workers", value: stats.activeWorkers.toString() },
    { label: "Queues", value: stats.queues.toString() },
    { label: "Queued tasks", value: stats.queuedTasks.toString() },
    { label: "DLQ replay", value: "Up to 1000 items" },
    { label: "Global rate limit", value: coordination.globalRateLimit },
    { label: "Leader lock", value: coordination.leaderLock },
    { label: "Lease", value: coordination.lease },
    { label: "Dequeue", value: coordination.paused ? "paused" : "active" },
    { label: "Session TTL", value: formatSeconds(sessionMaxAgeSeconds) },
    {
      label: "Session refresh",
      value: formatSeconds(sessionRefreshAfterSeconds),
    },
  ];

  const gateway = [
    { label: "Admin API URL", value: adminApiUrl },
    { label: "mTLS cert", value: mtlsCert },
    { label: "mTLS key", value: mtlsKey },
    { label: "mTLS CA", value: mtlsCa },
    { label: "Mock data", value: mockEnabled },
  ];

  return (
    <div className="space-y-6">
      <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
        <SectionHeader
          title="Settings"
          description="System defaults and coordination configuration."
        />
        <div className="mt-6 grid gap-4 md:grid-cols-2">
          {settings.map((item) => (
            <div
              key={item.label}
              className="rounded-2xl border border-soft bg-[var(--card)] p-4"
            >
              <p className="text-xs uppercase tracking-[0.2em] text-muted">
                {item.label}
              </p>
              <p className="mt-3 font-display text-lg font-semibold">
                {item.value}
              </p>
            </div>
          ))}
        </div>
      </section>

      <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
        <SectionHeader
          title="Gateway"
          description="Admin API connectivity and environment configuration."
        />
        <div className="mt-6 grid gap-4 md:grid-cols-2">
          {gateway.map((item) => (
            <div
              key={item.label}
              className="rounded-2xl border border-soft bg-[var(--card)] p-4"
            >
              <p className="text-xs uppercase tracking-[0.2em] text-muted">
                {item.label}
              </p>
              <p className="mt-3 text-sm font-medium text-slate-700">
                {item.value}
              </p>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
