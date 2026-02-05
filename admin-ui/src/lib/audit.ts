export type AuditAction = "pause" | "resume" | "replay" | "logout";

export type AuditEvent = {
  action: AuditAction;
  at: number;
  detail?: string;
};

const storageKey = "workerctl_audit_events";
const maxEvents = 20;

export const recordAuditEvent = (event: AuditEvent) => {
  if (typeof window === "undefined") {
    return;
  }
  const existing = readAuditEvents();
  const updated = [event, ...existing].slice(0, maxEvents);
  try {
    window.localStorage.setItem(storageKey, JSON.stringify(updated));
  } catch {
    // ignore storage errors
  }
};

export const readAuditEvents = (): AuditEvent[] => {
  if (typeof window === "undefined") {
    return [];
  }
  try {
    const raw = window.localStorage.getItem(storageKey);
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw) as AuditEvent[];
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.filter((event) => event && typeof event.at === "number");
  } catch {
    return [];
  }
};
