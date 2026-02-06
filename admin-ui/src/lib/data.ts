import { cache } from "react";
import {
  fetchDlq,
  fetchHealth,
  fetchOverview,
  fetchQueues,
  fetchQueueDetail,
  fetchSchedules,
} from "@/lib/api";
import {
  coordinationStatus,
  adminActionCounters,
  dlqEntries,
  healthInfo,
  jobSchedules,
  overviewStats,
  queueSummaries,
} from "@/lib/mock-data";

export const getOverviewStats = cache(async () => {
  try {
    const { stats } = await fetchOverview();
    return stats;
  } catch (error) {
    if (
      process.env.NODE_ENV !== "production" &&
      (process.env.WORKER_ADMIN_ALLOW_MOCK ??
        process.env.ADMIN_UI_ALLOW_MOCK) !== "false"
    ) {
      return overviewStats;
    }

    const detail =
      error instanceof Error ? error.message : "unknown error";
    throw new Error(`Failed to load overview stats: ${detail}`);
  }
});

export const getHealthInfo = cache(async () => {
  try {
    return await fetchHealth();
  } catch (error) {
    if (
      process.env.NODE_ENV !== "production" &&
      (process.env.WORKER_ADMIN_ALLOW_MOCK ??
        process.env.ADMIN_UI_ALLOW_MOCK) !== "false"
    ) {
      return healthInfo;
    }

    const detail =
      error instanceof Error ? error.message : "unknown error";
    throw new Error(`Failed to load health info: ${detail}`);
  }
});

export const getCoordinationStatus = cache(async () => {
  try {
    const { coordination } = await fetchOverview();
    return coordination;
  } catch (error) {
    if (
      process.env.NODE_ENV !== "production" &&
      (process.env.WORKER_ADMIN_ALLOW_MOCK ??
        process.env.ADMIN_UI_ALLOW_MOCK) !== "false"
    ) {
      return coordinationStatus;
    }

    const detail =
      error instanceof Error ? error.message : "unknown error";
    throw new Error(`Failed to load coordination status: ${detail}`);
  }
});

export const getAdminActionCounters = cache(async () => {
  try {
    const { actions } = await fetchOverview();
    return actions;
  } catch (error) {
    if (
      process.env.NODE_ENV !== "production" &&
      (process.env.WORKER_ADMIN_ALLOW_MOCK ??
        process.env.ADMIN_UI_ALLOW_MOCK) !== "false"
    ) {
      return adminActionCounters;
    }

    const detail =
      error instanceof Error ? error.message : "unknown error";
    throw new Error(`Failed to load admin actions: ${detail}`);
  }
});

export const getQueueSummaries = cache(async () => {
  try {
    const { queues } = await fetchQueues();
    return queues;
  } catch (error) {
    if (
      process.env.NODE_ENV !== "production" &&
      (process.env.WORKER_ADMIN_ALLOW_MOCK ??
        process.env.ADMIN_UI_ALLOW_MOCK) !== "false"
    ) {
      return queueSummaries;
    }

    const detail =
      error instanceof Error ? error.message : "unknown error";
    throw new Error(`Failed to load queue summaries: ${detail}`);
  }
});

export const getQueueDetail = cache(async (name: string) => {
  if (!name) {
    throw new Error("Queue name is required");
  }

  try {
    const { queue } = await fetchQueueDetail(name);
    return queue;
  } catch (error) {
    if (
      process.env.NODE_ENV !== "production" &&
      (process.env.WORKER_ADMIN_ALLOW_MOCK ??
        process.env.ADMIN_UI_ALLOW_MOCK) !== "false"
    ) {
      const fallback =
        queueSummaries.find((queue) => queue.name === name) ??
        queueSummaries[0];
      if (fallback) {
        return fallback;
      }
    }

    const detail =
      error instanceof Error ? error.message : "unknown error";
    throw new Error(`Failed to load queue detail: ${detail}`);
  }
});

export const getJobSchedules = cache(async () => {
  try {
    const { schedules } = await fetchSchedules();
    return schedules;
  } catch (error) {
    if (
      process.env.NODE_ENV !== "production" &&
      (process.env.WORKER_ADMIN_ALLOW_MOCK ??
        process.env.ADMIN_UI_ALLOW_MOCK) !== "false"
    ) {
      return jobSchedules;
    }

    const detail =
      error instanceof Error ? error.message : "unknown error";
    throw new Error(`Failed to load job schedules: ${detail}`);
  }
});

export const getDlqEntries = cache(async (params?: {
  limit?: number;
  offset?: number;
  queue?: string;
  handler?: string;
  query?: string;
}) => {
  try {
    const { entries, total } = await fetchDlq(params);
    return { entries, total };
  } catch (error) {
    if (
      process.env.NODE_ENV !== "production" &&
      (process.env.WORKER_ADMIN_ALLOW_MOCK ??
        process.env.ADMIN_UI_ALLOW_MOCK) !== "false"
    ) {
      return { entries: dlqEntries, total: dlqEntries.length };
    }

    const detail =
      error instanceof Error ? error.message : "unknown error";
    throw new Error(`Failed to load DLQ entries: ${detail}`);
  }
});
