import { cache } from "react";
import {
  fetchDlq,
  fetchOverview,
  fetchQueues,
  fetchSchedules,
} from "@/lib/api";
import {
  coordinationStatus,
  dlqEntries,
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

export const getDlqEntries = cache(async () => {
  try {
    const { entries } = await fetchDlq();
    return entries;
  } catch (error) {
    if (
      process.env.NODE_ENV !== "production" &&
      (process.env.WORKER_ADMIN_ALLOW_MOCK ??
        process.env.ADMIN_UI_ALLOW_MOCK) !== "false"
    ) {
      return dlqEntries;
    }

    const detail =
      error instanceof Error ? error.message : "unknown error";
    throw new Error(`Failed to load DLQ entries: ${detail}`);
  }
});
