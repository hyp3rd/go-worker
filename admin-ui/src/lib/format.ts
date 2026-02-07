export const formatNumber = (value: number): string => {
  if (value < 0) {
    return "—";
  }

  return new Intl.NumberFormat("en-US").format(value);
};

export const formatLatency = (ms: number): string => {
  if (ms < 0) {
    return "—";
  }

  return `${ms}ms`;
};

const trimDecimal = (value: string) => value.replace(/\.0$/, "");

export const formatDuration = (ms: number): string => {
  if (ms <= 0) {
    return "—";
  }

  if (ms < 1000) {
    return `${ms}ms`;
  }

  const seconds = ms / 1000;
  if (seconds < 60) {
    return `${trimDecimal(seconds.toFixed(1))}s`;
  }

  const minutes = seconds / 60;
  if (minutes < 60) {
    return `${trimDecimal(minutes.toFixed(1))}m`;
  }

  const hours = minutes / 60;
  if (hours < 24) {
    return `${trimDecimal(hours.toFixed(1))}h`;
  }

  const days = hours / 24;
  return `${trimDecimal(days.toFixed(1))}d`;
};
