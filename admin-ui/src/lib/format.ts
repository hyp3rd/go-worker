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
