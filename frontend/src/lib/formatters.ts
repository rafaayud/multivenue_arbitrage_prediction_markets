export function formatPrice(value: number): string {
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  })
}

export function formatQuantity(value: number): string {
  return value.toLocaleString("en-US", { maximumFractionDigits: 4 })
}

export function formatPercent(value: number): string {
  return `${(value * 100).toFixed(2)}%`
}

export function formatLatency(seconds: number): string {
  if (seconds < 1) {
    return `${Math.round(seconds * 1_000)}ms`
  }
  return `${seconds.toFixed(2)}s`
}

export function formatTimestamp(value: string | Date | null): string {
  if (!value) return "No data"
  const date = value instanceof Date ? value : new Date(value)
  return new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    day: "2-digit",
    month: "short",
  }).format(date)
}

export function intervalLabel(seconds: number): string {
  if (seconds % 86_400 === 0) return `${seconds / 86_400}d`
  if (seconds % 3_600 === 0) return `${seconds / 3_600}h`
  if (seconds % 60 === 0) return `${seconds / 60}m`
  return `${seconds}s`
}
