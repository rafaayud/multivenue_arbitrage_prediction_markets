const defaultWebSocketUrl = "ws://localhost:8000"

function withoutTrailingSlash(value: string): string {
  return value.replace(/\/+$/, "")
}

function browserWebSocketOrigin(): string {
  if (typeof window === "undefined") {
    return defaultWebSocketUrl
  }
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:"
  return `${protocol}//${window.location.host}`
}

export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL
  ? withoutTrailingSlash(import.meta.env.VITE_API_BASE_URL)
  : "/api"

export const WS_BASE_URL = import.meta.env.VITE_WS_BASE_URL
  ? withoutTrailingSlash(import.meta.env.VITE_WS_BASE_URL)
  : browserWebSocketOrigin()

export const MAX_RECENT_OPPORTUNITIES = 200
