export type VenueHealthStatus = "operational" | "degraded" | "unavailable"

export interface VenueHealth {
  venue_id: string
  status: VenueHealthStatus
  checked_at: string
  latency_ms: number | null
  source: string
  message: string
  error_type: string | null
  http_status: number | null
  retryable: boolean
}

export interface VenueHealthReport {
  generated_at: string
  overall_status: VenueHealthStatus
  venues: VenueHealth[]
}
