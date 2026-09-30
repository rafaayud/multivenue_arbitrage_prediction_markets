export interface HistogramBucket {
  upperBound: number
  count: number
}

/** One cumulative Prometheus histogram distinguished by its non-`le` labels. */
export interface HistogramSeries {
  metric: string
  labels: Record<string, string>
  buckets: HistogramBucket[]
  count: number
  sum: number
}

/** One numeric sample from the Prometheus text exposition format. */
export interface PrometheusSample {
  metric: string
  labels: Record<string, string>
  value: number
}

/** Dashboard-ready percentile estimates for one populated latency histogram. */
export interface LatencySummary {
  id: string
  metric: string
  label: string
  labels: Record<string, string>
  p50: number
  p95: number
  p99: number
  p999: number | null
  maximumLowerBound: number
  maximumUpperBound: number | null
  count: number
}

export interface VenuePipelineSummary {
  venue: string
  submitP50: number | null
  submitP95: number | null
  submitP99: number | null
  bookAgeP50: number | null
  bookAgeP95: number | null
  bookAgeP99: number | null
}

export interface OrderOutcomeSummary {
  id: string
  venue: string
  leg: string
  reason: string
  count: number
}

/** Current Polymarket WebSocket pressure and latency diagnostics. */
export interface PolymarketWsSummary {
  activeSockets: number | null
  expectedSockets: number | null
  activePumps: number | null
  connections: number | null
  framesReceived: number | null
  booksEmitted: number | null
  framesPerSecond: number | null
  booksPerSecond: number | null
  queueDepth: number | null
  queueHighWatermark: number | null
  pausedSockets: number | null
  messageAgeP95: number | null
  messageAgeP99: number | null
  priceChangeTimestampDelta: number | null
  queueWaitP95: number | null
  queueWaitP99: number | null
  queueWaitConditionId: string | null
  dequeueToEmitP95: number | null
  dequeueToEmitP99: number | null
  resyncs: number
  desyncs: number
  messageAgeResyncs: number
  queueDepthResyncs: number
  queueOverloadsDrained: number
  queueOverloadsRestarted: number
}

/** Local handoff latency for one public venue feed. */
export interface MarketFeedSummary {
  venue: string
  receiveToSinkP95: number | null
  sinkPublishP95: number | null
  venueTimestampDelta: number | null
  venueAgeP50: number | null
  venueAgeP95: number | null
  venueAgeP99: number | null
  wsQueueDepth: number | null
  wsQueueHighWatermark: number | null
  wsPaused: boolean | null
  wsQueueWaitP95: number | null
}

/** Worker-owned WebSocket pressure aggregated once per second. */
export interface MarketWorkerVenueSummary {
  venue: string
  queueDepth: number | null
  queueHighWatermark: number | null
  pausedStreams: number | null
  queueWaitMax: number | null
  sourceToTransportMax: number | null
  transportToSinkMax: number | null
}

/** Health and IPC telemetry for one process partition. */
export interface MarketWorkerSummary {
  partition: string
  alive: boolean
  heartbeatAge: number | null
  pid: number | null
  generation: number | null
  starts: number
  restarts: number
  unexpectedExits: number
  cpuSeconds: number | null
  memoryBytes: number | null
  eventLoopLagP95: number | null
  intentIpcP95: number | null
  validationP95: number | null
  validationQueueDepth: number | null
  validationQueueCapacity: number | null
  validationOutcomes: Array<{ outcome: string; count: number }> | null
  capture: FillCaptureSummary
  rejectedIntents: number
  submissionSourceAgeP95: number | null
  shadowAgreements: number
  shadowDisagreements: number
  venues: MarketWorkerVenueSummary[]
}

/** Optional capture health; missing metrics remain unknown rather than healthy. */
export interface FillCaptureSummary {
  state: string
  queueDepth: number | null
  queueCapacity: number | null
  droppedSamples: number | null
  offersAfterStop: number | null
  bytes: number | null
  activeWindows: number | null
  writerAlive: boolean | null
  writerProgressAge: number | null
}

/** Shared bounded opportunity-intent queue pressure. */
export interface MarketWorkerIpcSummary {
  depth: number | null
  capacity: number | null
  highWatermark: number | null
}

/** Pipeline diagnostics derived from the existing Prometheus endpoint. */
export interface PipelineMetrics {
  latencies: LatencySummary[]
  windowSeconds: number | null
  stages: LatencySummary[]
  cycleP95: number | null
  bothSubmitP95: number | null
  submitSkewP95: number | null
  venues: VenuePipelineSummary[]
  failures: OrderOutcomeSummary[]
  polymarketWs: PolymarketWsSummary
  eventLoopLagP95: number | null
  marketFeeds: MarketFeedSummary[]
  workerIpc: MarketWorkerIpcSummary
  marketWorkers: MarketWorkerSummary[]
  parentCapture: FillCaptureSummary
}
