import type {
  FillCaptureSummary,
  HistogramSeries,
  LatencySummary,
  PipelineMetrics,
  PrometheusSample,
} from "@/types/metrics"

const histogramMetrics = new Set([
  "order_operation_seconds",
  "arbitrage_orderbook_age_seconds",
  "arbitrage_orderbook_skew_seconds",
  "arbitrage_stage_seconds",
  "arbitrage_order_fill_ratio",
  "polymarket_ws_message_age_seconds",
  "polymarket_ws_message_queue_wait_seconds",
  "polymarket_ws_dequeue_to_emit_seconds",
  "market_feed_receive_to_sink_seconds",
  "market_feed_sink_publish_seconds",
  "market_feed_venue_age_seconds",
  "market_feed_ws_message_queue_wait_seconds",
  "trading_event_loop_lag_seconds",
  "market_worker_event_loop_lag_seconds",
  "market_worker_intent_ipc_seconds",
  "market_worker_validation_round_trip_seconds",
  "market_worker_book_age_seconds",
])

const criticalStages = [
  "opportunity_to_submit",
  "submit_start_skew",
  "opportunity_to_both_submits",
  "submit_to_ack",
  "opportunity_to_both_acks",
  "ack_to_first_fill",
  "opportunity_to_both_fills",
  "ack_to_terminal",
  "terminal_skew",
  "opportunity_to_both_terminal",
]

const successfulOutcomes = new Set([
  "completed",
  "filled",
  "success",
  "succeeded",
])

function parseLabels(source: string): Record<string, string> {
  const labels: Record<string, string> = {}
  const matcher = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"])*)"/g
  for (const match of source.matchAll(matcher)) {
    if (match[1] && match[2] !== undefined) {
      labels[match[1]] = match[2].replace(/\\"/g, '"')
    }
  }
  return labels
}

function seriesKey(
  metric: string,
  labels: Record<string, string>,
): string {
  return `${metric}:${Object.entries(labels)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join(",")}`
}

/** Parse numeric samples from the Prometheus text exposition format. */
export function parsePrometheusSamples(source: string): PrometheusSample[] {
  const samples: PrometheusSample[] = []
  const linePattern =
    /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([^\s#]+)$/

  for (const rawLine of source.split(/\r?\n/)) {
    const line = rawLine.trim()
    if (!line || line.startsWith("#")) continue
    const match = line.match(linePattern)
    if (!match?.[1] || !match[3]) continue
    const value = Number(match[3])
    if (!Number.isFinite(value)) continue
    samples.push({
      metric: match[1],
      labels: parseLabels(match[2] ?? ""),
      value,
    })
  }
  return samples
}

/** Parse the latency histograms used by the dashboard from Prometheus text. */
export function parsePrometheusHistograms(
  source: string,
): HistogramSeries[] {
  const series = new Map<string, HistogramSeries>()
  const linePattern =
    /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([^\s#]+)$/

  for (const rawLine of source.split(/\r?\n/)) {
    const line = rawLine.trim()
    if (!line || line.startsWith("#")) continue
    const match = line.match(linePattern)
    if (!match?.[1] || !match[3]) continue

    const sampleName = match[1]
    const suffix = ["_bucket", "_count", "_sum"].find((value) =>
      sampleName.endsWith(value),
    )
    if (!suffix) continue
    const metric = sampleName.slice(0, -suffix.length)
    if (!histogramMetrics.has(metric)) continue

    const labels = parseLabels(match[2] ?? "")
    const upperBound = labels.le
    delete labels.le
    const key = seriesKey(metric, labels)
    const current = series.get(key) ?? {
      metric,
      labels,
      buckets: [],
      count: 0,
      sum: 0,
    }
    const value = Number(match[3])
    if (!Number.isFinite(value)) continue

    if (suffix === "_bucket" && upperBound !== undefined) {
      current.buckets.push({
        upperBound: upperBound === "+Inf" ? Infinity : Number(upperBound),
        count: value,
      })
    } else if (suffix === "_count") {
      current.count = value
    } else if (suffix === "_sum") {
      current.sum = value
    }
    series.set(key, current)
  }

  return [...series.values()].map((item) => ({
    ...item,
    buckets: item.buckets.sort(
      (left, right) => left.upperBound - right.upperBound,
    ),
  }))
}

/**
 * Estimate a histogram quantile by interpolating within cumulative buckets.
 *
 * @returns The estimated bucket value, or zero when the series is empty.
 */
export function histogramQuantile(
  series: HistogramSeries,
  quantile: number,
): number {
  if (series.count <= 0 || series.buckets.length === 0) {
    return 0
  }
  const target = series.count * quantile
  let previousBound = 0
  let previousCount = 0

  for (const bucket of series.buckets) {
    if (bucket.count >= target) {
      if (!Number.isFinite(bucket.upperBound)) {
        return previousBound
      }
      const countInBucket = bucket.count - previousCount
      if (countInBucket <= 0) {
        return bucket.upperBound
      }
      const position = (target - previousCount) / countInBucket
      return previousBound + (bucket.upperBound - previousBound) * position
    }
    previousBound = bucket.upperBound
    previousCount = bucket.count
  }
  return previousBound
}

function histogramMaximum(
  series: HistogramSeries,
): Pick<LatencySummary, "maximumLowerBound" | "maximumUpperBound"> {
  let previousBound = 0
  for (const bucket of series.buckets) {
    if (bucket.count >= series.count) {
      return {
        maximumLowerBound: previousBound,
        maximumUpperBound: Number.isFinite(bucket.upperBound)
          ? bucket.upperBound
          : null,
      }
    }
    previousBound = bucket.upperBound
  }
  return { maximumLowerBound: previousBound, maximumUpperBound: null }
}

function histogramDeltas(
  current: HistogramSeries[],
  previous: HistogramSeries[],
): HistogramSeries[] {
  const previousByKey = new Map(
    previous.map((series) => [seriesKey(series.metric, series.labels), series]),
  )
  return current.map((series) => {
    const before = previousByKey.get(seriesKey(series.metric, series.labels))
    if (!before || series.count < before.count) return series
    const previousBuckets = new Map(
      before.buckets.map((bucket) => [bucket.upperBound, bucket.count]),
    )
    return {
      ...series,
      buckets: series.buckets.map((bucket) => ({
        ...bucket,
        count: Math.max(
          0,
          bucket.count - (previousBuckets.get(bucket.upperBound) ?? 0),
        ),
      })),
      count: series.count - before.count,
      sum: series.sum - before.sum,
    }
  })
}

function readableLabels(labels: Record<string, string>): string {
  const values = Object.values(labels)
  return values.length ? values.join(" · ") : "global"
}

/** Convert supported Prometheus histograms into dashboard percentile summaries. */
export function latencySummaries(
  source: string,
  previousSource?: string,
): LatencySummary[] {
  const current = parsePrometheusHistograms(source)
  const series =
    previousSource === undefined
      ? current
      : histogramDeltas(current, parsePrometheusHistograms(previousSource))
  return series
    .filter((series) => series.count > 0)
    .map((series) => ({
      id: seriesKey(series.metric, series.labels),
      metric: series.metric,
      label: readableLabels(series.labels),
      labels: series.labels,
      p50: histogramQuantile(series, 0.5),
      p95: histogramQuantile(series, 0.95),
      p99: histogramQuantile(series, 0.99),
      p999: series.count >= 1_000 ? histogramQuantile(series, 0.999) : null,
      ...histogramMaximum(series),
      count: series.count,
    }))
}

function maximum(values: number[]): number | null {
  return values.length ? Math.max(...values) : null
}

function normalizedVenue(value: string | undefined): string | undefined {
  const normalized = value?.trim().toLowerCase()
  return normalized || undefined
}

/** Build pipeline diagnostics from Prometheus metrics. */
export function pipelineMetrics(
  source: string,
  previousSource?: string,
  windowSeconds?: number,
): PipelineMetrics {
  const histograms = parsePrometheusHistograms(source)
  const summaries = latencySummaries(source, previousSource)
  const samples = parsePrometheusSamples(source)
  const previousSamples =
    previousSource === undefined ? [] : parsePrometheusSamples(previousSource)
  const stages = summaries
    .filter((summary) => summary.metric === "arbitrage_stage_seconds")
    .sort((left, right) => {
      const stageOrder =
        criticalStages.indexOf(left.labels.stage ?? "") -
        criticalStages.indexOf(right.labels.stage ?? "")
      return stageOrder || left.label.localeCompare(right.label)
    })
  const outcomes = samples.filter(
    (sample) => sample.metric === "arbitrage_order_outcomes_total",
  )
  const venues = new Set(
    [
      ...histograms.map((series) => normalizedVenue(series.labels.venue)),
      ...outcomes.map((sample) => normalizedVenue(sample.labels.venue)),
    ].filter((venue): venue is string => Boolean(venue)),
  )
  const stageP95 = (stage: string) =>
    maximum(
      stages
        .filter((summary) => summary.labels.stage === stage)
        .map((summary) => summary.p95),
    )
  const sampleValue = (metric: string) =>
    samples.find((sample) => sample.metric === metric)?.value ?? null
  const counterRate = (metric: string) => {
    if (!windowSeconds || windowSeconds <= 0) return null
    const current = sampleValue(metric)
    const previous = previousSamples.find(
      (sample) => sample.metric === metric,
    )?.value
    return current !== null && previous !== undefined && current >= previous
      ? (current - previous) / windowSeconds
      : null
  }
  const polymarketPumps =
    samples.find(
      (sample) =>
        sample.metric === "market_feed_active_pumps" &&
        normalizedVenue(sample.labels.venue) === "polymarket",
    )?.value ?? null
  const histogramP95 = (metric: string) =>
    maximum(
      summaries
        .filter((summary) => summary.metric === metric)
        .map((summary) => summary.p95),
    )
  const histogramP99 = (metric: string) =>
    maximum(
      summaries
        .filter((summary) => summary.metric === metric)
        .map((summary) => summary.p99),
    )
  const polymarketPriceChangeAgeP95 = maximum(
    summaries
      .filter(
        (summary) =>
          summary.metric === "polymarket_ws_message_age_seconds" &&
          (!summary.labels.event_type ||
            summary.labels.event_type === "price_change"),
      )
      .map((summary) => summary.p95),
  )
  const polymarketPriceChangeAgeP99 = maximum(
    summaries
      .filter(
        (summary) =>
          summary.metric === "polymarket_ws_message_age_seconds" &&
          (!summary.labels.event_type ||
            summary.labels.event_type === "price_change"),
      )
      .map((summary) => summary.p99),
  )
  const polymarketQueueWait = summaries
    .filter(
      (summary) =>
        summary.metric === "polymarket_ws_message_queue_wait_seconds",
    )
    .sort((left, right) => right.p99 - left.p99)[0]
  const polymarketCounter = (
    metric: string,
    label: string,
    value: string,
  ) =>
    samples.find(
      (sample) => sample.metric === metric && sample.labels[label] === value,
    )?.value ?? 0
  const feedVenues = new Set(
    [
      ...summaries
        .filter((summary) =>
          [
            "market_feed_receive_to_sink_seconds",
            "market_feed_sink_publish_seconds",
            "market_feed_venue_age_seconds",
            "market_feed_ws_message_queue_wait_seconds",
          ].includes(summary.metric),
        )
        .map((summary) => normalizedVenue(summary.labels.venue)),
      ...samples
        .filter(
          (sample) =>
            [
              "market_feed_venue_timestamp_delta_seconds",
              "market_feed_ws_queue_depth_frames",
              "market_feed_ws_queue_high_watermark_frames",
              "market_feed_ws_paused",
            ].includes(sample.metric),
        )
        .map((sample) => normalizedVenue(sample.labels.venue)),
    ].filter((venue): venue is string => Boolean(venue)),
  )
  const feedP95 = (metric: string, venue: string) =>
    maximum(
      summaries
        .filter(
          (summary) =>
            summary.metric === metric &&
            normalizedVenue(summary.labels.venue) === venue,
        )
        .map((summary) => summary.p95),
    )
  const feedQuantile = (
    metric: string,
    venue: string,
    quantile: "p50" | "p95" | "p99",
  ) =>
    maximum(
      summaries
        .filter(
          (summary) =>
            summary.metric === metric &&
            normalizedVenue(summary.labels.venue) === venue,
        )
        .map((summary) => summary[quantile]),
    )
  const feedSample = (metric: string, venue: string) =>
    samples.find(
      (sample) =>
        sample.metric === metric &&
        normalizedVenue(sample.labels.venue) === venue,
    )?.value ?? null
  const workerPartitions = new Set(
    samples
      .filter((sample) => sample.metric === "market_worker_up")
      .map((sample) => sample.labels.partition)
      .filter((partition): partition is string => Boolean(partition)),
  )
  const workerSample = (metric: string, partition: string) =>
    samples.find(
      (sample) =>
        sample.metric === metric && sample.labels.partition === partition,
    )?.value ?? null
  const workerCounter = (
    metric: string,
    partition: string,
    predicate: (labels: Record<string, string>) => boolean = () => true,
  ) =>
    samples
      .filter(
        (sample) =>
          sample.metric === metric &&
          sample.labels.partition === partition &&
          predicate(sample.labels),
      )
      .reduce((total, sample) => total + sample.value, 0)
  const workerP95 = (
    metric: string,
    partition: string,
    predicate: (labels: Record<string, string>) => boolean = () => true,
  ) =>
    maximum(
      summaries
        .filter(
          (summary) =>
            summary.metric === metric &&
            summary.labels.partition === partition &&
            predicate(summary.labels),
        )
        .map((summary) => summary.p95),
    )
  const captureSummary = (producer: string): FillCaptureSummary => {
    const value = (metric: string) => samples.find((sample) =>
      sample.metric === `predict_fill_capture_${metric}` &&
      sample.labels.producer === producer,
    )?.value ?? null
    const states = samples.filter((sample) =>
      sample.metric === "predict_fill_capture_state" &&
      sample.labels.producer === producer && sample.value === 1,
    )
    const state = states.length === 1 ? states[0]?.labels.state : undefined
    const writerAlive = value("writer_alive")
    return {
      state: state && ["disabled", "running", "closed", "disk_limit", "io_error", "start_failed"].includes(state)
        ? state : "unknown",
      queueDepth: value("queue_depth"),
      queueCapacity: value("queue_capacity"),
      droppedSamples: value("dropped_samples"),
      offersAfterStop: value("offers_after_stop"),
      bytes: value("bytes"),
      activeWindows: value("active_windows"),
      writerAlive: writerAlive === null ? null : writerAlive > 0,
      writerProgressAge: value("writer_progress_age_seconds"),
    }
  }

  return {
    latencies: summaries,
    windowSeconds: windowSeconds ?? null,
    parentCapture: captureSummary("parent"),
    stages,
    cycleP95: stageP95("opportunity_to_both_terminal"),
    bothSubmitP95: stageP95("opportunity_to_both_submits"),
    submitSkewP95: stageP95("submit_start_skew"),
    venues: [...venues].sort().map((venue) => {
      return {
        venue: venue.charAt(0).toUpperCase() + venue.slice(1),
        submitP50: maximum(
          summaries
            .filter(
              (summary) =>
                summary.metric === "order_operation_seconds" &&
                normalizedVenue(summary.labels.venue) === venue &&
                summary.labels.operation === "submit",
            )
            .map((summary) => summary.p50),
        ),
        submitP95: maximum(
          summaries
            .filter(
              (summary) =>
                summary.metric === "order_operation_seconds" &&
                normalizedVenue(summary.labels.venue) === venue &&
                summary.labels.operation === "submit",
            )
            .map((summary) => summary.p95),
        ),
        submitP99: maximum(
          summaries
            .filter(
              (summary) =>
                summary.metric === "order_operation_seconds" &&
                normalizedVenue(summary.labels.venue) === venue &&
                summary.labels.operation === "submit",
            )
            .map((summary) => summary.p99),
        ),
        bookAgeP50: maximum(
          summaries
            .filter(
              (summary) =>
                summary.metric === "arbitrage_orderbook_age_seconds" &&
                normalizedVenue(summary.labels.venue) === venue,
            )
            .map((summary) => summary.p50),
        ),
        bookAgeP95: maximum(
          summaries
            .filter(
              (summary) =>
                summary.metric === "arbitrage_orderbook_age_seconds" &&
                normalizedVenue(summary.labels.venue) === venue,
            )
            .map((summary) => summary.p95),
        ),
        bookAgeP99: maximum(
          summaries
            .filter(
              (summary) =>
                summary.metric === "arbitrage_orderbook_age_seconds" &&
                normalizedVenue(summary.labels.venue) === venue,
            )
            .map((summary) => summary.p99),
        ),
      }
    }),
    failures: outcomes
      .filter(
        (sample) =>
          !successfulOutcomes.has(sample.labels.outcome?.toLowerCase() ?? ""),
      )
      .map((sample) => ({
        id: seriesKey(sample.metric, sample.labels),
        venue: sample.labels.venue ?? "unknown",
        leg: sample.labels.leg ?? "unknown",
        reason: sample.labels.outcome ?? "unknown",
        count: sample.value,
      }))
      .sort((left, right) => right.count - left.count)
      .slice(0, 5),
    polymarketWs: {
      activeSockets: sampleValue("polymarket_ws_active_sockets"),
      expectedSockets: sampleValue("polymarket_ws_expected_sockets"),
      activePumps: polymarketPumps,
      connections: sampleValue("polymarket_ws_connections_total"),
      framesReceived: sampleValue("polymarket_ws_frames_received_total"),
      booksEmitted: sampleValue("polymarket_books_emitted_total"),
      framesPerSecond: counterRate("polymarket_ws_frames_received_total"),
      booksPerSecond: counterRate("polymarket_books_emitted_total"),
      queueDepth: sampleValue("polymarket_ws_queue_depth_frames"),
      queueHighWatermark: sampleValue(
        "polymarket_ws_queue_high_watermark_frames",
      ),
      pausedSockets: sampleValue("polymarket_ws_paused_sockets"),
      messageAgeP95: polymarketPriceChangeAgeP95,
      messageAgeP99: polymarketPriceChangeAgeP99,
      priceChangeTimestampDelta:
        samples.find(
          (sample) =>
            sample.metric === "polymarket_ws_venue_timestamp_delta_seconds" &&
            sample.labels.event_type === "price_change",
        )?.value ?? null,
      queueWaitP95: polymarketQueueWait?.p95 ?? null,
      queueWaitP99: polymarketQueueWait?.p99 ?? null,
      queueWaitConditionId:
        polymarketQueueWait?.labels.condition_id ?? null,
      dequeueToEmitP95: histogramP95(
        "polymarket_ws_dequeue_to_emit_seconds",
      ),
      dequeueToEmitP99: histogramP99(
        "polymarket_ws_dequeue_to_emit_seconds",
      ),
      resyncs: samples
        .filter((sample) => sample.metric === "polymarket_ws_resyncs_total")
        .reduce((total, sample) => total + sample.value, 0),
      desyncs: polymarketCounter(
        "polymarket_ws_resyncs_total",
        "reason",
        "desync",
      ),
      messageAgeResyncs: polymarketCounter(
        "polymarket_ws_resyncs_total",
        "reason",
        "message_age",
      ),
      queueDepthResyncs: polymarketCounter(
        "polymarket_ws_resyncs_total",
        "reason",
        "queue_depth",
      ),
      queueOverloadsDrained: polymarketCounter(
        "polymarket_ws_queue_overloads_total",
        "outcome",
        "drained",
      ),
      queueOverloadsRestarted: polymarketCounter(
        "polymarket_ws_queue_overloads_total",
        "outcome",
        "restarted",
      ),
    },
    eventLoopLagP95: histogramP95("trading_event_loop_lag_seconds"),
    marketFeeds: [...feedVenues].sort().map((venue) => ({
      venue: venue.charAt(0).toUpperCase() + venue.slice(1),
      receiveToSinkP95: feedP95("market_feed_receive_to_sink_seconds", venue),
      sinkPublishP95: feedP95("market_feed_sink_publish_seconds", venue),
      venueTimestampDelta: feedSample(
        "market_feed_venue_timestamp_delta_seconds",
        venue,
      ),
      venueAgeP50: feedQuantile("market_feed_venue_age_seconds", venue, "p50"),
      venueAgeP95: feedQuantile("market_feed_venue_age_seconds", venue, "p95"),
      venueAgeP99: feedQuantile("market_feed_venue_age_seconds", venue, "p99"),
      wsQueueDepth: feedSample("market_feed_ws_queue_depth_frames", venue),
      wsQueueHighWatermark: feedSample(
        "market_feed_ws_queue_high_watermark_frames",
        venue,
      ),
      wsPaused: (() => {
        const paused = feedSample("market_feed_ws_paused", venue)
        return paused === null ? null : paused > 0
      })(),
      wsQueueWaitP95: feedP95(
        "market_feed_ws_message_queue_wait_seconds",
        venue,
      ),
    })),
    workerIpc: {
      depth: sampleValue("market_worker_ipc_queue_depth"),
      capacity: sampleValue("market_worker_ipc_queue_capacity"),
      highWatermark: sampleValue("market_worker_ipc_queue_high_watermark"),
    },
    marketWorkers: [...workerPartitions].sort().map((partition) => {
      const workerVenues = new Set(
        [
          ...samples
            .filter(
              (sample) =>
                sample.labels.partition === partition &&
                [
                  "market_worker_ws_queue_depth_frames",
                  "market_worker_ws_queue_high_watermark_frames",
                  "market_worker_ws_paused_streams",
                  "market_worker_ws_queue_wait_max_seconds",
                  "market_worker_timing_seconds",
                ].includes(sample.metric),
            )
            .map((sample) => normalizedVenue(sample.labels.venue)),
        ].filter((venue): venue is string => Boolean(venue)),
      )
      const venueSample = (metric: string, venue: string, stage?: string) =>
        samples.find(
          (sample) =>
            sample.metric === metric &&
            sample.labels.partition === partition &&
            normalizedVenue(sample.labels.venue) === venue &&
            (stage === undefined || sample.labels.stage === stage),
        )?.value ?? null
      const validationResults = samples.filter((sample) =>
        sample.metric === "market_worker_validation_results_total" &&
        sample.labels.partition === partition,
      )
      return {
        partition,
        alive: (workerSample("market_worker_up", partition) ?? 0) > 0,
        heartbeatAge: workerSample(
          "market_worker_heartbeat_age_seconds",
          partition,
        ),
        pid: workerSample("market_worker_pid", partition),
        generation: workerSample("market_worker_generation", partition),
        starts: workerCounter("market_worker_starts_total", partition),
        restarts: workerCounter("market_worker_restarts_total", partition),
        unexpectedExits: workerCounter(
          "market_worker_unexpected_exits_total",
          partition,
        ),
        cpuSeconds: workerSample("market_worker_cpu_seconds", partition),
        memoryBytes: workerSample("market_worker_memory_bytes", partition),
        eventLoopLagP95: workerP95(
          "market_worker_event_loop_lag_seconds",
          partition,
        ),
        intentIpcP95: workerP95(
          "market_worker_intent_ipc_seconds",
          partition,
        ),
        validationP95: workerP95("market_worker_validation_round_trip_seconds", partition),
        validationQueueDepth: workerSample("market_worker_validation_queue_depth", partition),
        validationQueueCapacity: workerSample("market_worker_validation_queue_capacity", partition),
        validationOutcomes: validationResults.length ? validationResults
          .filter((sample) => sample.value > 0)
          .map((sample) => ({ outcome: sample.labels.outcome ?? "unknown", count: sample.value }))
          .sort((left, right) => right.count - left.count) : null,
        capture: captureSummary(partition),
        rejectedIntents: workerCounter(
          "market_worker_intents_rejected_total",
          partition,
        ),
        submissionSourceAgeP95: workerP95(
          "market_worker_book_age_seconds",
          partition,
          (labels) =>
            labels.stage === "submission" && labels.clock === "source",
        ),
        shadowAgreements: workerCounter(
          "market_worker_shadow_detections_total",
          partition,
          (labels) => labels.outcome === "agree",
        ),
        shadowDisagreements: workerCounter(
          "market_worker_shadow_detections_total",
          partition,
          (labels) => labels.outcome === "disagree",
        ),
        venues: [...workerVenues].sort().map((venue) => ({
          venue: venue.charAt(0).toUpperCase() + venue.slice(1),
          queueDepth: venueSample(
            "market_worker_ws_queue_depth_frames",
            venue,
          ),
          queueHighWatermark: venueSample(
            "market_worker_ws_queue_high_watermark_frames",
            venue,
          ),
          pausedStreams: venueSample(
            "market_worker_ws_paused_streams",
            venue,
          ),
          queueWaitMax: venueSample(
            "market_worker_ws_queue_wait_max_seconds",
            venue,
          ),
          sourceToTransportMax: venueSample(
            "market_worker_timing_seconds",
            venue,
            "source_to_transport_max",
          ),
          transportToSinkMax: venueSample(
            "market_worker_timing_seconds",
            venue,
            "transport_to_sink_max",
          ),
        })),
      }
    }),
  }
}
