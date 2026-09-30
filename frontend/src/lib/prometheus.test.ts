import { describe, expect, test } from "vitest"

import { latencySummaries, pipelineMetrics } from "@/lib/prometheus"

describe("latencySummaries", () => {
  test("calculates display percentiles from cumulative Prometheus buckets", () => {
    const source = `
order_operation_seconds_bucket{venue="limitless",operation="submit",le="0.1"} 5
order_operation_seconds_bucket{venue="limitless",operation="submit",le="0.5"} 9
order_operation_seconds_bucket{venue="limitless",operation="submit",le="+Inf"} 10
order_operation_seconds_sum{venue="limitless",operation="submit"} 1.4
order_operation_seconds_count{venue="limitless",operation="submit"} 10
`

    const [summary] = latencySummaries(source)
    expect(summary?.label).toBe("limitless · submit")
    expect(summary?.p50).toBe(0.1)
    expect(summary?.p95).toBe(0.5)
    expect(summary?.p999).toBeNull()
    expect(summary?.maximumLowerBound).toBe(0.5)
    expect(summary?.maximumUpperBound).toBeNull()
    expect(summary?.count).toBe(10)
  })

  test("builds pipeline cards, venue rows, and failure ranking", () => {
    const stage = (name: string, value: number) => `
arbitrage_stage_seconds_bucket{venue="limitless",leg="hedge",stage="${name}",le="${value}"} 1
arbitrage_stage_seconds_bucket{venue="limitless",leg="hedge",stage="${name}",le="+Inf"} 1
arbitrage_stage_seconds_sum{venue="limitless",leg="hedge",stage="${name}"} ${value}
arbitrage_stage_seconds_count{venue="limitless",leg="hedge",stage="${name}"} 1`
    const source = [
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
    ].map((name) => stage(name, 0.1)).join("\n") + `
arbitrage_orderbook_age_seconds_bucket{venue="limitless",le="0.5"} 2
arbitrage_orderbook_age_seconds_bucket{venue="limitless",le="+Inf"} 2
arbitrage_orderbook_age_seconds_sum{venue="limitless"} 0.4
arbitrage_orderbook_age_seconds_count{venue="limitless"} 2
arbitrage_order_fill_ratio_bucket{venue="limitless",leg="hedge",le="1.0"} 2
arbitrage_order_fill_ratio_bucket{venue="limitless",leg="hedge",le="+Inf"} 2
arbitrage_order_fill_ratio_sum{venue="limitless",leg="hedge"} 1.5
arbitrage_order_fill_ratio_count{venue="limitless",leg="hedge"} 2
arbitrage_order_outcomes_total{venue="limitless",leg="hedge",outcome="filled"} 3
arbitrage_order_outcomes_total{venue="limitless",leg="hedge",outcome="timeout"} 1
polymarket_ws_queue_depth_frames 27
polymarket_ws_queue_high_watermark_frames 223
polymarket_ws_paused_sockets 1
polymarket_ws_active_sockets 5
polymarket_ws_expected_sockets 6
market_feed_active_pumps{venue="POLYMARKET"} 2
polymarket_ws_connections_total 14
polymarket_ws_frames_received_total 1200
polymarket_books_emitted_total 300
polymarket_ws_message_age_seconds_bucket{condition_id="condition-a",event_type="price_change",le="0.5"} 1
polymarket_ws_message_age_seconds_bucket{condition_id="condition-a",event_type="price_change",le="2.0"} 2
polymarket_ws_message_age_seconds_bucket{condition_id="condition-a",event_type="price_change",le="+Inf"} 2
polymarket_ws_message_age_seconds_sum{condition_id="condition-a",event_type="price_change"} 1.4
polymarket_ws_message_age_seconds_count{condition_id="condition-a",event_type="price_change"} 2
polymarket_ws_message_age_seconds_bucket{condition_id="condition-a",event_type="book",le="5.0"} 1
polymarket_ws_message_age_seconds_bucket{condition_id="condition-a",event_type="book",le="+Inf"} 1
polymarket_ws_message_age_seconds_sum{condition_id="condition-a",event_type="book"} 4
polymarket_ws_message_age_seconds_count{condition_id="condition-a",event_type="book"} 1
polymarket_ws_message_queue_wait_seconds_bucket{condition_id="condition-a",le="0.1"} 1
polymarket_ws_message_queue_wait_seconds_bucket{condition_id="condition-a",le="0.5"} 2
polymarket_ws_message_queue_wait_seconds_bucket{condition_id="condition-a",le="+Inf"} 2
polymarket_ws_message_queue_wait_seconds_sum{condition_id="condition-a"} 0.3
polymarket_ws_message_queue_wait_seconds_count{condition_id="condition-a"} 2
polymarket_ws_venue_timestamp_delta_seconds{event_type="price_change"} -0.185
polymarket_ws_dequeue_to_emit_seconds_bucket{le="0.01"} 2
polymarket_ws_dequeue_to_emit_seconds_bucket{le="+Inf"} 2
polymarket_ws_dequeue_to_emit_seconds_sum 0.01
polymarket_ws_dequeue_to_emit_seconds_count 2
polymarket_ws_processing_seconds_bucket{condition_id="condition-a",stage="process_frame_total",le="0.0005"} 2
polymarket_ws_processing_seconds_bucket{condition_id="condition-a",stage="process_frame_total",le="+Inf"} 2
polymarket_ws_processing_seconds_sum{condition_id="condition-a",stage="process_frame_total"} 0.0004
polymarket_ws_processing_seconds_count{condition_id="condition-a",stage="process_frame_total"} 2
market_feed_receive_to_sink_seconds_bucket{venue="POLYMARKET",le="0.025"} 2
market_feed_receive_to_sink_seconds_bucket{venue="POLYMARKET",le="+Inf"} 2
market_feed_receive_to_sink_seconds_sum{venue="POLYMARKET"} 0.03
market_feed_receive_to_sink_seconds_count{venue="POLYMARKET"} 2
market_feed_sink_publish_seconds_bucket{venue="POLYMARKET",le="0.005"} 2
market_feed_sink_publish_seconds_bucket{venue="POLYMARKET",le="+Inf"} 2
market_feed_sink_publish_seconds_sum{venue="POLYMARKET"} 0.006
market_feed_sink_publish_seconds_count{venue="POLYMARKET"} 2
market_feed_venue_timestamp_delta_seconds{venue="POLYMARKET"} 0.27
market_feed_venue_age_seconds_bucket{venue="POLYMARKET",le="0.1"} 1
market_feed_venue_age_seconds_bucket{venue="POLYMARKET",le="0.5"} 2
market_feed_venue_age_seconds_bucket{venue="POLYMARKET",le="+Inf"} 2
market_feed_venue_age_seconds_sum{venue="POLYMARKET"} 0.4
market_feed_venue_age_seconds_count{venue="POLYMARKET"} 2
market_feed_ws_queue_depth_frames{venue="POLYMARKET"} 27
market_feed_ws_queue_high_watermark_frames{venue="POLYMARKET"} 223
market_feed_ws_paused{venue="POLYMARKET"} 1
market_feed_ws_message_queue_wait_seconds_bucket{venue="POLYMARKET",le="0.025"} 2
market_feed_ws_message_queue_wait_seconds_bucket{venue="POLYMARKET",le="+Inf"} 2
market_feed_ws_message_queue_wait_seconds_sum{venue="POLYMARKET"} 0.03
market_feed_ws_message_queue_wait_seconds_count{venue="POLYMARKET"} 2
market_feed_receive_to_sink_seconds_bucket{venue="LIMITLESS",le="0.05"} 1
market_feed_receive_to_sink_seconds_bucket{venue="LIMITLESS",le="+Inf"} 1
market_feed_receive_to_sink_seconds_sum{venue="LIMITLESS"} 0.04
market_feed_receive_to_sink_seconds_count{venue="LIMITLESS"} 1
market_feed_sink_publish_seconds_bucket{venue="LIMITLESS",le="0.01"} 1
market_feed_sink_publish_seconds_bucket{venue="LIMITLESS",le="+Inf"} 1
market_feed_sink_publish_seconds_sum{venue="LIMITLESS"} 0.008
market_feed_sink_publish_seconds_count{venue="LIMITLESS"} 1
market_feed_venue_timestamp_delta_seconds{venue="LIMITLESS"} 0.31
market_feed_venue_age_seconds_bucket{venue="LIMITLESS",le="0.25"} 1
market_feed_venue_age_seconds_bucket{venue="LIMITLESS",le="1"} 2
market_feed_venue_age_seconds_bucket{venue="LIMITLESS",le="+Inf"} 2
market_feed_venue_age_seconds_sum{venue="LIMITLESS"} 0.8
market_feed_venue_age_seconds_count{venue="LIMITLESS"} 2
market_feed_ws_queue_depth_frames{venue="LIMITLESS"} 4
market_feed_ws_queue_high_watermark_frames{venue="LIMITLESS"} 12
market_feed_ws_paused{venue="LIMITLESS"} 0
market_feed_ws_message_queue_wait_seconds_bucket{venue="LIMITLESS",le="0.01"} 1
market_feed_ws_message_queue_wait_seconds_bucket{venue="LIMITLESS",le="0.1"} 2
market_feed_ws_message_queue_wait_seconds_bucket{venue="LIMITLESS",le="+Inf"} 2
market_feed_ws_message_queue_wait_seconds_sum{venue="LIMITLESS"} 0.08
market_feed_ws_message_queue_wait_seconds_count{venue="LIMITLESS"} 2
trading_event_loop_lag_seconds_bucket{le="0.1"} 2
trading_event_loop_lag_seconds_bucket{le="+Inf"} 2
trading_event_loop_lag_seconds_sum 0.08
trading_event_loop_lag_seconds_count 2
polymarket_ws_resyncs_total{reason="message_age"} 3
polymarket_ws_resyncs_total{reason="queue_depth"} 2
polymarket_ws_resyncs_total{reason="desync"} 7
polymarket_ws_queue_overloads_total{outcome="drained"} 4
polymarket_ws_queue_overloads_total{outcome="restarted"} 1`

    const result = pipelineMetrics(source)
    expect(result.cycleP95).toBeCloseTo(0.095)
    expect(result.bothSubmitP95).toBeCloseTo(0.095)
    expect(result.submitSkewP95).toBeCloseTo(0.095)
    expect(result.venues[0]?.bookAgeP50).toBe(0.25)
    expect(result.venues[0]?.bookAgeP95).toBeCloseTo(0.475)
    expect(result.venues[0]?.bookAgeP99).toBeCloseTo(0.495)
    expect(result.failures[0]?.reason).toBe("timeout")
    expect(result.polymarketWs.queueDepth).toBe(27)
    expect(result.polymarketWs.queueHighWatermark).toBe(223)
    expect(result.polymarketWs.pausedSockets).toBe(1)
    expect(result.polymarketWs.messageAgeP95).toBeCloseTo(1.85)
    expect(result.polymarketWs.messageAgeP99).toBeCloseTo(1.97)
    expect(result.polymarketWs.priceChangeTimestampDelta).toBe(-0.185)
    expect(result.polymarketWs.queueWaitP95).toBeCloseTo(0.46)
    expect(result.polymarketWs.queueWaitP99).toBeCloseTo(0.492)
    expect(result.polymarketWs.queueWaitConditionId).toBe("condition-a")
    expect(result.polymarketWs.dequeueToEmitP95).toBeCloseTo(0.0095)
    expect(result.polymarketWs.dequeueToEmitP99).toBeCloseTo(0.0099)
    expect(result.polymarketWs.resyncs).toBe(12)
    expect(result.polymarketWs.desyncs).toBe(7)
    expect(result.polymarketWs.messageAgeResyncs).toBe(3)
    expect(result.polymarketWs.queueDepthResyncs).toBe(2)
    expect(result.polymarketWs.queueOverloadsDrained).toBe(4)
    expect(result.polymarketWs.queueOverloadsRestarted).toBe(1)
    expect(result.polymarketWs.activeSockets).toBe(5)
    expect(result.polymarketWs.expectedSockets).toBe(6)
    expect(result.polymarketWs.activePumps).toBe(2)
    expect(result.polymarketWs.connections).toBe(14)
    expect(result.polymarketWs.framesReceived).toBe(1200)
    expect(result.polymarketWs.booksEmitted).toBe(300)
    expect(result.eventLoopLagP95).toBeCloseTo(0.095)
    expect(result.marketFeeds).toEqual([
      {
        venue: "Limitless",
        receiveToSinkP95: 0.0475,
        sinkPublishP95: 0.0095,
        venueTimestampDelta: 0.31,
        venueAgeP50: 0.25,
        venueAgeP95: 0.9249999999999999,
        venueAgeP99: 0.985,
        wsQueueDepth: 4,
        wsQueueHighWatermark: 12,
        wsPaused: false,
        wsQueueWaitP95: 0.091,
      },
      {
        venue: "Polymarket",
        receiveToSinkP95: 0.02375,
        sinkPublishP95: 0.00475,
        venueTimestampDelta: 0.27,
        venueAgeP50: 0.1,
        venueAgeP95: 0.45999999999999996,
        venueAgeP99: 0.492,
        wsQueueDepth: 27,
        wsQueueHighWatermark: 223,
        wsPaused: true,
        wsQueueWaitP95: 0.02375,
      },
    ])
  })

  test("builds per-partition worker and bounded IPC diagnostics", () => {
    const source = `
market_worker_up{partition="btc-5m"} 1
market_worker_heartbeat_age_seconds{partition="btc-5m"} 0.25
market_worker_pid{partition="btc-5m"} 123
market_worker_generation{partition="btc-5m"} 2
market_worker_starts_total{partition="btc-5m"} 2
market_worker_restarts_total{partition="btc-5m"} 1
market_worker_unexpected_exits_total{partition="btc-5m",exit_code="1"} 1
market_worker_cpu_seconds{partition="btc-5m"} 7.5
market_worker_memory_bytes{partition="btc-5m"} 104857600
market_worker_ipc_queue_depth 3
market_worker_ipc_queue_capacity 1024
market_worker_ipc_queue_high_watermark 19
market_worker_intents_rejected_total{partition="btc-5m",reason="stale_source"} 2
market_worker_shadow_detections_total{partition="btc-5m",outcome="agree"} 4
market_worker_ws_queue_depth_frames{partition="btc-5m",venue="POLYMARKET"} 8
market_worker_ws_queue_high_watermark_frames{partition="btc-5m",venue="POLYMARKET"} 44
market_worker_ws_paused_streams{partition="btc-5m",venue="POLYMARKET"} 1
market_worker_timing_seconds{partition="btc-5m",venue="POLYMARKET",stage="source_to_transport_max"} 0.22
market_worker_timing_seconds{partition="btc-5m",venue="POLYMARKET",stage="transport_to_sink_max"} 0.04
market_worker_event_loop_lag_seconds_bucket{partition="btc-5m",le="0.1"} 1
market_worker_event_loop_lag_seconds_bucket{partition="btc-5m",le="+Inf"} 1
market_worker_event_loop_lag_seconds_sum{partition="btc-5m"} 0.05
market_worker_event_loop_lag_seconds_count{partition="btc-5m"} 1
market_worker_intent_ipc_seconds_bucket{partition="btc-5m",le="0.025"} 1
market_worker_intent_ipc_seconds_bucket{partition="btc-5m",le="+Inf"} 1
market_worker_intent_ipc_seconds_sum{partition="btc-5m"} 0.01
market_worker_intent_ipc_seconds_count{partition="btc-5m"} 1
market_worker_book_age_seconds_bucket{partition="btc-5m",venue="POLYMARKET",stage="submission",clock="source",le="0.5"} 1
market_worker_book_age_seconds_bucket{partition="btc-5m",venue="POLYMARKET",stage="submission",clock="source",le="+Inf"} 1
market_worker_book_age_seconds_sum{partition="btc-5m",venue="POLYMARKET",stage="submission",clock="source"} 0.2
market_worker_book_age_seconds_count{partition="btc-5m",venue="POLYMARKET",stage="submission",clock="source"} 1
market_worker_ws_queue_wait_max_seconds{partition="btc-5m",venue="POLYMARKET"} 0.05`

    const result = pipelineMetrics(source)
    expect(result.workerIpc).toEqual({
      depth: 3,
      capacity: 1024,
      highWatermark: 19,
    })
    expect(result.marketWorkers[0]).toMatchObject({
      partition: "btc-5m",
      alive: true,
      heartbeatAge: 0.25,
      pid: 123,
      generation: 2,
      restarts: 1,
      unexpectedExits: 1,
      rejectedIntents: 2,
      shadowAgreements: 4,
      memoryBytes: 104857600,
    })
    expect(result.marketWorkers[0]?.venues[0]).toMatchObject({
      venue: "Polymarket",
      queueDepth: 8,
      queueHighWatermark: 44,
      pausedStreams: 1,
      queueWaitMax: 0.05,
      sourceToTransportMax: 0.22,
      transportToSinkMax: 0.04,
    })
  })

  test("keeps absent capture and validation telemetry unknown", () => {
    const result = pipelineMetrics('market_worker_up{partition="crypto-slow"} 1')
    expect(result.parentCapture).toEqual({
      state: "unknown", queueDepth: null, queueCapacity: null, droppedSamples: null,
      offersAfterStop: null, bytes: null, activeWindows: null, writerAlive: null,
      writerProgressAge: null,
    })
    expect(result.marketWorkers[0]?.capture).toEqual(result.parentCapture)
    expect(result.marketWorkers[0]?.validationP95).toBeNull()
    expect(result.marketWorkers[0]?.validationOutcomes).toBeNull()
  })

  test("ignores configured partitions when multiprocessing is disabled", () => {
    const result = pipelineMetrics(
      'market_worker_validation_queue_depth{partition="crypto-slow"} 0',
    )

    expect(result.marketWorkers).toEqual([])
  })

  test("parses capture exhaustion, loss, pressure and final validation outcomes", () => {
    const result = pipelineMetrics(`
market_worker_up{partition="crypto-slow"} 1
predict_fill_capture_state{producer="parent",state="disabled"} 1
predict_fill_capture_state{producer="crypto-slow",state="running"} 0
predict_fill_capture_state{producer="crypto-slow",state="disk_limit"} 1
predict_fill_capture_queue_depth{producer="crypto-slow"} 4
predict_fill_capture_queue_capacity{producer="crypto-slow"} 512
predict_fill_capture_dropped_samples{producer="crypto-slow"} 7
predict_fill_capture_offers_after_stop{producer="crypto-slow"} 9
predict_fill_capture_bytes{producer="crypto-slow"} 33554432
predict_fill_capture_active_windows{producer="crypto-slow"} 2
predict_fill_capture_writer_alive{producer="crypto-slow"} 0
predict_fill_capture_writer_progress_age_seconds{producer="crypto-slow"} 3
market_worker_validation_queue_depth{partition="crypto-slow"} 1
market_worker_validation_queue_capacity{partition="crypto-slow"} 8
market_worker_validation_results_total{partition="crypto-slow",outcome="accepted"} 3
market_worker_validation_results_total{partition="crypto-slow",outcome="timeout"} 1
market_worker_validation_round_trip_seconds_bucket{partition="crypto-slow",le="0.1"} 4
market_worker_validation_round_trip_seconds_bucket{partition="crypto-slow",le="+Inf"} 4
market_worker_validation_round_trip_seconds_count{partition="crypto-slow"} 4
market_worker_validation_round_trip_seconds_sum{partition="crypto-slow"} 0.2
`)
    expect(result.parentCapture.state).toBe("disabled")
    expect(result.marketWorkers[0]?.capture).toEqual({
      state: "disk_limit", queueDepth: 4, queueCapacity: 512, droppedSamples: 7,
      offersAfterStop: 9, bytes: 33554432, activeWindows: 2, writerAlive: false,
      writerProgressAge: 3,
    })
    expect(result.marketWorkers[0]?.validationP95).toBeCloseTo(0.095)
    expect(result.marketWorkers[0]?.validationQueueDepth).toBe(1)
    expect(result.marketWorkers[0]?.validationQueueCapacity).toBe(8)
    expect(result.marketWorkers[0]?.validationOutcomes).toEqual([
      { outcome: "accepted", count: 3 }, { outcome: "timeout", count: 1 },
    ])
  })

  test("does not report healthy capture from conflicting or invalid metrics", () => {
    const capture = pipelineMetrics(`
predict_fill_capture_state{producer="parent",state="running"} 1
predict_fill_capture_state{producer="parent",state="io_error"} 1
predict_fill_capture_bytes{producer="parent"} NaN
predict_fill_capture_writer_alive{producer="parent"} NaN
`).parentCapture
    expect(capture.state).toBe("unknown")
    expect(capture.bytes).toBeNull()
    expect(capture.writerAlive).toBeNull()
  })

  test("derives one interval distribution without averaging percentiles", () => {
    const previous = `
order_operation_seconds_bucket{venue="limitless",operation="submit",le="0.1"} 5
order_operation_seconds_bucket{venue="limitless",operation="submit",le="0.5"} 9
order_operation_seconds_bucket{venue="limitless",operation="submit",le="+Inf"} 10
order_operation_seconds_sum{venue="limitless",operation="submit"} 1.4
order_operation_seconds_count{venue="limitless",operation="submit"} 10
polymarket_ws_frames_received_total 100
polymarket_books_emitted_total 40`
    const current = `
order_operation_seconds_bucket{venue="limitless",operation="submit",le="0.1"} 5
order_operation_seconds_bucket{venue="limitless",operation="submit",le="0.5"} 11
order_operation_seconds_bucket{venue="limitless",operation="submit",le="+Inf"} 12
order_operation_seconds_sum{venue="limitless",operation="submit"} 2.2
order_operation_seconds_count{venue="limitless",operation="submit"} 12
polymarket_ws_frames_received_total 130
polymarket_books_emitted_total 50`

    const [summary] = latencySummaries(current, previous)

    expect(summary?.count).toBe(2)
    expect(summary?.p50).toBeCloseTo(0.3)
    expect(summary?.maximumLowerBound).toBe(0.1)
    expect(summary?.maximumUpperBound).toBe(0.5)
    expect(pipelineMetrics(current, previous, 10).polymarketWs).toMatchObject({
      framesPerSecond: 3,
      booksPerSecond: 1,
    })
  })
})
