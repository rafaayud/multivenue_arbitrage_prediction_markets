import { afterEach, beforeEach, expect, test, vi } from "vitest"

import { readTradingPreferences, saveTradingPreferences } from "./trading-preferences"

beforeEach(() => localStorage.clear())
afterEach(() => vi.restoreAllMocks())

test("merges limits and selections without storing consent or credentials", () => {
  saveTradingPreferences({ max_arbitrages: 3, allow_degraded_venues: true })
  saveTradingPreferences({ existing_inventory_pairs: [["poly-no", "predict-yes"]] })
  expect(readTradingPreferences()).toEqual({
    max_arbitrages: 3, existing_inventory_pairs: [["poly-no", "predict-yes"]],
  })
  expect(JSON.parse(localStorage.getItem("pm-trading-preferences-v1")!)).toEqual(readTradingPreferences())
})

test("ignores malformed or unrelated stored values", () => {
  localStorage.setItem("pm-trading-preferences-v1", "{broken")
  expect(readTradingPreferences()).toEqual({})
  localStorage.setItem("pm-trading-preferences-v1", JSON.stringify({
    trading_key: "secret", live: true, confirmation: "LIVE", max_arbitrages: "invalid",
    short_market_keys: [null, "regular:fed"], existing_inventory_pairs: [[null, 2], ["poly", "predict"]],
  }))
  expect(readTradingPreferences()).toEqual({
    short_market_keys: ["regular:fed"], existing_inventory_pairs: [["poly", "predict"]],
  })
})

test("keeps controls usable when browser storage is blocked", () => {
  vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => { throw new Error("Blocked") })
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => { throw new Error("Blocked") })
  expect(readTradingPreferences()).toEqual({})
  expect(() => saveTradingPreferences({ max_arbitrages: 3 })).not.toThrow()
})
