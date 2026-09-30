import path from "node:path"
import { fileURLToPath } from "node:url"

import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import { loadEnv } from "vite"
import { defineConfig } from "vitest/config"

const root = path.dirname(fileURLToPath(import.meta.url))

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, root, "")
  const rewriteApi = (requestPath: string) => requestPath.replace(/^\/api/, "")
  const tradingProxy = {
    target: env.VITE_TRADING_BASE_URL || "http://localhost:8001",
    changeOrigin: true,
    rewrite: rewriteApi,
  }

  return {
    plugins: [react(), tailwindcss()],
    resolve: {
      alias: {
        "@": path.resolve(root, "src"),
      },
    },
    server: {
      proxy: {
        "/api/runtime": tradingProxy,
        "/api/trading-runs": tradingProxy,
        "/api/arbitrage-candidates/monitor": tradingProxy,
        "^/api/execution-journals/[^/]+/complete$": tradingProxy,
        "/api/metrics": tradingProxy,
        "/api": {
          target: env.VITE_API_BASE_URL || "http://localhost:8000",
          changeOrigin: true,
          rewrite: rewriteApi,
        },
        "/ws/arbitrage-signals": {
          target: env.VITE_TRADING_WS_BASE_URL || "ws://localhost:8001",
          ws: true,
        },
        "/ws": {
          target: env.VITE_WS_BASE_URL || "ws://localhost:8000",
          ws: true,
        },
      },
    },
    test: {
      environment: "jsdom",
      setupFiles: "./src/test/setup.ts",
      css: true,
      clearMocks: true,
    },
  }
})
