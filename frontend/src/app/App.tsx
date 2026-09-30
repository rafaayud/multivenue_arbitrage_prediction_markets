import { RouterProvider } from "react-router-dom"

import { router } from "@/app/routes"
import { Toaster } from "@/components/ui/sonner"
import { ArbitrageStreamProvider } from "@/features/arbitrage/arbitrage-stream-provider"
import { ExecutionActivityProvider } from "@/features/execution/execution-activity-provider"

/** Render the application router with global providers and notifications. */
export function App() {
  return (
    <ArbitrageStreamProvider>
      <ExecutionActivityProvider>
        <RouterProvider router={router} />
        <Toaster />
      </ExecutionActivityProvider>
    </ArbitrageStreamProvider>
  )
}
