import { createBrowserRouter } from "react-router-dom"

import { DashboardPage } from "@/components/dashboard/dashboard-page"
import { SettingsPage } from "@/components/dashboard/settings-page"
import { CandidatesPage } from "@/components/candidates/candidates-page"
import { AppShell } from "@/components/layout/app-shell"
import { OrdersPage } from "@/components/orders/orders-page"
import { PnlPage } from "@/components/pnl/pnl-page"
import { PipelineMetricsPage } from "@/components/pipeline/pipeline-metrics-page"
import { SignalsPage } from "@/components/signals/signals-page"

export const router = createBrowserRouter([
  {
    element: <AppShell />,
    children: [
      { path: "/", element: <DashboardPage /> },
      { path: "/events", element: <CandidatesPage /> },
      { path: "/signals", element: <SignalsPage /> },
      { path: "/orders", element: <OrdersPage /> },
      { path: "/pnl", element: <PnlPage /> },
      { path: "/pipeline", element: <PipelineMetricsPage /> },
      { path: "/latency", element: <PipelineMetricsPage view="latency" /> },
      { path: "/settings", element: <SettingsPage /> },
    ],
  },
])
