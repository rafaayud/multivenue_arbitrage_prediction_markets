import { type FormEvent, useState } from "react"
import { KeyRound, LoaderCircle, LockKeyhole } from "lucide-react"

import { PageHeader } from "@/components/layout/page-header"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { useExecutionActivity } from "@/features/execution/execution-activity-provider"
import { API_BASE_URL, WS_BASE_URL } from "@/lib/config"

/** Explain the backend-owned configuration visible to dashboard users. */
export function SettingsPage() {
  const activity = useExecutionActivity()
  const [tradingKey, setTradingKey] = useState("")
  const [authState, setAuthState] = useState<
    { status: "loading" | "success" | "error"; message: string } | undefined
  >()

  async function authenticate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const key = tradingKey.trim()
    if (!key) return

    setAuthState({ status: "loading", message: "Connecting session..." })
    try {
      await activity.authenticate(key)
      setTradingKey("")
      setAuthState({
        status: "success",
        message: "Session connected. Market monitoring is now available.",
      })
    } catch (reason) {
      setAuthState({
        status: "error",
        message:
          reason instanceof Error ? reason.message : "Authentication failed",
      })
    }
  }

  return (
    <>
      <PageHeader
        eyebrow="Workspace"
        title="Settings"
        description="Connect this browser session to the protected monitoring API."
      />
      <div className="grid items-start gap-5 xl:grid-cols-[minmax(0,1.4fr)_minmax(280px,1fr)]">
        <Card>
          <CardHeader>
            <CardTitle>Connect your browser</CardTitle>
            <CardDescription>
              The key is exchanged once for an HttpOnly session cookie and is
              not persisted by the dashboard.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex items-start gap-3 rounded-lg border border-amber-400/20 bg-amber-400/8 p-4">
              <LockKeyhole className="mt-0.5 size-4 text-amber-300" />
              <div>
                <div className="flex items-center gap-2">
                  <p className="text-xs font-medium">Protected API session</p>
                  <Badge
                    variant={
                      authState?.status === "success" ||
                      activity.connectionStatus === "connected"
                        ? "positive"
                        : "warning"
                    }
                  >
                    {authState?.status === "success" ||
                    activity.connectionStatus === "connected"
                      ? "Connected"
                      : "Required"}
                  </Badge>
                </div>
                <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
                  Authenticate to access monitoring and execution reports.
                  Starting live trading is a separate action.
                </p>
              </div>
            </div>
            <form
              className="space-y-3 rounded-lg border border-border/70 p-4"
              onSubmit={authenticate}
            >
              <label
                className="block text-xs font-medium"
                htmlFor="trading-api-key"
              >
                Trading API key
              </label>
              <div className="flex flex-col gap-2 sm:flex-row">
                <div className="relative flex-1">
                  <KeyRound className="absolute left-3 top-2.5 size-4 text-muted-foreground" />
                  <input
                    id="trading-api-key"
                    type="password"
                    autoComplete="current-password"
                    value={tradingKey}
                    onChange={(event) => setTradingKey(event.target.value)}
                    className="h-9 w-full rounded-md border border-border bg-background pl-9 pr-3 text-sm outline-none transition focus:border-primary"
                    placeholder="Enter TRADING_API_KEY"
                  />
                </div>
                <Button
                  type="submit"
                  disabled={
                    !tradingKey.trim() || authState?.status === "loading"
                  }
                >
                  {authState?.status === "loading" && (
                    <LoaderCircle className="mr-2 size-4 animate-spin" />
                  )}
                  Connect
                </Button>
              </div>
              {authState && (
                <p
                  role={authState.status === "error" ? "alert" : "status"}
                  className={
                    authState.status === "error"
                      ? "text-xs text-rose-300"
                      : "text-xs text-emerald-300"
                  }
                >
                  {authState.message}
                </p>
              )}
            </form>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>How configuration works</CardTitle>
            <CardDescription>
              The server and your browser have separate responsibilities.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-5 text-sm">
            <div>
              <p className="font-medium">1. Credentials stay on the server</p>
              <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
                Venue credentials are loaded from the server environment. The
                dashboard never needs your wallet private keys.
              </p>
            </div>
            <div>
              <p className="font-medium">2. Authorize this browser</p>
              <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
                Enter the server’s TRADING_API_KEY here. Having a .env file
                configures the server; it does not sign your browser in.
              </p>
            </div>
            <div>
              <p className="font-medium">3. Choose what to monitor</p>
              <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
                Open the event catalog and connect a supported market group.
                Check Pipeline for data flow and Latency for timings.
              </p>
            </div>
            <details className="border-t border-border pt-4 text-xs text-muted-foreground">
              <summary className="cursor-pointer">Connection details</summary>
              <dl className="mt-3 space-y-3">
                <div>
                  <dt>API base</dt>
                  <dd className="mt-1 break-all font-mono">{API_BASE_URL}</dd>
                </div>
                <div>
                  <dt>WebSocket base</dt>
                  <dd className="mt-1 break-all font-mono">{WS_BASE_URL}</dd>
                </div>
              </dl>
            </details>
          </CardContent>
        </Card>
      </div>
    </>
  )
}
