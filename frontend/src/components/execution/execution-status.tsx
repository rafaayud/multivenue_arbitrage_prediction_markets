import { KeyRound, LoaderCircle } from "lucide-react"
import { type FormEvent, useState } from "react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
} from "@/components/ui/card"
import { useExecutionActivity } from "@/features/execution/execution-activity-provider"
import { formatTimestamp } from "@/lib/formatters"

/** Render a consistent status badge for orders, journals and recoveries. */
export function ExecutionStateBadge({ status }: { status: string }) {
  const normalized = status.toLowerCase()
  const variant =
    normalized === "completed" ||
    normalized === "filled" ||
    normalized === "resolved" ||
    normalized === "long"
      ? "positive"
      : normalized === "failed" ||
          normalized === "rejected" ||
          normalized === "needs_review" ||
          normalized === "short"
        ? "negative"
        : normalized.includes("pending") ||
            normalized === "recovered" ||
            normalized === "partially_filled" ||
            normalized === "stopping"
          ? "warning"
          : "secondary"
  return <Badge variant={variant}>{status.replaceAll("_", " ")}</Badge>
}

/** Show execution-stream freshness and allow safe cookie authentication. */
export function ExecutionFeedStatus() {
  const activity = useExecutionActivity()
  const [tradingKey, setTradingKey] = useState("")
  const [authenticating, setAuthenticating] = useState(false)

  const authenticate = async (event: FormEvent) => {
    event.preventDefault()
    setAuthenticating(true)
    try {
      await activity.authenticate(tradingKey)
      setTradingKey("")
    } catch {
      // The provider exposes the backend error beside the connection status.
    } finally {
      setAuthenticating(false)
    }
  }

  return (
    <Card className="mb-4">
      <CardContent className="flex flex-col gap-3 p-4 xl:flex-row xl:items-center xl:justify-between">
        <div className="flex flex-wrap items-center gap-3">
          <Badge
            variant={
              activity.connectionStatus === "connected"
                ? "positive"
                : activity.connectionStatus === "error"
                  ? "negative"
                  : "warning"
            }
          >
            {activity.connectionStatus}
          </Badge>
          <p className="text-xs text-muted-foreground">
            Execution stream · last snapshot{" "}
            {formatTimestamp(activity.lastReceivedAt)}
          </p>
          {activity.connectionError && (
            <p className="text-xs text-rose-300">
              {activity.connectionError}
            </p>
          )}
        </div>

        {!activity.snapshot && (
          <form
            className="flex w-full gap-2 sm:max-w-md"
            onSubmit={(event) => void authenticate(event)}
          >
            <label className="sr-only" htmlFor="execution-trading-key">
              Trading API key
            </label>
            <input
              id="execution-trading-key"
              type="password"
              autoComplete="off"
              placeholder="Trading API key"
              value={tradingKey}
              onChange={(event) => setTradingKey(event.target.value)}
              className="h-9 min-w-0 flex-1 rounded-md border border-input bg-background px-3 text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring"
            />
            <Button
              type="submit"
              variant="secondary"
              disabled={!tradingKey || authenticating}
            >
              {authenticating ? (
                <LoaderCircle className="size-4 animate-spin" />
              ) : (
                <KeyRound className="size-4" />
              )}
              Authenticate
            </Button>
          </form>
        )}
      </CardContent>
    </Card>
  )
}
