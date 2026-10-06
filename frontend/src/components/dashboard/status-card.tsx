import type { LucideIcon } from "lucide-react"

import { Card, CardContent } from "@/components/ui/card"
import { cn } from "@/lib/utils"

/** Summarize one backend status value in a compact dashboard card. */
export function StatusCard({
  label,
  value,
  detail,
  icon: Icon,
  tone = "neutral",
}: {
  label: string
  value: string
  detail: string
  icon: LucideIcon
  tone?: "positive" | "negative" | "warning" | "neutral"
}) {
  const color = {
    positive: "text-emerald-700 dark:text-emerald-300 bg-emerald-400/10 border-emerald-400/15",
    negative: "text-rose-700 dark:text-rose-300 bg-rose-400/10 border-rose-400/15",
    warning: "text-amber-700 dark:text-amber-300 bg-amber-400/10 border-amber-400/15",
    neutral: "text-muted-foreground bg-muted/70 border-border/70",
  }[tone]

  return (
    <Card>
      <CardContent className="flex items-start justify-between gap-3 p-4 sm:p-5">
        <div className="min-w-0">
          <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
            {label}
          </p>
          <p className="numeric mt-3 break-words text-2xl font-semibold tracking-tight">
            {value}
          </p>
          <p className="mt-2 text-xs leading-relaxed text-muted-foreground">{detail}</p>
        </div>
        <div className={cn("shrink-0 rounded-lg border p-2", color)}>
          <Icon className="size-4" />
        </div>
      </CardContent>
    </Card>
  )
}
