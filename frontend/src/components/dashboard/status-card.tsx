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
    positive: "text-emerald-300 bg-emerald-400/10 border-emerald-400/15",
    negative: "text-rose-300 bg-rose-400/10 border-rose-400/15",
    warning: "text-amber-300 bg-amber-400/10 border-amber-400/15",
    neutral: "text-sky-300 bg-sky-400/10 border-sky-400/15",
  }[tone]

  return (
    <Card>
      <CardContent className="flex items-start justify-between p-4 sm:p-5">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
            {label}
          </p>
          <p className="numeric mt-2 text-xl font-semibold tracking-tight">
            {value}
          </p>
          <p className="mt-1 text-[11px] text-muted-foreground">{detail}</p>
        </div>
        <div className={cn("rounded-lg border p-2", color)}>
          <Icon className="size-4" />
        </div>
      </CardContent>
    </Card>
  )
}
