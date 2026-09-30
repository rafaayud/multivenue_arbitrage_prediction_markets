import {
  Activity,
  ChartNoAxesCombined,
  LayoutDashboard,
  ListOrdered,
  Moon,
  RadioTower,
  ReceiptText,
  Settings,
  Sun,
  Workflow,
} from "lucide-react"
import { useEffect, useState } from "react"
import { NavLink, Outlet } from "react-router-dom"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

const navigation = [
  { to: "/", label: "Event monitor", mobileLabel: "Monitor", icon: LayoutDashboard },
  { to: "/events", label: "Event catalog", mobileLabel: "Events", icon: ListOrdered },
  { to: "/signals", label: "Opportunities", mobileLabel: "Signals", icon: RadioTower },
  { to: "/orders", label: "Orders", mobileLabel: "Orders", icon: ReceiptText },
  { to: "/pnl", label: "PnL", mobileLabel: "PnL", icon: ChartNoAxesCombined },
  { to: "/pipeline", label: "Pipeline & latency", mobileLabel: "Latency", icon: Workflow },
  { to: "/settings", label: "Settings", mobileLabel: "Settings", icon: Settings },
]

function NavigationLink({
  to,
  label,
  mobileLabel,
  icon: Icon,
  compact = false,
}: (typeof navigation)[number] & { compact?: boolean }) {
  return (
    <NavLink
      to={to}
      end={to === "/"}
      className={({ isActive }) =>
        cn(
          compact
            ? "group flex min-w-16 shrink-0 flex-col items-center gap-1 rounded-lg px-2 py-1.5 text-[10px] font-medium transition-colors"
            : "group flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors",
          isActive
            ? "bg-primary/10 text-primary"
            : "text-muted-foreground hover:bg-white/[0.035] hover:text-foreground",
        )
      }
    >
      <Icon className={compact ? "size-5" : "size-4"} strokeWidth={1.8} />
      <span>{compact ? mobileLabel : label}</span>
    </NavLink>
  )
}

/** Provide the persistent dashboard navigation and routed content shell. */
export function AppShell() {
  const [dark, setDark] = useState(
    () => localStorage.getItem("pm-theme") !== "light",
  )

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark)
    localStorage.setItem("pm-theme", dark ? "dark" : "light")
  }, [dark])

  return (
    <div className="min-h-dvh lg:grid lg:grid-cols-[248px_1fr]">
      <aside className="hidden border-r border-border/70 bg-background/80 px-4 py-5 backdrop-blur-xl lg:sticky lg:top-0 lg:flex lg:h-screen lg:flex-col">
        <div className="flex items-center gap-3 px-2">
          <div className="flex size-9 items-center justify-center rounded-xl border border-primary/20 bg-primary/10 text-primary shadow-[0_0_28px_rgb(53_211_158/0.12)]">
            <Activity className="size-5" />
          </div>
          <div>
            <p className="text-sm font-bold tracking-tight">Policy Arb</p>
            <p className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
              Multi-venue monitor
            </p>
          </div>
        </div>

        <nav className="mt-8 space-y-1">
          {navigation.map((item) => (
            <NavigationLink key={item.to} {...item} />
          ))}
        </nav>

        <div className="mt-auto rounded-xl border border-border/70 bg-card/60 p-3">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium">Observation mode</span>
            <Badge variant="outline">Events only</Badge>
          </div>
          <p className="mt-2 text-[11px] leading-relaxed text-muted-foreground">
            Connect only the policy markets you explicitly select.
          </p>
        </div>
      </aside>

      <div className="min-w-0">
        <header className="mobile-safe-top sticky top-0 z-30 flex min-h-16 items-center justify-between border-b border-border/65 bg-background/85 px-4 backdrop-blur-xl sm:px-6 lg:h-16 lg:px-8">
          <div className="flex items-center gap-3 lg:hidden">
            <div className="flex size-8 items-center justify-center rounded-lg bg-primary/10 text-primary">
              <Activity className="size-4" />
            </div>
            <span className="text-sm font-semibold">Policy Arb</span>
          </div>
          <div className="hidden items-center gap-2 text-xs text-muted-foreground lg:flex">
            <span className="size-1.5 rounded-full bg-primary shadow-[0_0_10px_currentColor]" />
            Policy event workspace
          </div>
          <Button
            aria-label={dark ? "Use light theme" : "Use dark theme"}
            variant="ghost"
            size="icon"
            onClick={() => setDark((value) => !value)}
          >
            {dark ? <Sun className="size-4" /> : <Moon className="size-4" />}
          </Button>
        </header>

        <main className="mobile-safe-content mx-auto w-full max-w-[1600px] p-4 sm:p-6 lg:p-8">
          <Outlet />
        </main>

        <nav
          aria-label="Primary navigation"
          className="mobile-safe-bottom fixed inset-x-0 bottom-0 z-40 border-t border-border/70 bg-background/95 backdrop-blur-xl lg:hidden"
        >
          <div className="scrollbar-thin flex gap-1 overflow-x-auto px-2 py-1.5">
            {navigation.map((item) => (
              <NavigationLink key={item.to} {...item} compact />
            ))}
          </div>
        </nav>
      </div>
    </div>
  )
}
