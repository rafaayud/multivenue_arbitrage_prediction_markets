import type { ReactNode } from "react"

/** Render a page title, description, and optional actions. */
export function PageHeader({
  eyebrow,
  title,
  description,
  actions,
}: {
  eyebrow: string
  title: string
  description: string
  actions?: ReactNode
}) {
  return (
    <div className="mb-8 flex flex-col justify-between gap-5 xl:flex-row xl:items-end">
      <div className="min-w-0">
        <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-primary">
          {eyebrow}
        </p>
        <h1 className="text-3xl font-semibold tracking-[-0.045em] sm:text-[34px] sm:leading-tight">
          {title}
        </h1>
        <p className="mt-2 max-w-2xl text-sm leading-relaxed text-muted-foreground">
          {description}
        </p>
      </div>
      {actions}
    </div>
  )
}
