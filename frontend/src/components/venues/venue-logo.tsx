const assets: Record<string, string> = {
  limitless: "/venues/limitless.svg",
  polymarket: "/venues/polymarket.svg",
  predict: "/venues/predict.png",
}

/** Render a decorative venue logo beside a visible venue label. */
export function VenueLogo({ venue }: { venue: string }) {
  const normalized = venue.toLowerCase()
  const asset = assets[normalized]
  if (asset) {
    return (
      <img
        src={asset}
        alt=""
        aria-hidden="true"
        className="size-5 rounded-md object-cover"
      />
    )
  }

  const color =
    normalized === "polymarket"
      ? "border-indigo-400/25 bg-indigo-400/12 text-indigo-200"
      : normalized === "limitless"
        ? "border-emerald-400/25 bg-emerald-400/12 text-emerald-200"
        : "border-border bg-secondary text-muted-foreground"

  return (
    <span
      aria-hidden="true"
      className={`inline-flex size-5 items-center justify-center rounded-md border text-[10px] font-bold ${color}`}
    >
      {venue.charAt(0).toUpperCase()}
    </span>
  )
}
