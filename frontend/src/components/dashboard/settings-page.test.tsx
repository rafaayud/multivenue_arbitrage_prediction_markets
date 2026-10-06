import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { describe, expect, test, vi } from "vitest"

import { SettingsPage } from "@/components/dashboard/settings-page"
const session = vi.hoisted(() => ({
  authenticate: vi.fn().mockResolvedValue(undefined),
}))

vi.mock("@/features/execution/execution-activity-provider", () => ({
  useExecutionActivity: () => ({
    authenticate: session.authenticate,
    connectionStatus: "disconnected",
  }),
}))

describe("SettingsPage", () => {
  test("exchanges the trading key for a browser session", async () => {
    const user = userEvent.setup()
    render(<SettingsPage />)

    await user.type(screen.getByLabelText("Trading API key"), "secret")
    await user.click(screen.getByRole("button", { name: "Connect" }))

    expect(session.authenticate).toHaveBeenCalledWith("secret")
    expect(await screen.findByText(/Session connected/i)).toBeVisible()
    expect(screen.getByLabelText("Trading API key")).toHaveValue("")
  })
})
