import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { describe, expect, test, vi } from "vitest"

import { SettingsPage } from "@/components/dashboard/settings-page"
import { apiClient } from "@/lib/api-client"

vi.mock("@/lib/api-client", () => ({
  apiClient: {
    createTradingSession: vi.fn().mockResolvedValue({ authenticated: true }),
  },
}))

describe("SettingsPage", () => {
  test("exchanges the trading key for a browser session", async () => {
    const user = userEvent.setup()
    render(<SettingsPage />)

    await user.type(screen.getByLabelText("Trading API key"), "secret")
    await user.click(screen.getByRole("button", { name: "Connect" }))

    expect(apiClient.createTradingSession).toHaveBeenCalledWith("secret")
    expect(await screen.findByText(/Session connected/i)).toBeVisible()
    expect(screen.getByLabelText("Trading API key")).toHaveValue("")
  })
})
