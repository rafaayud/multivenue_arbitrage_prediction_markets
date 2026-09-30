import type { CapacitorConfig } from "@capacitor/cli"

const config: CapacitorConfig = {
  appId: "com.romagooners.predictiondesk",
  appName: "Prediction Desk",
  webDir: "dist",
  backgroundColor: "#090c12",
  plugins: {
    CapacitorCookies: { enabled: true },
    CapacitorHttp: { enabled: true },
  },
}

export default config
