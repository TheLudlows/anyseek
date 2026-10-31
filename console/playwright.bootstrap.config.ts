import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./tests/bootstrap-browser",
  workers: 1,
  use: {
    baseURL: "http://127.0.0.1:4174",
    launchOptions: process.env.CHROMIUM_PATH
      ? { executablePath: process.env.CHROMIUM_PATH }
      : {},
  },
  webServer: {
    command: "npm run dev -- --port 4174 --strictPort",
    url: "http://127.0.0.1:4174",
    reuseExistingServer: false,
  },
});
