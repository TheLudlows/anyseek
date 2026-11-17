import { createHash, randomBytes } from "node:crypto";
import { defineConfig } from "@playwright/test";
process.env.ANYSEEK_TEST_SECRET ??= randomBytes(32).toString("hex");
const serviceKey = createHash("sha256").update(process.env.ANYSEEK_TEST_SECRET).digest("base64");
export default defineConfig({
  testDir: "./tests/browser",
  fullyParallel: false,
  workers: 1,
  timeout: 60000,
  expect: { timeout: 10000 },
  use: {
    baseURL: "http://127.0.0.1:4173",
    viewport: { width: 1440, height: 1000 },
    trace: "retain-on-failure",
    launchOptions: process.env.CHROMIUM_PATH
      ? { executablePath: process.env.CHROMIUM_PATH }
      : {},
  },
  globalSetup: "./tests/service-setup.ts",
  webServer: {
    command: "npm run dev -- --port 4173 --strictPort",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: false,
    env: { API_TARGET: "http://127.0.0.1:18180", ANYSEEK_SERVICE_KEY: serviceKey },
  },
});
