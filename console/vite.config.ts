import { defineConfig, type ProxyOptions } from "vite";

// Optional credential injection is server-side and loopback-only. A deployed
// console uses an authenticated gateway; no service secret enters browser JS.
const serviceKey = process.env.ANYSEEK_SERVICE_KEY;
const proxy: Record<string, ProxyOptions> = {
  "/api": {
    target: process.env.API_TARGET || "http://127.0.0.1:8080",
    changeOrigin: true,
    configure(proxy) {
      if (serviceKey) proxy.on("proxyReq", request => request.setHeader("apikey", serviceKey));
    },
  },
};
export default defineConfig({ server: { proxy }, preview: { proxy } });
