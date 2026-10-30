import { defineConfig } from "vite";

// Only the development/preview server sees API_TARGET; credentials stay in headers.
const proxy = {
  "/api": {
    target: process.env.API_TARGET || "http://127.0.0.1:8080",
    changeOrigin: true,
  },
};
export default defineConfig({ server: { proxy }, preview: { proxy } });
