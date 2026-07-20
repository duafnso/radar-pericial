import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["frontend/tests/setup.ts"],
    include: ["frontend/tests/**/*.behavior.test.{ts,tsx}"],
    css: true,
    restoreMocks: true,
  },
});
