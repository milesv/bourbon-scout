import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      include: ["scraper.js", "lib/**/*.js"],
      exclude: ["debug-locators.js", "node_modules/**"],
    },
  },
});
