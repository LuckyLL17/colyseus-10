import { defineConfig } from "vitest/config";
import path from "node:path";

// Local test runner config: resolve workspace packages to their TS sources.
// The package.json `exports` maps gate those behind an `@source` condition
// that TS understands but Vite does not — alias them here instead.
export default defineConfig({
  resolve: {
    alias: [
      { find: "@colyseus/shared-types", replacement: path.resolve(__dirname, "../shared-types/src/index.ts") },
      { find: "@colyseus/core", replacement: path.resolve(__dirname, "../core/src/index.ts") },
    ],
  },
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
  },
});
