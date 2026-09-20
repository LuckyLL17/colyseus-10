import { defineConfig } from "vitest/config";
import path from "node:path";

// LOCAL-ONLY test runner config (the workspace installs deps via pnpm, whose
// @source export condition Vite doesn't read). Resolves workspace packages to
// their TS sources / installed builds.
export default defineConfig({
  resolve: {
    alias: [
      { find: "@colyseus/shared-types", replacement: path.resolve(__dirname, "../shared-types/src/index.ts") },
      { find: /^@colyseus\/schema$/, replacement: path.resolve(
        __dirname, "../../my-server/node_modules/@colyseus/schema/build/index.mjs") },
      { find: /^@colyseus\/schema\/(.*)$/, replacement: path.resolve(
        __dirname, "../../my-server/node_modules/@colyseus/schema/build/$1/index.mjs") },
    ],
  },
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
  },
});
