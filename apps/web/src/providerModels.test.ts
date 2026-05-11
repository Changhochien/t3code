import type { ServerProvider } from "@t3tools/contracts";
import { describe, expect, it } from "vitest";

import { resolveSelectableProvider } from "./providerModels";

const TEST_PROVIDERS: ReadonlyArray<ServerProvider> = [
  {
    provider: "opencode",
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-04-22T00:00:00.000Z",
    slashCommands: [],
    skills: [],
    models: [],
  },
  {
    provider: "pi",
    enabled: false,
    installed: true,
    version: "0.68.0",
    status: "error",
    auth: { status: "unauthenticated" },
    checkedAt: "2026-04-22T00:00:00.000Z",
    slashCommands: [],
    skills: [],
    models: [],
  },
];

describe("resolveSelectableProvider", () => {
  it("preserves an explicitly selected provider even when it is disabled", () => {
    expect(resolveSelectableProvider(TEST_PROVIDERS, "pi")).toBe("pi");
  });

  it("falls back to an enabled provider when the requested provider is missing", () => {
    expect(resolveSelectableProvider(TEST_PROVIDERS, "codex")).toBe("opencode");
  });
});
