import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import { describe, expect, it } from "vitest";

import { resolveSelectableProvider } from "./providerModels";

const TEST_PROVIDERS: ReadonlyArray<ServerProvider> = [
  {
    instanceId: ProviderInstanceId.make("opencode"),
    driver: ProviderDriverKind.make("opencode"),
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
    instanceId: ProviderInstanceId.make("pi"),
    driver: ProviderDriverKind.make("pi"),
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
    const result = resolveSelectableProvider(TEST_PROVIDERS, ProviderInstanceId.make("pi"));
    expect(result).toBe(ProviderDriverKind.make("pi"));
  });

  it("falls back to an enabled provider when the requested provider is missing", () => {
    const result = resolveSelectableProvider(TEST_PROVIDERS, ProviderInstanceId.make("codex"));
    expect(result).toBe(ProviderDriverKind.make("opencode"));
  });
});
