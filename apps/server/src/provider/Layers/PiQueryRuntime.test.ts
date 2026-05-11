import assert from "node:assert/strict";

import { describe, it } from "@effect/vitest";

import { PiQueryRuntime } from "./PiQueryRuntime.ts";

describe("PiQueryRuntime", () => {
  it("starts RPC sessions with a fresh Pi session", () => {
    const runtime = new PiQueryRuntime({
      provider: "minimax",
      model: "MiniMax-M2.7-highspeed",
    });

    const args = (runtime as any).buildArgs() as string[];

    assert.deepEqual(args, [
      "--no-session",
      "--model",
      "MiniMax-M2.7-highspeed",
      "--provider",
      "minimax",
      "--mode",
      "rpc",
    ]);
  });
});
