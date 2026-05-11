import assert from "node:assert/strict";
import * as os from "node:os";
import * as path from "node:path";
import { chmod, mkdtemp, writeFile } from "node:fs/promises";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { Effect, Fiber, Layer, Stream } from "effect";

import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";

import { ServerSettingsService } from "../../serverSettings.ts";
import { PiAdapter } from "../Services/PiAdapter.ts";
import { makePiAdapterLive } from "./PiAdapter.ts";

async function makeMockPiWrapper(options?: { readonly keepAliveAfterTurn?: boolean }) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "pi-rpc-mock-"));
  const wrapperPath = path.join(dir, "fake-pi.js");
  const keepAliveAfterTurn = options?.keepAliveAfterTurn === true;
  const script = `#!/usr/bin/env node
process.stdin.setEncoding("utf8");

let buffer = "";
const keepAliveAfterTurn = ${JSON.stringify(keepAliveAfterTurn)};
let thinkingLevel = "medium";

function emit(event) {
  process.stdout.write(JSON.stringify(event) + "\\n");
}

process.on("SIGTERM", () => process.exit(0));

process.stdin.on("data", (chunk) => {
  buffer += chunk;

  let newlineIndex = buffer.indexOf("\\n");
  while (newlineIndex >= 0) {
    const line = buffer.slice(0, newlineIndex).trim();
    buffer = buffer.slice(newlineIndex + 1);
    if (line.length > 0) {
      const message = JSON.parse(line);
      if (message.type === "prompt") {
        emit({ type: "response", command: "prompt", success: true, sessionId: "mock-session" });
        if (String(message.message).includes("tool")) {
          emit({
            type: "turn_end",
            sessionId: "mock-session",
            message: {
              role: "assistant",
              content: [
                {
                  type: "toolCall",
                  id: "call_mock_ls",
                  name: "bash",
                  arguments: { command: "ls" },
                },
              ],
            },
            toolResults: [
              {
                toolCallId: "call_mock_ls",
                content: [{ type: "text", text: "AGENTS.md\\nCLAUDE.md\\nCONTRIBUTING.md\\n" }],
                isError: false,
              },
            ],
          });
        } else {
          emit({
            type: "message_update",
            sessionId: "mock-session",
            assistantMessageEvent: {
              type: "text_delta",
              contentIndex: 0,
              delta: "hello from fake pi (" + thinkingLevel + ")",
            },
          });
        }
        emit({ type: "agent_end", sessionId: "mock-session" });
        if (!keepAliveAfterTurn) {
          setTimeout(() => process.exit(0), 10);
        }
      } else if (message.type === "set_thinking_level") {
        thinkingLevel = message.level;
        emit({
          type: "response",
          command: "set_thinking_level",
          success: true,
          sessionId: "mock-session",
        });
      } else if (message.type === "abort") {
        emit({ type: "response", command: "abort", success: true, sessionId: "mock-session" });
        setTimeout(() => process.exit(0), 10);
      }
    }
    newlineIndex = buffer.indexOf("\\n");
  }
});

process.stdin.resume();
`;

  await writeFile(wrapperPath, script, "utf8");
  await chmod(wrapperPath, 0o755);
  return wrapperPath;
}

const PiAdapterTestLayer = makePiAdapterLive().pipe(
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(NodeServices.layer),
);

it.layer(PiAdapterTestLayer)("PiAdapterLive", (it) => {
  it.effect("starts a session and completes a turn from a mock Pi RPC CLI", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("pi-mock-thread");

      const wrapperPath = yield* Effect.promise(() => makeMockPiWrapper());
      yield* settings.updateSettings({
        providers: {
          pi: {
            binaryPath: wrapperPath,
          },
        },
      });

      const runtimeEventsFiber = yield* Stream.take(adapter.streamEvents, 6).pipe(
        Stream.runCollect,
        Effect.forkChild,
      );

      const session = yield* adapter.startSession({
        threadId,
        provider: "pi",
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: {
          instanceId: ProviderInstanceId.make("pi"),
          model: "minimax/MiniMax-M2.7",
        },
      });

      assert.equal(session.provider, "pi");
      assert.equal(session.threadId, threadId);

      yield* adapter.sendTurn({
        threadId,
        input: "hello mock",
        attachments: [],
      });

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const types = new Set(runtimeEvents.map((event) => event.type));

      assert.ok(types.has("session.started"));
      assert.ok(types.has("session.configured"));
      assert.ok(types.has("session.state.changed"));
      assert.ok(types.has("turn.started"));
      assert.ok(types.has("content.delta"));
      assert.ok(types.has("turn.completed"));

      const deltaEvent = runtimeEvents.find((event) => event.type === "content.delta");
      assert.equal(deltaEvent?.payload.delta, "hello from fake pi (medium)");

      const turnCompleted = runtimeEvents.find((event) => event.type === "turn.completed");
      assert.equal(turnCompleted?.payload.state, "completed");
    }),
  );

  it.effect("forwards Pi effort selections into the RPC thinking level", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("pi-mock-thread-thinking");

      const wrapperPath = yield* Effect.promise(() => makeMockPiWrapper());
      yield* settings.updateSettings({
        providers: {
          pi: {
            binaryPath: wrapperPath,
          },
        },
      });

      const runtimeEventsFiber = yield* Stream.take(adapter.streamEvents, 6).pipe(
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: "pi",
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: {
          instanceId: ProviderInstanceId.make("pi"),
          model: "minimax/MiniMax-M2.7",
        },
      });

      yield* adapter.sendTurn({
        threadId,
        input: "hello mock",
        attachments: [],
        modelSelection: {
          instanceId: ProviderInstanceId.make("pi"),
          model: "minimax/MiniMax-M2.7",
          options: {
            effort: "high",
          },
        },
      });

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));
      const deltaEvent = runtimeEvents.find((event) => event.type === "content.delta");

      assert.equal(deltaEvent?.payload.delta, "hello from fake pi (high)");
    }),
  );

  it.effect("projects Pi tool calls from completed turn snapshots into work items", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const settings = yield* ServerSettingsService;
      const threadId = ThreadId.make("pi-mock-thread-tools");

      const wrapperPath = yield* Effect.promise(() => makeMockPiWrapper());
      yield* settings.updateSettings({
        providers: {
          pi: {
            binaryPath: wrapperPath,
          },
        },
      });

      const runtimeEventsFiber = yield* Stream.take(adapter.streamEvents, 10).pipe(
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.startSession({
        threadId,
        provider: "pi",
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: {
          instanceId: ProviderInstanceId.make("pi"),
          model: "minimax/MiniMax-M2.7",
        },
      });

      yield* adapter.sendTurn({
        threadId,
        input: "use tool",
        attachments: [],
      });

      const runtimeEvents = Array.from(yield* Fiber.join(runtimeEventsFiber));

      assert.ok(runtimeEvents.some((event) => event.type === "item.started"));
      assert.ok(runtimeEvents.some((event) => event.type === "item.updated"));
      assert.ok(runtimeEvents.some((event) => event.type === "item.completed"));

      const toolStart = runtimeEvents.find((event) => event.type === "item.started");
      assert.equal(toolStart?.payload.itemType, "command_execution");
      assert.equal(toolStart?.payload.title, "Command");

      const toolOutput = runtimeEvents.find(
        (event) => event.type === "content.delta" && event.payload.streamKind === "command_output",
      );
      assert.equal(toolOutput?.type, "content.delta");
      if (toolOutput?.type !== "content.delta") {
        throw new Error("expected command output delta");
      }
      assert.match(toolOutput.payload.delta, /AGENTS\.md/);

      const toolComplete = runtimeEvents.find((event) => event.type === "item.completed");
      assert.equal(toolComplete?.payload.status, "completed");
    }),
  );
});
