import assert from "node:assert/strict";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { Effect, Layer, Sink, Stream } from "effect";
import * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerSettingsService } from "../../serverSettings.ts";
import { PiProvider } from "../Services/PiProvider.ts";
import { checkPiProviderStatus, PiProviderLive } from "./PiProvider.ts";

const encoder = new TextEncoder();

function mockHandle(result: { stdout: string; stderr: string; code: number }) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(result.code)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: Stream.make(encoder.encode(result.stdout)),
    stderr: Stream.make(encoder.encode(result.stderr)),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

function mockCommandSpawnerLayer(
  handler: (
    command: string,
    args: ReadonlyArray<string>,
  ) => { stdout: string; stderr: string; code: number },
) {
  return Layer.succeed(
    ChildProcessSpawner.ChildProcessSpawner,
    ChildProcessSpawner.make((command) => {
      const cmd = command as unknown as { command: string; args: ReadonlyArray<string> };
      return Effect.succeed(mockHandle(handler(cmd.command, cmd.args)));
    }),
  );
}

function failingSpawnerLayer(description: string) {
  return Layer.succeed(
    ChildProcessSpawner.ChildProcessSpawner,
    ChildProcessSpawner.make(() =>
      Effect.fail(
        PlatformError.systemError({
          _tag: "NotFound",
          module: "ChildProcess",
          method: "spawn",
          description,
        }),
      ),
    ),
  );
}

const makeTestLayer = (
  handler: (
    command: string,
    args: ReadonlyArray<string>,
  ) => { stdout: string; stderr: string; code: number },
  settingsOverrides?: Parameters<typeof ServerSettingsService.layerTest>[0],
) =>
  PiProviderLive.pipe(
    Layer.provideMerge(mockCommandSpawnerLayer(handler)),
    Layer.provideMerge(ServerSettingsService.layerTest(settingsOverrides)),
    Layer.provideMerge(NodeServices.layer),
  );

it.layer(
  makeTestLayer(
    (_command, args) => {
      if (args[0] === "--version") {
        return { stdout: "pi 0.68.0\n", stderr: "", code: 0 };
      }

      return {
        stdout: "",
        stderr:
          "provider  model  context  max-out  thinking  images\n" +
          "minimax  MiniMax-M2.7\n" +
          "minimax  MiniMax-M2.7-highspeed\n",
        code: 0,
      };
    },
    {
      providers: {
        pi: {
          binaryPath: "fake-pi",
        },
      },
    },
  ),
)("PiProviderLive", (it) => {
  it.effect("surfaces Pi RPC commands and skills in the provider snapshot", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus({
        cwd: "/Users/test/project",
        settings: {
          enabled: true,
          binaryPath: "fake-pi",
          customModels: [],
        },
        runCommand: (args) =>
          Effect.succeed(
            args[0] === "--version"
              ? { stdout: "pi 0.68.0\n", stderr: "", code: 0 }
              : {
                  stdout: "",
                  stderr:
                    "provider  model  context  max-out  thinking  images\n" +
                    "minimax  MiniMax-M2.7\n",
                  code: 0,
                },
          ),
        resolveCommandCatalog: () =>
          Effect.succeed({
            slashCommands: [
              { name: "handoff", description: "Cross-provider model handoff" },
              { name: "fix-tests", description: "Fix failing tests" },
              { name: "skill:brave-search", description: "Web search via Brave API" },
            ],
            skills: [
              {
                name: "brave-search",
                description: "Web search via Brave API",
                path: "/Users/test/.pi/agent/skills/brave-search/SKILL.md",
                scope: "user",
                enabled: true,
              },
            ],
          }),
      });

      assert.deepStrictEqual(snapshot.slashCommands, [
        { name: "handoff", description: "Cross-provider model handoff" },
        { name: "fix-tests", description: "Fix failing tests" },
        { name: "skill:brave-search", description: "Web search via Brave API" },
      ]);
      assert.deepStrictEqual(snapshot.skills, [
        {
          name: "brave-search",
          description: "Web search via Brave API",
          path: "/Users/test/.pi/agent/skills/brave-search/SKILL.md",
          scope: "user",
          enabled: true,
        },
      ]);
    }),
  );

  it.effect("reports ready when Pi returns models from stderr", () =>
    Effect.gen(function* () {
      const provider = yield* PiProvider;
      const snapshot = yield* provider.refresh;

      assert.equal(snapshot.provider, "pi");
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.installed, true);
      assert.equal(snapshot.version, "0.68.0");
      assert.equal(snapshot.auth.status, "authenticated");
      assert.deepStrictEqual(
        snapshot.models.map((model) => model.slug),
        ["minimax/MiniMax-M2.7", "minimax/MiniMax-M2.7-highspeed"],
      );
      assert.deepStrictEqual(snapshot.slashCommands, []);
      assert.deepStrictEqual(snapshot.skills, []);
    }),
  );
});

it.layer(
  PiProviderLive.pipe(
    Layer.provideMerge(failingSpawnerLayer("spawn fake-pi ENOENT")),
    Layer.provideMerge(
      ServerSettingsService.layerTest({
        providers: {
          pi: {
            binaryPath: "fake-pi",
          },
        },
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  ),
)("PiProviderLive missing binary", (it) => {
  it.effect("surfaces a missing Pi binary as a provider error", () =>
    Effect.gen(function* () {
      const provider = yield* PiProvider;
      const snapshot = yield* provider.refresh;

      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.installed, false);
      assert.equal(snapshot.message, "Pi CLI (`pi`) is not installed or not on PATH.");
    }),
  );
});
