/**
 * PiProviderLive - Provides Pi provider snapshot and refresh capabilities.
 *
 * @module PiProviderLive
 */
import type {
  ModelCapabilities,
  PiSettings,
  ServerProvider,
  ServerProviderModel,
  ServerProviderSkill,
  ServerProviderSlashCommand,
} from "@t3tools/contracts";
import * as PlatformError from "effect/PlatformError";
import { Cause, Data, Effect, Equal, Layer, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";

import { runProcess } from "../../processRunner.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  buildServerProvider,
  detailFromResult,
  isCommandMissingCause,
  nonEmptyTrimmed,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
} from "../providerSnapshot.ts";
import { PiProvider } from "../Services/PiProvider.ts";
import { ChildProcessSpawner } from "effect/unstable/process";

const PROVIDER = "pi" as const;

const DEFAULT_PI_MODEL_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [
    { value: "low", label: "Low" },
    { value: "medium", label: "Medium", isDefault: true },
    { value: "high", label: "High" },
  ],
  supportsFastMode: false,
  supportsThinkingToggle: false,
  contextWindowOptions: [],
  promptInjectedEffortLevels: [],
};

const DEFAULT_PI_MODEL: ServerProviderModel = {
  slug: "minimax/MiniMax-M2.7",
  name: "MiniMax · MiniMax M2.7",
  isCustom: false,
  capabilities: DEFAULT_PI_MODEL_CAPABILITIES,
};

type PiCommandRunner = (args: ReadonlyArray<string>) => Effect.Effect<
  {
    readonly stdout: string;
    readonly stderr: string;
    readonly code: number;
  },
  Error | PlatformError.PlatformError
>;

interface PiCommandCatalog {
  readonly slashCommands: ReadonlyArray<ServerProviderSlashCommand>;
  readonly skills: ReadonlyArray<ServerProviderSkill>;
}

type PiCommandCatalogResolver = (input: {
  readonly binaryPath: string;
  readonly cwd: string;
}) => Effect.Effect<PiCommandCatalog | undefined, Error>;

const EMPTY_PI_COMMAND_CATALOG: PiCommandCatalog = {
  slashCommands: [],
  skills: [],
};

class PiCommandCatalogProbeError extends Data.TaggedError("PiCommandCatalogProbeError")<{
  readonly cause: unknown;
}> {}

function parsePiListModelsOutput(output: string): ReadonlyArray<ServerProviderModel> {
  const lines = output
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  if (lines.length < 2) {
    return [];
  }

  const models: Array<ServerProviderModel> = [];
  for (const line of lines.slice(1)) {
    const [provider, model] = line.split(/\s+/, 3);
    if (!provider || !model) {
      continue;
    }

    models.push({
      slug: `${provider}/${model}`,
      name: `${provider.charAt(0).toUpperCase()}${provider.slice(1)} · ${model.replace(/[-_]/g, " ")}`,
      isCustom: false,
      capabilities: DEFAULT_PI_MODEL_CAPABILITIES,
    });
  }

  return models;
}

function combineCommandOutput(result: {
  readonly stdout: string;
  readonly stderr: string;
}): string {
  return `${result.stdout}\n${result.stderr}`;
}

function parsePiCommandCatalog(output: string): PiCommandCatalog | undefined {
  const slashCommands: Array<ServerProviderSlashCommand> = [];
  const skills: Array<ServerProviderSkill> = [];
  const seenSlashCommands = new Set<string>();
  const seenSkills = new Set<string>();

  for (const line of output.split("\n")) {
    const trimmedLine = line.trim();
    if (trimmedLine.length === 0) {
      continue;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmedLine);
    } catch {
      continue;
    }

    if (
      typeof parsed !== "object" ||
      parsed === null ||
      (parsed as { type?: unknown }).type !== "response" ||
      (parsed as { command?: unknown }).command !== "get_commands" ||
      (parsed as { success?: unknown }).success !== true
    ) {
      continue;
    }

    const commands = (parsed as { data?: { commands?: ReadonlyArray<unknown> } }).data?.commands;
    if (!Array.isArray(commands)) {
      return EMPTY_PI_COMMAND_CATALOG;
    }

    for (const command of commands) {
      if (typeof command !== "object" || command === null) {
        continue;
      }

      const record = command as {
        name?: unknown;
        description?: unknown;
        source?: unknown;
        sourceInfo?: {
          path?: unknown;
          scope?: unknown;
        };
      };
      const name = typeof record.name === "string" ? nonEmptyTrimmed(record.name) : undefined;
      if (!name) {
        continue;
      }

      const description =
        typeof record.description === "string" ? nonEmptyTrimmed(record.description) : undefined;

      if (!seenSlashCommands.has(name)) {
        seenSlashCommands.add(name);
        slashCommands.push({
          name,
          ...(description ? { description } : {}),
        });
      }

      if (record.source !== "skill") {
        continue;
      }

      const skillName = nonEmptyTrimmed(name.replace(/^skill:/, ""));
      const path =
        typeof record.sourceInfo?.path === "string"
          ? nonEmptyTrimmed(record.sourceInfo.path)
          : undefined;
      const scope =
        typeof record.sourceInfo?.scope === "string"
          ? nonEmptyTrimmed(record.sourceInfo.scope)
          : undefined;
      if (!skillName || !path || seenSkills.has(skillName)) {
        continue;
      }

      seenSkills.add(skillName);
      skills.push({
        name: skillName,
        path,
        enabled: true,
        ...(description ? { description } : {}),
        ...(scope ? { scope } : {}),
      });
    }

    return {
      slashCommands,
      skills,
    };
  }

  return undefined;
}

function getFallbackModels(piSettings: PiSettings): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(
    [DEFAULT_PI_MODEL],
    PROVIDER,
    piSettings.customModels,
    DEFAULT_PI_MODEL_CAPABILITIES,
  );
}

function buildPiProbeFailure(input: {
  readonly settings: PiSettings;
  readonly checkedAt: string;
  readonly version: string | null;
  readonly cause: unknown;
  readonly commands?: PiCommandCatalog;
}): ServerProvider {
  const fallbackModels = getFallbackModels(input.settings);
  const isMissingCommand = input.cause instanceof Error && isCommandMissingCause(input.cause);
  const detail = input.cause instanceof Error ? input.cause.message.trim() : String(input.cause);

  return buildServerProvider({
    provider: PROVIDER,
    enabled: input.settings.enabled,
    checkedAt: input.checkedAt,
    models: fallbackModels,
    ...(input.commands
      ? {
          slashCommands: input.commands.slashCommands,
          skills: input.commands.skills,
        }
      : {}),
    probe: {
      installed: !isMissingCommand,
      version: input.version,
      status: "error",
      auth: { status: "unknown" },
      message: isMissingCommand
        ? "Pi CLI (`pi`) is not installed or not on PATH."
        : detail.length > 0
          ? `Failed to execute Pi CLI health check: ${detail}`
          : "Failed to execute Pi CLI health check.",
    },
  });
}

function runPiCommand(
  binaryPath: string,
  args: ReadonlyArray<string>,
  childProcessSpawner: typeof ChildProcessSpawner.ChildProcessSpawner.Service,
) {
  return spawnAndCollect(
    binaryPath,
    ChildProcess.make(binaryPath, [...args], {
      shell: process.platform === "win32",
    }),
  ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner));
}

const probePiCommandCatalog = Effect.fn("probePiCommandCatalog")(function* (input: {
  readonly binaryPath: string;
  readonly cwd: string;
}) {
  const result = yield* Effect.tryPromise({
    try: () =>
      runProcess(input.binaryPath, ["--mode", "rpc", "--no-session"], {
        cwd: input.cwd,
        stdin: '{"type":"get_commands"}\n',
        timeoutMs: 10_000,
      }),
    catch: (cause) => new PiCommandCatalogProbeError({ cause }),
  });

  return parsePiCommandCatalog(result.stdout);
});

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (input: {
  readonly settings: PiSettings;
  readonly runCommand: PiCommandRunner;
  readonly cwd?: string;
  readonly resolveCommandCatalog?: PiCommandCatalogResolver;
}) {
  const checkedAt = new Date().toISOString();
  const fallbackModels = getFallbackModels(input.settings);

  if (!input.settings.enabled) {
    return buildServerProvider({
      provider: PROVIDER,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi is disabled in T3 Code settings.",
      },
    });
  }

  const versionExit = yield* Effect.exit(input.runCommand(["--version"]));
  if (versionExit._tag === "Failure") {
    return buildPiProbeFailure({
      settings: input.settings,
      checkedAt,
      version: null,
      cause: Cause.squash(versionExit.cause),
    });
  }

  const version =
    parseGenericCliVersion(`${versionExit.value.stdout}\n${versionExit.value.stderr}`) ?? null;

  const commandCatalog =
    input.resolveCommandCatalog && input.cwd
      ? ((yield* input
          .resolveCommandCatalog({
            binaryPath: input.settings.binaryPath,
            cwd: input.cwd,
          })
          .pipe(Effect.orElseSucceed(() => undefined))) ?? EMPTY_PI_COMMAND_CATALOG)
      : EMPTY_PI_COMMAND_CATALOG;

  const listModelsExit = yield* Effect.exit(input.runCommand(["--list-models"]));
  if (listModelsExit._tag === "Failure") {
    return buildPiProbeFailure({
      settings: input.settings,
      checkedAt,
      version,
      cause: Cause.squash(listModelsExit.cause),
      commands: commandCatalog,
    });
  }

  const modelsResult = listModelsExit.value;
  const parsedModels = parsePiListModelsOutput(combineCommandOutput(modelsResult));
  if (parsedModels.length === 0) {
    const combinedOutput = combineCommandOutput(modelsResult).toLowerCase();
    const message = combinedOutput.includes("no models available")
      ? "Pi is installed, but it did not report any available models. Configure API keys or authenticate in Pi, then try again."
      : (detailFromResult(modelsResult) ??
        "Pi is installed, but it did not report any available models.");

    return buildServerProvider({
      provider: PROVIDER,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      slashCommands: commandCatalog.slashCommands,
      skills: commandCatalog.skills,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unauthenticated" },
        message,
      },
    });
  }

  return buildServerProvider({
    provider: PROVIDER,
    enabled: true,
    checkedAt,
    models: providerModelsFromSettings(
      parsedModels,
      PROVIDER,
      input.settings.customModels,
      DEFAULT_PI_MODEL_CAPABILITIES,
    ),
    slashCommands: commandCatalog.slashCommands,
    skills: commandCatalog.skills,
    probe: {
      installed: true,
      version,
      status: "ready",
      auth: { status: "authenticated" },
      message: `Pi is available with ${parsedModels.length} model${parsedModels.length === 1 ? "" : "s"}.`,
    },
  });
});

function buildPendingPiProviderSnapshot(piSettings: PiSettings): ServerProvider {
  const checkedAt = new Date().toISOString();
  const fallbackModels = getFallbackModels(piSettings);

  if (!piSettings.enabled) {
    return buildServerProvider({
      provider: PROVIDER,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi is disabled in T3 Code settings.",
      },
    });
  }

  return buildServerProvider({
    provider: PROVIDER,
    enabled: true,
    checkedAt,
    models: fallbackModels,
    probe: {
      installed: true,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: "Checking Pi availability...",
    },
  });
}

export const PiProviderLive = Layer.effect(
  PiProvider,
  Effect.gen(function* () {
    const serverSettings = yield* ServerSettingsService;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const cwd = process.cwd();
    const getProviderSettings = serverSettings.getSettings.pipe(
      Effect.map((settings) => settings.providers.pi),
    );

    return yield* makeManagedServerProvider<PiSettings>({
      getSettings: getProviderSettings.pipe(Effect.orDie),
      streamSettings: serverSettings.streamChanges.pipe(
        Stream.map((settings) => settings.providers.pi),
      ),
      haveSettingsChanged: (previous, next) => !Equal.equals(previous, next),
      initialSnapshot: buildPendingPiProviderSnapshot,
      checkProvider: getProviderSettings.pipe(
        Effect.flatMap((settings) =>
          checkPiProviderStatus({
            settings,
            cwd,
            runCommand: (args) => runPiCommand(settings.binaryPath, args, childProcessSpawner),
            resolveCommandCatalog: probePiCommandCatalog,
          }),
        ),
        Effect.orDie,
      ),
    });
  }),
);
