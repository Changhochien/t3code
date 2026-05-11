/**
 * PiAdapterLive - Scoped live implementation for the pi provider adapter.
 *
 * Wraps pi's RPC mode behind the generic provider adapter contract and emits
 * canonical runtime events.
 *
 * @module PiAdapterLive
 */
import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import {
  ApprovalRequestId,
  type CanonicalItemType,
  type CanonicalRequestType,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderRuntimeTurnStatus,
  type ProviderSession,
  type ThreadTokenUsageSnapshot,
  type ProviderUserInputAnswers,
  RuntimeItemId,
  ThreadId,
  TurnId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Random from "effect/Random";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { PiAdapter, type PiAdapterShape } from "../Services/PiAdapter.ts";
import { type EventNdjsonLogger } from "./EventNdjsonLogger.ts";
import { PiQueryRuntime } from "./PiQueryRuntime.ts";

const PROVIDER = ProviderDriverKind.make("pi");
const PI_INSTANCE_ID = ProviderInstanceId.make("pi");

interface PiTurnState {
  readonly turnId: TurnId;
  readonly startedAt: string;
  readonly items: Array<unknown>;
  readonly assistantTextBlocks: Map<number, AssistantTextBlockState>;
  readonly assistantTextBlockOrder: Array<AssistantTextBlockState>;
  nextSyntheticAssistantBlockIndex: number;
}

interface AssistantTextBlockState {
  readonly itemId: string;
  readonly blockIndex: number;
  emittedTextDelta: boolean;
  fallbackText: string;
  streamClosed: boolean;
  completionEmitted: boolean;
}

interface PendingApproval {
  readonly requestType: CanonicalRequestType;
  readonly detail?: string;
  readonly suggestions?: ReadonlyArray<unknown>;
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
}

interface PendingUserInput {
  readonly questions: ReadonlyArray<UserInputQuestion>;
  readonly answers: Deferred.Deferred<ProviderUserInputAnswers>;
}

interface ToolInFlight {
  readonly itemId: string;
  readonly itemType: CanonicalItemType;
  readonly toolName: string;
  readonly title: string;
  readonly detail?: string;
  readonly input: Record<string, unknown>;
  readonly partialInputJson: string;
  readonly lastEmittedInputFingerprint?: string;
}

interface PiSessionContext {
  session: ProviderSession;
  readonly promptQueue: Queue.Queue<PromptQueueItem>;
  readonly query: PiQueryRuntime;
  streamFiber: Fiber.Fiber<void, Error> | undefined;
  readonly startedAt: string;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly pendingUserInputs: Map<ApprovalRequestId, PendingUserInput>;
  readonly turns: Array<{
    id: TurnId;
    items: Array<unknown>;
  }>;
  readonly inFlightTools: Map<number, ToolInFlight>;
  turnState: PiTurnState | undefined;
  lastKnownTokenUsage: ThreadTokenUsageSnapshot | undefined;
  lastAssistantUuid: string | undefined;
  lastThreadStartedId: string | undefined;
  stopped: boolean;
}

type PromptQueueItem =
  | {
      readonly type: "message";
      readonly message: string;
    }
  | {
      readonly type: "terminate";
    };

function asRuntimeItemId(value: string): RuntimeItemId {
  return RuntimeItemId.make(value);
}

function classifyToolItemType(toolName: string): CanonicalItemType {
  const normalized = toolName.toLowerCase();
  if (
    normalized.includes("bash") ||
    normalized.includes("command") ||
    normalized.includes("shell")
  ) {
    return "command_execution";
  }
  if (
    normalized.includes("edit") ||
    normalized.includes("write") ||
    normalized.includes("file") ||
    normalized.includes("patch")
  ) {
    return "file_change";
  }
  if (normalized.includes("mcp")) {
    return "mcp_tool_call";
  }
  return "dynamic_tool_call";
}

function titleForTool(itemType: CanonicalItemType): string {
  switch (itemType) {
    case "command_execution":
      return "Command";
    case "file_change":
      return "File Edit";
    case "mcp_tool_call":
      return "MCP Tool";
    default:
      return "Tool";
  }
}

function summarizeToolRequest(toolName: string, input: Record<string, unknown>): string {
  const commandValue = input.command ?? input.cmd;
  const command = typeof commandValue === "string" ? commandValue : undefined;
  if (command && command.trim().length > 0) {
    return `${toolName}: ${command.trim().slice(0, 400)}`;
  }
  return toolName;
}

function toolInputFingerprint(input: Record<string, unknown>): string | undefined {
  try {
    return JSON.stringify(input);
  } catch {
    return undefined;
  }
}

function tryParseJsonRecord(value: string): Record<string, unknown> | undefined {
  try {
    const parsed = JSON.parse(value);
    if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
    return undefined;
  } catch {
    return undefined;
  }
}

function extractTextContent(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map((entry) => extractTextContent(entry)).join("");
  }

  if (!value || typeof value !== "object") {
    return "";
  }

  const record = value as {
    text?: unknown;
    content?: unknown;
  };

  if (typeof record.text === "string") {
    return record.text;
  }

  return extractTextContent(record.content);
}

function toolResultStreamKind(
  itemType: CanonicalItemType,
): "command_output" | "file_change_output" | undefined {
  switch (itemType) {
    case "command_execution":
      return "command_output";
    case "file_change":
      return "file_change_output";
    default:
      return undefined;
  }
}

function toolResultBlocksFromUserMessage(message: SDKMessage): Array<{
  readonly toolUseId: string;
  readonly block: Record<string, unknown>;
  readonly text: string;
  readonly isError: boolean;
}> {
  if (message.type !== "user") {
    return [];
  }

  const content = (message.message as { content?: unknown } | undefined)?.content;
  if (!Array.isArray(content)) {
    return [];
  }

  const blocks: Array<{
    readonly toolUseId: string;
    readonly block: Record<string, unknown>;
    readonly text: string;
    readonly isError: boolean;
  }> = [];

  for (const entry of content) {
    if (!entry || typeof entry !== "object") {
      continue;
    }

    const block = entry as Record<string, unknown>;
    if (block.type !== "tool_result") {
      continue;
    }

    const toolUseId = typeof block.tool_use_id === "string" ? block.tool_use_id : undefined;
    if (!toolUseId) {
      continue;
    }

    blocks.push({
      toolUseId,
      block,
      text: extractTextContent(block.content),
      isError: block.is_error === true,
    });
  }

  return blocks;
}

function toolUseBlocksFromAssistantMessage(message: SDKMessage): Array<{
  readonly index: number;
  readonly toolUseId: string;
  readonly toolName: string;
  readonly input: Record<string, unknown>;
}> {
  if (message.type !== "assistant") {
    return [];
  }

  const content = (message.message as { content?: unknown } | undefined)?.content;
  if (!Array.isArray(content)) {
    return [];
  }

  const blocks: Array<{
    readonly index: number;
    readonly toolUseId: string;
    readonly toolName: string;
    readonly input: Record<string, unknown>;
  }> = [];

  for (const [index, entry] of content.entries()) {
    if (!entry || typeof entry !== "object") {
      continue;
    }

    const block = entry as {
      type?: unknown;
      id?: unknown;
      name?: unknown;
      input?: unknown;
      arguments?: unknown;
    };
    if (block.type !== "tool_use" && block.type !== "toolCall") {
      continue;
    }

    const toolUseId = typeof block.id === "string" ? block.id : undefined;
    const toolName = typeof block.name === "string" ? block.name : undefined;
    if (!toolUseId || !toolName) {
      continue;
    }

    const rawInput = block.input ?? block.arguments;
    blocks.push({
      index,
      toolUseId,
      toolName,
      input:
        typeof rawInput === "object" && rawInput !== null
          ? (rawInput as Record<string, unknown>)
          : {},
    });
  }

  return blocks;
}

function streamKindFromDeltaType(
  deltaType: string,
): "assistant_text" | "reasoning_text" | undefined {
  if (deltaType === "thinking_delta" || deltaType === "reasoning_text") {
    return "reasoning_text";
  }
  if (deltaType === "text_delta" || deltaType === "assistant_text") {
    return "assistant_text";
  }
  return undefined;
}

function nativeProviderRefs(
  context: PiSessionContext,
  options?: { readonly providerItemId?: string },
): Record<string, unknown> {
  return options?.providerItemId ? { providerItemId: options.providerItemId } : {};
}

function toError(cause: unknown, fallback: string): Error {
  return cause instanceof Error ? cause : new Error(fallback);
}

function toRequestError(
  threadId: ThreadId,
  operation: string,
  cause: unknown,
): ProviderAdapterError {
  const message = cause instanceof Error ? cause.message : String(cause);
  return new ProviderAdapterProcessError({
    provider: PROVIDER,
    threadId,
    detail: `${operation} failed: ${message}`,
    cause,
  });
}

export interface PiAdapterLiveOptions {
  readonly piBinaryPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
}

const makePiAdapter = Effect.fn("makePiAdapter")(function* (options?: PiAdapterLiveOptions) {
  const sessions = new Map<ThreadId, PiSessionContext>();
  const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const serverSettingsService = yield* ServerSettingsService;
  const runtimeContext = yield* Effect.context<never>();
  const runFork = Effect.runForkWith(runtimeContext);

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const nextEventId = Effect.map(Random.nextUUIDv4, (id) => EventId.make(id));
  const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

  const offerRuntimeEvent = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
    Queue.offer(runtimeEventQueue, event).pipe(Effect.asVoid);

  const ensureAssistantTextBlock = (
    context: PiSessionContext,
    blockIndex: number,
    opts?: { readonly fallbackText?: string; readonly streamClosed?: boolean },
  ) =>
    Effect.sync(() => {
      const turnState = context.turnState;
      if (!turnState) {
        return undefined;
      }

      const existing = turnState.assistantTextBlocks.get(blockIndex);
      if (existing) {
        if (opts?.fallbackText !== undefined && !existing.emittedTextDelta) {
          existing.fallbackText = opts.fallbackText;
        }
        if (opts?.streamClosed !== undefined) {
          existing.streamClosed = opts.streamClosed;
        }
        return { blockIndex, block: existing };
      }

      const itemId = crypto.randomUUID();
      const block: AssistantTextBlockState = {
        itemId,
        blockIndex,
        emittedTextDelta: false,
        fallbackText: opts?.fallbackText ?? "",
        streamClosed: opts?.streamClosed ?? false,
        completionEmitted: false,
      };
      turnState.assistantTextBlocks.set(blockIndex, block);
      turnState.assistantTextBlockOrder.push(block);
      return { blockIndex, block };
    });

  const completeAssistantTextBlock = Effect.fn("completeAssistantTextBlock")(function* (
    context: PiSessionContext,
    block: AssistantTextBlockState,
  ) {
    const turnState = context.turnState;
    if (!turnState || block.completionEmitted) {
      return;
    }

    if (!block.streamClosed) {
      return;
    }

    if (!block.emittedTextDelta && block.fallbackText.length > 0) {
      const deltaStamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "content.delta",
        eventId: deltaStamp.eventId,
        provider: PROVIDER,
        createdAt: deltaStamp.createdAt,
        threadId: context.session.threadId,
        turnId: turnState.turnId,
        itemId: asRuntimeItemId(block.itemId),
        payload: {
          streamKind: "assistant_text",
          delta: block.fallbackText,
        },
        providerRefs: nativeProviderRefs(context),
      });
    }

    block.completionEmitted = true;
    if (turnState.assistantTextBlocks.get(block.blockIndex) === block) {
      turnState.assistantTextBlocks.delete(block.blockIndex);
    }

    const stamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "item.completed",
      eventId: stamp.eventId,
      provider: PROVIDER,
      createdAt: stamp.createdAt,
      itemId: asRuntimeItemId(block.itemId),
      threadId: context.session.threadId,
      turnId: turnState.turnId,
      payload: {
        itemType: "assistant_message",
        status: "completed",
        title: "Assistant message",
        ...(block.fallbackText.length > 0 ? { detail: block.fallbackText } : {}),
      },
      providerRefs: nativeProviderRefs(context),
    });
  });

  const emitRuntimeError = Effect.fn("emitRuntimeError")(function* (
    context: PiSessionContext,
    message: string,
    cause?: unknown,
  ) {
    const turnState = context.turnState;
    const stamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "runtime.error",
      eventId: stamp.eventId,
      provider: PROVIDER,
      createdAt: stamp.createdAt,
      threadId: context.session.threadId,
      ...(turnState ? { turnId: turnState.turnId } : {}),
      payload: {
        message,
        class: "provider_error",
        ...(cause !== undefined ? { detail: cause } : {}),
      },
      providerRefs: nativeProviderRefs(context),
    });
  });

  const completeTurn = Effect.fn("completeTurn")(function* (
    context: PiSessionContext,
    status: ProviderRuntimeTurnStatus,
    errorMessage?: string,
  ) {
    const turnState = context.turnState;
    if (!turnState) {
      return;
    }

    context.turns.push({
      id: turnState.turnId,
      items: [...turnState.items],
    });

    if (context.lastKnownTokenUsage) {
      const usageStamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "thread.token-usage.updated",
        eventId: usageStamp.eventId,
        provider: PROVIDER,
        createdAt: usageStamp.createdAt,
        threadId: context.session.threadId,
        turnId: turnState.turnId,
        payload: {
          usage: context.lastKnownTokenUsage,
        },
        providerRefs: nativeProviderRefs(context),
      });
    }

    const stamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "turn.completed",
      eventId: stamp.eventId,
      provider: PROVIDER,
      createdAt: stamp.createdAt,
      threadId: context.session.threadId,
      turnId: turnState.turnId,
      payload: {
        state: status,
        ...(errorMessage ? { errorMessage } : {}),
      },
      providerRefs: nativeProviderRefs(context),
    });

    const updatedAt = yield* nowIso;
    context.turnState = undefined;
    context.session = {
      ...context.session,
      status: "ready",
      activeTurnId: undefined,
      updatedAt,
      ...(status === "failed" && errorMessage ? { lastError: errorMessage } : {}),
    };
  });

  const handleStreamEvent = Effect.fn("handleStreamEvent")(function* (
    context: PiSessionContext,
    message: SDKMessage,
  ) {
    if (message.type !== "stream_event") {
      return;
    }

    const { event } = message;

    if (event.type === "content_block_delta") {
      if (
        (event.delta.type === "text_delta" || event.delta.type === "thinking_delta") &&
        context.turnState
      ) {
        const deltaText =
          event.delta.type === "text_delta"
            ? event.delta.text
            : typeof event.delta.thinking === "string"
              ? event.delta.thinking
              : "";
        if (deltaText.length === 0) {
          return;
        }
        const streamKind = streamKindFromDeltaType(event.delta.type);
        if (!streamKind) {
          return;
        }
        const assistantBlockEntry =
          event.delta.type === "text_delta"
            ? yield* ensureAssistantTextBlock(context, event.index)
            : context.turnState.assistantTextBlocks.get(event.index)
              ? {
                  blockIndex: event.index,
                  block: context.turnState.assistantTextBlocks.get(
                    event.index,
                  ) as AssistantTextBlockState,
                }
              : undefined;
        if (assistantBlockEntry?.block && event.delta.type === "text_delta") {
          assistantBlockEntry.block.emittedTextDelta = true;
        }
        const stamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "content.delta",
          eventId: stamp.eventId,
          provider: PROVIDER,
          createdAt: stamp.createdAt,
          threadId: context.session.threadId,
          turnId: context.turnState.turnId,
          ...(assistantBlockEntry?.block
            ? {
                itemId: asRuntimeItemId(assistantBlockEntry.block.itemId),
              }
            : {}),
          payload: {
            streamKind,
            delta: deltaText,
          },
          providerRefs: nativeProviderRefs(context),
          raw: {
            source: "pi.event",
            method: "content_block_delta",
            payload: message,
          },
        });
        return;
      }

      if (event.delta.type === "input_json_delta") {
        const tool = context.inFlightTools.get(event.index);
        if (!tool || typeof event.delta.partial_json !== "string") {
          return;
        }

        const partialInputJson = tool.partialInputJson + event.delta.partial_json;
        const parsedInput = tryParseJsonRecord(partialInputJson);
        const detail = parsedInput ? summarizeToolRequest(tool.toolName, parsedInput) : tool.detail;
        let nextTool: ToolInFlight = {
          ...tool,
          partialInputJson,
          ...(parsedInput ? { input: parsedInput } : {}),
          ...(detail ? { detail } : {}),
        };

        const nextFingerprint =
          parsedInput && Object.keys(parsedInput).length > 0
            ? toolInputFingerprint(parsedInput)
            : undefined;
        context.inFlightTools.set(event.index, nextTool);

        if (
          !parsedInput ||
          !nextFingerprint ||
          tool.lastEmittedInputFingerprint === nextFingerprint
        ) {
          return;
        }

        nextTool = {
          ...nextTool,
          lastEmittedInputFingerprint: nextFingerprint,
        };
        context.inFlightTools.set(event.index, nextTool);

        const stamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "item.updated",
          eventId: stamp.eventId,
          provider: PROVIDER,
          createdAt: stamp.createdAt,
          threadId: context.session.threadId,
          ...(context.turnState
            ? {
                turnId: context.turnState.turnId,
              }
            : {}),
          itemId: asRuntimeItemId(nextTool.itemId),
          payload: {
            itemType: nextTool.itemType,
            status: "inProgress",
            title: nextTool.title,
            ...(nextTool.detail ? { detail: nextTool.detail } : {}),
            data: {
              toolName: nextTool.toolName,
              input: nextTool.input,
            },
          },
          providerRefs: nativeProviderRefs(context, {
            providerItemId: nextTool.itemId,
          }),
          raw: {
            source: "pi.event",
            method: "content_block_delta/input_json_delta",
            payload: message,
          },
        });
        return;
      }
      return;
    }

    if (event.type === "content_block_start") {
      const { index, content_block: block } = event;
      if (block.type === "text") {
        yield* ensureAssistantTextBlock(context, index, {
          fallbackText:
            "fallbackText" in block
              ? ((block as { fallbackText?: string }).fallbackText ?? "")
              : "",
        });
        return;
      }
      if (
        block.type !== "tool_use" &&
        block.type !== "server_tool_use" &&
        block.type !== "mcp_tool_use"
      ) {
        return;
      }

      const toolName = block.name;
      const itemType = classifyToolItemType(toolName);
      const toolInput =
        typeof block.input === "object" && block.input !== null
          ? (block.input as Record<string, unknown>)
          : {};
      const itemId = block.id;
      const detail = summarizeToolRequest(toolName, toolInput);

      const tool: ToolInFlight = {
        itemId,
        itemType,
        toolName,
        title: titleForTool(itemType),
        detail,
        input: toolInput,
        partialInputJson: "",
      };
      context.inFlightTools.set(index, tool);

      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "item.started",
        eventId: stamp.eventId,
        provider: PROVIDER,
        createdAt: stamp.createdAt,
        threadId: context.session.threadId,
        ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
        itemId: asRuntimeItemId(tool.itemId),
        payload: {
          itemType: tool.itemType,
          status: "inProgress",
          title: tool.title,
          ...(tool.detail ? { detail: tool.detail } : {}),
          data: {
            toolName: tool.toolName,
            input: toolInput,
          },
        },
        providerRefs: nativeProviderRefs(context, {
          providerItemId: tool.itemId,
        }),
        raw: {
          source: "pi.event",
          method: "content_block_start",
          payload: message,
        },
      });
      return;
    }

    if (event.type === "content_block_stop") {
      const { index } = event;
      const assistantBlock = context.turnState?.assistantTextBlocks.get(index);
      if (assistantBlock) {
        assistantBlock.streamClosed = true;
        yield* completeAssistantTextBlock(context, assistantBlock);
        return;
      }
      const tool = context.inFlightTools.get(index);
      if (!tool) {
        return;
      }
    }
  });

  const handleResultMessage = Effect.fn("handleResultMessage")(function* (
    context: PiSessionContext,
    message: SDKMessage,
  ) {
    if (message.type !== "result") {
      return;
    }

    const status = message.subtype === "success" ? "completed" : "failed";
    const errorMessage =
      message.subtype === "success" ? undefined : (message.errors?.[0] ?? "pi turn failed");

    if (status === "failed") {
      yield* emitRuntimeError(context, errorMessage ?? "pi turn failed.");
    }

    yield* completeTurn(context, status, errorMessage);
  });

  const handleUserMessage = Effect.fn("handleUserMessage")(function* (
    context: PiSessionContext,
    message: SDKMessage,
  ) {
    if (message.type !== "user") {
      return;
    }

    if (context.turnState) {
      context.turnState.items.push(message.message);
    }

    for (const toolResult of toolResultBlocksFromUserMessage(message)) {
      const toolEntry = Array.from(context.inFlightTools.entries()).find(
        ([, tool]) => tool.itemId === toolResult.toolUseId,
      );
      if (!toolEntry) {
        continue;
      }

      const [index, tool] = toolEntry;
      const itemStatus = toolResult.isError ? "failed" : "completed";
      const toolData = {
        toolName: tool.toolName,
        input: tool.input,
        result: toolResult.block,
      };

      const updatedStamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "item.updated",
        eventId: updatedStamp.eventId,
        provider: PROVIDER,
        createdAt: updatedStamp.createdAt,
        threadId: context.session.threadId,
        ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
        itemId: asRuntimeItemId(tool.itemId),
        payload: {
          itemType: tool.itemType,
          status: toolResult.isError ? "failed" : "inProgress",
          title: tool.title,
          ...(tool.detail ? { detail: tool.detail } : {}),
          data: toolData,
        },
        providerRefs: nativeProviderRefs(context, {
          providerItemId: tool.itemId,
        }),
        raw: {
          source: "pi.event",
          method: "pi/user",
          payload: message,
        },
      });

      const streamKind = toolResultStreamKind(tool.itemType);
      if (streamKind && toolResult.text.length > 0 && context.turnState) {
        const deltaStamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "content.delta",
          eventId: deltaStamp.eventId,
          provider: PROVIDER,
          createdAt: deltaStamp.createdAt,
          threadId: context.session.threadId,
          turnId: context.turnState.turnId,
          itemId: asRuntimeItemId(tool.itemId),
          payload: {
            streamKind,
            delta: toolResult.text,
          },
          providerRefs: nativeProviderRefs(context, {
            providerItemId: tool.itemId,
          }),
          raw: {
            source: "pi.event",
            method: "pi/user",
            payload: message,
          },
        });
      }

      const completedStamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "item.completed",
        eventId: completedStamp.eventId,
        provider: PROVIDER,
        createdAt: completedStamp.createdAt,
        threadId: context.session.threadId,
        ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
        itemId: asRuntimeItemId(tool.itemId),
        payload: {
          itemType: tool.itemType,
          status: itemStatus,
          title: tool.title,
          ...(tool.detail ? { detail: tool.detail } : {}),
          data: toolData,
        },
        providerRefs: nativeProviderRefs(context, {
          providerItemId: tool.itemId,
        }),
        raw: {
          source: "pi.event",
          method: "pi/user",
          payload: message,
        },
      });

      context.inFlightTools.delete(index);
    }
  });

  const handleAssistantMessage = Effect.fn("handleAssistantMessage")(function* (
    context: PiSessionContext,
    message: SDKMessage,
  ) {
    if (message.type !== "assistant") {
      return;
    }

    if (context.turnState) {
      context.turnState.items.push(message.message);
    }

    for (const toolUse of toolUseBlocksFromAssistantMessage(message)) {
      const alreadyTracked = Array.from(context.inFlightTools.values()).some(
        (tool) => tool.itemId === toolUse.toolUseId,
      );
      if (alreadyTracked) {
        continue;
      }

      const itemType = classifyToolItemType(toolUse.toolName);
      const tool: ToolInFlight = {
        itemId: toolUse.toolUseId,
        itemType,
        toolName: toolUse.toolName,
        title: titleForTool(itemType),
        detail: summarizeToolRequest(toolUse.toolName, toolUse.input),
        input: toolUse.input,
        partialInputJson: "",
      };
      context.inFlightTools.set(toolUse.index, tool);

      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "item.started",
        eventId: stamp.eventId,
        provider: PROVIDER,
        createdAt: stamp.createdAt,
        threadId: context.session.threadId,
        ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
        itemId: asRuntimeItemId(tool.itemId),
        payload: {
          itemType: tool.itemType,
          status: "inProgress",
          title: tool.title,
          ...(tool.detail ? { detail: tool.detail } : {}),
          data: {
            toolName: tool.toolName,
            input: tool.input,
          },
        },
        providerRefs: nativeProviderRefs(context, {
          providerItemId: tool.itemId,
        }),
        raw: {
          source: "pi.event",
          method: "pi/assistant",
          payload: message,
        },
      });
    }
  });

  const handleSystemMessage = Effect.fn("handleSystemMessage")(function* (
    context: PiSessionContext,
    message: SDKMessage,
  ) {
    if (message.type !== "system") {
      return;
    }

    const stamp = yield* makeEventStamp();
    const base = {
      eventId: stamp.eventId,
      provider: PROVIDER,
      createdAt: stamp.createdAt,
      threadId: context.session.threadId,
      ...(context.turnState ? { turnId: context.turnState.turnId } : {}),
      providerRefs: nativeProviderRefs(context),
    };

    switch (message.subtype) {
      case "init":
        yield* offerRuntimeEvent({
          ...base,
          type: "session.configured",
          payload: {
            config: message as Record<string, unknown>,
          },
        });
        return;
      case "status":
        yield* offerRuntimeEvent({
          ...base,
          type: "session.state.changed",
          payload: {
            state: "running",
            reason: `status:${message.status ?? "active"}`,
          },
        });
        return;
      default:
        return;
    }
  });

  const handleSdkMessage = Effect.fn("handleSdkMessage")(function* (
    context: PiSessionContext,
    message: SDKMessage,
  ) {
    switch (message.type) {
      case "stream_event":
        yield* handleStreamEvent(context, message);
        return;
      case "assistant":
        yield* handleAssistantMessage(context, message);
        return;
      case "user":
        yield* handleUserMessage(context, message);
        return;
      case "result":
        yield* handleResultMessage(context, message);
        return;
      case "system":
        yield* handleSystemMessage(context, message);
        return;
      default:
        return;
    }
  });

  const runSdkStream = (context: PiSessionContext): Effect.Effect<void, Error> =>
    Stream.fromAsyncIterable(context.query, (cause) =>
      toError(cause, "pi runtime stream failed."),
    ).pipe(
      Stream.takeWhile(() => !context.stopped),
      Stream.runForEach((message) => handleSdkMessage(context, message)),
    );

  const handleStreamExit = Effect.fn("handleStreamExit")(function* (
    context: PiSessionContext,
    exit: Exit.Exit<void, Error>,
  ) {
    if (context.stopped) {
      return;
    }

    if (Exit.isFailure(exit)) {
      yield* emitRuntimeError(context, "pi runtime stream failed.", Cause.pretty(exit.cause));
      yield* completeTurn(context, "failed", "pi runtime stream failed.");
    } else if (context.turnState) {
      yield* completeTurn(context, "interrupted", "pi runtime stream ended.");
    }

    yield* stopSessionInternal(context, {
      emitExitEvent: true,
    });
  });

  const stopSessionInternal = Effect.fn("stopSessionInternal")(function* (
    context: PiSessionContext,
    options?: { readonly emitExitEvent?: boolean },
  ) {
    if (context.stopped) return;

    context.stopped = true;

    for (const [, pending] of context.pendingApprovals) {
      yield* Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore);
    }
    context.pendingApprovals.clear();

    if (context.turnState) {
      yield* completeTurn(context, "interrupted", "Session stopped.");
    }

    yield* Queue.shutdown(context.promptQueue);

    yield* Effect.try({
      try: () => context.query.close(),
      catch: (cause) =>
        new ProviderAdapterProcessError({
          provider: PROVIDER,
          threadId: context.session.threadId,
          detail: "Failed to close pi runtime query.",
          cause,
        }),
    }).pipe(
      Effect.matchEffect({
        onFailure: (error) => emitRuntimeError(context, error.detail, error.cause),
        onSuccess: () => Effect.void,
      }),
    );

    const streamFiber = context.streamFiber;
    context.streamFiber = undefined;
    if (streamFiber && streamFiber.pollUnsafe() === undefined) {
      yield* Fiber.interrupt(streamFiber);
    }

    const updatedAt = yield* nowIso;
    context.session = {
      ...context.session,
      status: "closed",
      activeTurnId: undefined,
      updatedAt,
    };

    if (options?.emitExitEvent !== false) {
      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "session.exited",
        eventId: stamp.eventId,
        provider: PROVIDER,
        createdAt: stamp.createdAt,
        threadId: context.session.threadId,
        payload: {
          reason: "Session stopped",
          exitKind: "graceful",
        },
        providerRefs: {},
      });
    }

    sessions.delete(context.session.threadId);
  });

  const requireSession = (
    threadId: ThreadId,
  ): Effect.Effect<PiSessionContext, ProviderAdapterError> => {
    const context = sessions.get(threadId);
    if (!context) {
      return Effect.fail(
        new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId,
        }),
      );
    }
    if (context.stopped || context.session.status === "closed") {
      return Effect.fail(
        new ProviderAdapterSessionClosedError({
          provider: PROVIDER,
          threadId,
        }),
      );
    }
    return Effect.succeed(context);
  };

  const startSession: PiAdapterShape["startSession"] = Effect.fn("startSession")(function* (input) {
    if (input.provider !== undefined && input.provider !== PROVIDER) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "startSession",
        issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
      });
    }

    const existingContext = sessions.get(input.threadId);
    if (existingContext) {
      yield* Effect.logWarning("pi.session.replacing", {
        threadId: input.threadId,
        existingSessionStatus: existingContext.session.status,
        reason: "startSession called with existing active session",
      });
      yield* stopSessionInternal(existingContext, {
        emitExitEvent: false,
      }).pipe(
        Effect.catchCause(() =>
          Effect.logWarning("pi.session.replace.stop-failed", {
            threadId: input.threadId,
          }),
        ),
      );
    }

    const startedAt = yield* nowIso;
    const threadId = input.threadId;

    const promptQueue = yield* Queue.unbounded<PromptQueueItem>();
    const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
    const pendingUserInputs = new Map<ApprovalRequestId, PendingUserInput>();
    const inFlightTools = new Map<number, ToolInFlight>();

    // Get pi settings
    const piSettings = yield* serverSettingsService.getSettings.pipe(
      Effect.map((settings) => settings.providers.pi),
      Effect.mapError(
        (error) =>
          new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId,
            detail: error.message,
            cause: error,
          }),
      ),
    );
    const piBinaryPath = piSettings.binaryPath || options?.piBinaryPath;
    const modelSelection =
      input.modelSelection?.instanceId === PI_INSTANCE_ID ? input.modelSelection : undefined;

    // Parse the model slug to extract provider and model
    // Model slug format can be "provider/model" (e.g., "minimax/MiniMax-M2.7") or just "model"
    let piProvider = "anthropic"; // default LLM provider
    let piModel = "claude-sonnet-4"; // default model
    if (modelSelection?.model) {
      const modelSlug = modelSelection.model;
      const slashIndex = modelSlug.indexOf("/");
      if (slashIndex > 0) {
        piProvider = modelSlug.substring(0, slashIndex);
        piModel = modelSlug.substring(slashIndex + 1);
      } else {
        piModel = modelSlug;
      }
    }

    // Create pi query runtime
    const queryRuntime = new PiQueryRuntime({
      ...(input.cwd ? { cwd: input.cwd } : {}),
      env: process.env,
      provider: piProvider,
      model: piModel,
      ...(piBinaryPath ? { piCliPath: piBinaryPath } : {}),
    });

    // Start the RPC client
    yield* Effect.tryPromise({
      try: () => queryRuntime.start(),
      catch: (cause) =>
        new ProviderAdapterProcessError({
          provider: PROVIDER,
          threadId,
          detail: toMessage(cause, "Failed to start pi runtime session."),
          cause,
        }),
    });

    const session: ProviderSession = {
      threadId,
      provider: PROVIDER,
      status: "ready",
      runtimeMode: input.runtimeMode,
      ...(input.cwd ? { cwd: input.cwd } : {}),
      ...(piModel ? { model: piModel } : {}),
      ...(threadId ? { threadId } : {}),
      resumeCursor: {},
      createdAt: startedAt,
      updatedAt: startedAt,
    };

    const context: PiSessionContext = {
      session,
      promptQueue,
      query: queryRuntime,
      streamFiber: undefined,
      startedAt,
      pendingApprovals,
      pendingUserInputs,
      turns: [],
      inFlightTools,
      turnState: undefined,
      lastKnownTokenUsage: undefined,
      lastAssistantUuid: undefined,
      lastThreadStartedId: undefined,
      stopped: false,
    };
    sessions.set(threadId, context);

    const sessionStartedStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "session.started",
      eventId: sessionStartedStamp.eventId,
      provider: PROVIDER,
      createdAt: sessionStartedStamp.createdAt,
      threadId,
      payload: {},
      providerRefs: {},
    });

    const configuredStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "session.configured",
      eventId: configuredStamp.eventId,
      provider: PROVIDER,
      createdAt: configuredStamp.createdAt,
      threadId,
      payload: {
        config: input.cwd ? { cwd: input.cwd } : {},
      },
      providerRefs: {},
    });

    const readyStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "session.state.changed",
      eventId: readyStamp.eventId,
      provider: PROVIDER,
      createdAt: readyStamp.createdAt,
      threadId,
      payload: {
        state: "ready",
      },
      providerRefs: {},
    });

    let streamFiber: Fiber.Fiber<void, never>;
    streamFiber = runFork(
      Effect.exit(runSdkStream(context)).pipe(
        Effect.flatMap((exit) => {
          if (context.stopped) {
            return Effect.void;
          }
          if (context.streamFiber === streamFiber) {
            context.streamFiber = undefined;
          }
          return handleStreamExit(context, exit);
        }),
      ),
    );
    context.streamFiber = streamFiber;
    streamFiber.addObserver(() => {
      if (context.streamFiber === streamFiber) {
        context.streamFiber = undefined;
      }
    });

    return {
      ...session,
    };
  });

  const sendTurn: PiAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
    const context = yield* requireSession(input.threadId);

    if (context.turnState) {
      yield* completeTurn(context, "completed");
    }

    const turnId = TurnId.make(yield* Random.nextUUIDv4);
    const turnState: PiTurnState = {
      turnId,
      startedAt: yield* nowIso,
      items: [],
      assistantTextBlocks: new Map(),
      assistantTextBlockOrder: [],
      nextSyntheticAssistantBlockIndex: -1,
    };

    const updatedAt = yield* nowIso;
    context.turnState = turnState;
    context.session = {
      ...context.session,
      status: "running",
      activeTurnId: turnId,
      updatedAt,
    };

    const turnStartedStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "turn.started",
      eventId: turnStartedStamp.eventId,
      provider: PROVIDER,
      createdAt: turnStartedStamp.createdAt,
      threadId: context.session.threadId,
      turnId,
      payload: {},
      providerRefs: {},
    });

    const requestedEffort =
      input.modelSelection?.instanceId === PI_INSTANCE_ID
        ? (getModelSelectionStringOptionValue(input.modelSelection, "effort") ?? "")
        : "";
    if (requestedEffort.length > 0) {
      yield* Effect.tryPromise({
        try: () => context.query.setThinkingLevel(requestedEffort),
        catch: (cause) => toRequestError(input.threadId, "turn/setThinkingLevel", cause),
      });
    }

    // Send the prompt to pi via RPC stdin
    // input.input is optional but ProviderService validates that at least input or attachments exist
    const promptMessage = input.input ?? "";
    yield* Effect.tryPromise({
      try: () => context.query.prompt(promptMessage),
      catch: (cause) => toRequestError(input.threadId, "turn/start", cause),
    });

    return {
      threadId: context.session.threadId,
      turnId,
    };
  });

  const interruptTurn: PiAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
    function* (threadId) {
      const context = yield* requireSession(threadId);
      yield* Effect.tryPromise({
        try: () => context.query.interrupt(),
        catch: (cause) => toRequestError(threadId, "turn/interrupt", cause),
      });
    },
  );

  const readThread: PiAdapterShape["readThread"] = Effect.fn("readThread")(function* (threadId) {
    const context = yield* requireSession(threadId);
    return {
      threadId: context.session.threadId,
      turns: context.turns.map((turn) => ({
        id: turn.id,
        items: [...turn.items],
      })),
    };
  });

  const rollbackThread: PiAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
    function* (threadId, numTurns) {
      const context = yield* requireSession(threadId);
      const nextLength = Math.max(0, context.turns.length - numTurns);
      context.turns.splice(nextLength);
      return yield* Effect.succeed({
        threadId: context.session.threadId,
        turns: context.turns.map((turn) => ({
          id: turn.id,
          items: [...turn.items],
        })),
      });
    },
  );

  const respondToRequest: PiAdapterShape["respondToRequest"] = Effect.fn("respondToRequest")(
    function* (threadId, requestId, decision) {
      const context = yield* requireSession(threadId);
      const pending = context.pendingApprovals.get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "item/requestApproval/decision",
          detail: `Unknown pending approval request: ${requestId}`,
        });
      }

      context.pendingApprovals.delete(requestId);
      yield* Deferred.succeed(pending.decision, decision);
    },
  );

  const respondToUserInput: PiAdapterShape["respondToUserInput"] = Effect.fn("respondToUserInput")(
    function* (threadId, requestId, answers) {
      const context = yield* requireSession(threadId);
      const pending = context.pendingUserInputs.get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "item/tool/respondToUserInput",
          detail: `Unknown pending user-input request: ${requestId}`,
        });
      }

      context.pendingUserInputs.delete(requestId);
      yield* Deferred.succeed(pending.answers, answers);
    },
  );

  const stopSession: PiAdapterShape["stopSession"] = Effect.fn("stopSession")(function* (threadId) {
    const context = yield* requireSession(threadId);
    yield* stopSessionInternal(context, {
      emitExitEvent: true,
    });
  });

  const listSessions: PiAdapterShape["listSessions"] = () =>
    Effect.sync(() => Array.from(sessions.values(), ({ session }) => ({ ...session })));

  const hasSession: PiAdapterShape["hasSession"] = (threadId) =>
    Effect.sync(() => {
      const context = sessions.get(threadId);
      return context !== undefined && !context.stopped;
    });

  const stopAll: PiAdapterShape["stopAll"] = () =>
    Effect.forEach(
      sessions,
      ([, context]) =>
        stopSessionInternal(context, {
          emitExitEvent: true,
        }),
      { discard: true },
    );

  yield* Effect.addFinalizer(() =>
    Effect.forEach(
      sessions,
      ([, context]) =>
        stopSessionInternal(context, {
          emitExitEvent: false,
        }),
      { discard: true },
    ).pipe(Effect.tap(() => Queue.shutdown(runtimeEventQueue))),
  );

  return {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "in-session",
    },
    startSession,
    sendTurn,
    interruptTurn,
    readThread,
    rollbackThread,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    stopAll,
    streamEvents: Stream.fromQueue(runtimeEventQueue),
  } satisfies PiAdapterShape;
});

function toMessage(cause: unknown, fallback: string): string {
  if (cause instanceof Error && cause.message.length > 0) {
    return cause.message;
  }
  return fallback;
}

export const PiAdapterLive = Layer.effect(PiAdapter, makePiAdapter());

export function makePiAdapterLive(options?: PiAdapterLiveOptions) {
  return Layer.effect(PiAdapter, makePiAdapter(options));
}
