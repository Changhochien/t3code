/**
 * PiQueryRuntime - Adapter that wraps pi as a ClaudeQueryRuntime.
 *
 * This allows pi to be used as a provider in t3code by spawning pi's CLI
 * and adapting its JSON-Lines output to the AsyncIterable<SDKMessage> interface.
 *
 * @module PiQueryRuntime
 */
import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import { spawn, type ChildProcess } from "node:child_process";

export interface PiQueryRuntimeOptions {
  readonly cwd?: string;
  readonly env?: NodeJS.ProcessEnv;
  readonly provider?: string;
  readonly model?: string;
  readonly piCliPath?: string;
  readonly additionalArgs?: string[];
}

/**
 * PiQueryRuntime implements the ClaudeQueryRuntime interface by spawning
 * pi in RPC mode and converting its JSON-Lines output to SDKMessage format.
 */
export class PiQueryRuntime implements AsyncIterable<SDKMessage>, ClaudeQueryRuntime {
  private process: ChildProcess | null = null;
  private messageQueue: SDKMessage[] = [];
  private isStreaming = false;
  private currentModel: string | undefined = undefined;
  private permissionMode?: string;
  private maxThinkingTokens?: number | null;
  private resolveQueue: Array<(value: IteratorResult<SDKMessage>) => void> = [];
  private isDone = false;
  private started = false;
  private abortController: AbortController | null = null;
  private options: PiQueryRuntimeOptions;
  private pendingPromptResolve: (() => void) | null = null;
  private isProcessing = false;

  constructor(options: PiQueryRuntimeOptions = {}) {
    this.options = options;
  }

  /**
   * Send a prompt to pi via stdin using the RPC protocol.
   */
  readonly prompt: (message: string) => Promise<void> = async (message: string) => {
    if (!this.process?.stdin) {
      throw new Error("pi process not started or stdin not available");
    }

    return new Promise((resolve, reject) => {
      const command = JSON.stringify({ type: "prompt", message }) + "\n";
      this.process!.stdin!.write(command, (err) => {
        if (err) {
          reject(err);
        } else {
          // Wait for the prompt to be accepted before resolving
          // The response will come via stdout and we'll handle it there
          this.pendingPromptResolve = resolve;
          // Timeout after 30 seconds
          setTimeout(() => {
            if (this.pendingPromptResolve === resolve) {
              this.pendingPromptResolve = null;
              resolve(); // Resolve anyway - the prompt was likely sent
            }
          }, 30000);
        }
      });
    });
  };

  /**
   * Send an abort command to pi via stdin.
   */
  readonly abort: () => Promise<void> = async () => {
    if (!this.process?.stdin) {
      return;
    }

    return new Promise((resolve) => {
      const command = JSON.stringify({ type: "abort" }) + "\n";
      this.process!.stdin!.write(command, () => {
        resolve();
      });
    });
  };

  readonly interrupt: () => Promise<void> = async () => {
    // Send abort via stdin first (cleaner than SIGINT)
    await this.abort();
    if (this.abortController) {
      this.abortController.abort();
    }
  };

  readonly setModel: (model?: string) => Promise<void> = async (model?: string) => {
    this.currentModel = model;
    // pi doesn't support dynamic model switching after start
    // This would need to be handled per-session
  };

  readonly setPermissionMode: (mode: string) => Promise<void> = async (mode: string) => {
    this.permissionMode = mode;
    // pi handles permissions via its own auth/config
  };

  readonly setMaxThinkingTokens: (maxThinkingTokens: number | null) => Promise<void> = async (
    maxThinkingTokens: number | null,
  ) => {
    this.maxThinkingTokens = maxThinkingTokens;
    // pi doesn't expose this directly
  };

  readonly setThinkingLevel: (level: string) => Promise<void> = async (level: string) => {
    if (!this.process?.stdin) {
      throw new Error("pi process not started or stdin not available");
    }

    return new Promise((resolve, reject) => {
      const command = JSON.stringify({ type: "set_thinking_level", level }) + "\n";
      this.process!.stdin!.write(command, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  };

  readonly close: () => void = () => {
    this.isDone = true;

    // Resolve any pending iterators
    for (const resolve of this.resolveQueue) {
      resolve({ value: undefined as unknown as SDKMessage, done: true });
    }
    this.resolveQueue = [];

    // Stop the pi process
    if (this.process) {
      this.process.kill("SIGTERM");
      this.process = null;
    }
  };

  async start(): Promise<void> {
    if (this.started) {
      throw new Error("PiQueryRuntime already started");
    }
    this.started = true;

    return new Promise((resolve, reject) => {
      const piPath = this.options.piCliPath ?? this.findPiCliPath();
      const args = this.buildArgs();

      this.process = spawn(piPath, args, {
        cwd: this.options.cwd,
        env: { ...process.env, ...this.options.env },
        stdio: ["pipe", "pipe", "pipe"],
      });

      this.process.on("error", (err) => {
        reject(err);
      });

      this.process.on("spawn", () => {
        // Set up stdout parsing
        if (this.process?.stdout) {
          let stdoutBuffer = "";

          this.process.stdout.on("data", (data: Buffer) => {
            stdoutBuffer += data.toString();

            let newlineIndex = stdoutBuffer.indexOf("\n");
            while (newlineIndex >= 0) {
              const line = stdoutBuffer.slice(0, newlineIndex).trim();
              stdoutBuffer = stdoutBuffer.slice(newlineIndex + 1);

              if (line.length > 0) {
                try {
                  const json = JSON.parse(line);
                  this.handlePiJsonEvent(json);
                } catch {
                  // Ignore non-JSON lines
                }
              }

              newlineIndex = stdoutBuffer.indexOf("\n");
            }
          });
        }

        // Forward stderr for logging
        if (this.process?.stderr) {
          this.process.stderr.on("data", (data: Buffer) => {
            console.error("[pi]", data.toString());
          });
        }

        resolve();
      });

      // Handle process exit
      this.process.on("exit", (code, signal) => {
        if (code !== 0 && code !== null) {
          this.enqueueMessage({
            type: "result",
            subtype: "error_during_execution",
            is_error: true,
            errors: [`pi exited with code ${code}, signal: ${signal}`],
            session_id: "",
          } as unknown as SDKMessage);
        }
        this.isDone = true;
      });
    });
  }

  private buildArgs(): string[] {
    const args: string[] = [];

    // T3 sessions are ephemeral and should not inherit or resume Pi's persisted session state.
    args.push("--no-session");

    // Model selection
    if (this.options.model) {
      args.push("--model", this.options.model);
    }

    // Provider selection
    if (this.options.provider) {
      args.push("--provider", this.options.provider);
    }

    // Additional args
    if (this.options.additionalArgs) {
      args.push(...this.options.additionalArgs);
    }

    // Enable RPC mode
    args.push("--mode", "rpc");

    return args;
  }

  private findPiCliPath(): string {
    // Look for pi in common locations - prefer system installation
    const possiblePaths = [
      // System installation (Homebrew)
      "/opt/homebrew/bin/pi",
      "/usr/local/bin/pi",
      // npx/npm global
      "pi",
      // Local development path
      "/Users/changtom/pi-mono/packages/coding-agent/dist/cli.js",
    ];

    // Try to find a working pi
    for (const path of possiblePaths) {
      try {
        // Simple check - if path exists or if it's just "pi" (will be in PATH)
        if (path === "pi" || require("node:fs").existsSync(path)) {
          return path;
        }
      } catch {
        // Continue to next path
      }
    }

    // Default to system pi
    return "pi";
  }

  private handlePiJsonEvent(event: Record<string, unknown>): void {
    const messages = this.convertEventToSdkMessages(event);
    for (const msg of messages) {
      this.enqueueMessage(msg);
    }
  }

  private assistantMessageContentBlock(
    value: unknown,
    contentIndex: number,
  ): Record<string, unknown> | undefined {
    if (!value || typeof value !== "object") {
      return undefined;
    }

    const content = (value as { content?: unknown }).content;
    if (!Array.isArray(content)) {
      return undefined;
    }

    const block = content.at(contentIndex);
    return block && typeof block === "object" ? (block as Record<string, unknown>) : undefined;
  }

  private messageUpdateText(
    assistantMessageEvent: Record<string, unknown>,
    event: Record<string, unknown>,
    contentIndex: number,
    blockType: "text" | "thinking",
  ): string | undefined {
    const block =
      this.assistantMessageContentBlock(assistantMessageEvent.partial, contentIndex) ??
      this.assistantMessageContentBlock(event.message, contentIndex);
    if (!block || block.type !== blockType) {
      return undefined;
    }

    if (blockType === "text") {
      return typeof block.text === "string" ? block.text : undefined;
    }

    return typeof block.thinking === "string" ? block.thinking : undefined;
  }

  private normalizeToolCall(block: Record<string, unknown> | undefined):
    | {
        readonly id: string;
        readonly name: string;
        readonly input: Record<string, unknown>;
      }
    | undefined {
    if (!block) {
      return undefined;
    }

    const type = block.type as string | undefined;
    if (type !== "tool_use" && type !== "toolCall") {
      return undefined;
    }

    const id = typeof block.id === "string" ? block.id : crypto.randomUUID();
    const name = typeof block.name === "string" ? block.name : "unknown";
    const rawInput = block.input ?? block.arguments;
    const input =
      typeof rawInput === "object" && rawInput !== null
        ? (rawInput as Record<string, unknown>)
        : {};

    return { id, name, input };
  }

  private messageUpdateToolCall(
    assistantMessageEvent: Record<string, unknown>,
    event: Record<string, unknown>,
    contentIndex: number,
  ) {
    return (
      this.normalizeToolCall(
        assistantMessageEvent.toolCall as Record<string, unknown> | undefined,
      ) ??
      this.normalizeToolCall(
        this.assistantMessageContentBlock(assistantMessageEvent.partial, contentIndex),
      ) ??
      this.normalizeToolCall(this.assistantMessageContentBlock(event.message, contentIndex))
    );
  }

  private enqueueMessage(message: SDKMessage): void {
    this.messageQueue.push(message);

    // Resolve any pending iterators
    const resolves = this.resolveQueue.splice(0);
    for (const resolve of resolves) {
      resolve({ value: message, done: false });
    }
  }

  private convertEventToSdkMessages(event: Record<string, unknown>): SDKMessage[] {
    const messages: SDKMessage[] = [];
    const type = event.type as string;

    switch (type) {
      case "agent_start":
        messages.push({
          type: "system",
          subtype: "init",
          session_id: (event.sessionId as string) ?? "",
        } as unknown as SDKMessage);
        break;

      case "agent_end":
        messages.push({
          type: "result",
          subtype: "success",
          stop_reason: "end_turn",
          session_id: (event.sessionId as string) ?? "",
        } as unknown as SDKMessage);
        break;

      case "turn_start": {
        const turnId = event.turnId as string;
        messages.push({
          type: "system",
          subtype: "task_started",
          task_id: turnId,
          description: "Turn started",
          session_id: (event.sessionId as string) ?? "",
        } as unknown as SDKMessage);
        break;
      }

      case "turn_end": {
        // pi's turn_end: { type: "turn_end", message: {...}, toolResults: [...] }
        const messageObj = event.message as Record<string, unknown> | undefined;
        const content = messageObj?.content as Array<Record<string, unknown>> | undefined;
        const toolResults = event.toolResults as Array<Record<string, unknown>> | undefined;
        const messageId = (messageObj?.id as string) ?? crypto.randomUUID();

        // Emit assistant message
        if (content && content.length > 0) {
          messages.push({
            type: "assistant",
            message: {
              id: messageId,
              type: "assistant",
              role: "assistant",
              content: this.convertContentToBlocks(content),
              model: this.currentModel ?? "unknown",
              stop_reason: toolResults && toolResults.length > 0 ? "tool_use" : "end_turn",
            },
            uuid: messageId,
            session_id: (event.sessionId as string) ?? "",
          } as unknown as SDKMessage);
        }

        // Emit tool results
        if (toolResults) {
          for (const toolResult of toolResults) {
            // toolResult can be: { toolCallId, result, isError } or { toolCallId, content, isError }
            const resultContent = toolResult.result ?? toolResult.content;
            messages.push({
              type: "user",
              message: {
                id: crypto.randomUUID(),
                type: "message",
                role: "user",
                content: [
                  {
                    type: "tool_result",
                    tool_use_id: (toolResult.toolCallId as string) ?? "",
                    content:
                      typeof resultContent === "string" || Array.isArray(resultContent)
                        ? resultContent
                        : JSON.stringify(resultContent),
                    is_error: toolResult.isError === true,
                  },
                ],
              },
              session_id: (event.sessionId as string) ?? "",
            } as unknown as SDKMessage);
          }
        }
        break;
      }

      case "message_start": {
        const content = event.content as Array<Record<string, unknown>> | undefined;
        if (content) {
          messages.push({
            type: "assistant",
            message: {
              id: (event.messageId as string) ?? crypto.randomUUID(),
              type: "assistant",
              role: "assistant",
              content: this.convertContentToBlocks(content),
              model: this.currentModel ?? "unknown",
            },
            uuid: (event.messageId as string) ?? "",
            session_id: (event.sessionId as string) ?? "",
          } as unknown as SDKMessage);
        }
        break;
      }

      case "response": {
        // Handle RPC command responses
        const command = event.command as string;
        const success = event.success as boolean;
        if (command === "prompt" && success && this.pendingPromptResolve) {
          const resolve = this.pendingPromptResolve;
          this.pendingPromptResolve = null;
          resolve();
        }
        if (command === "abort" && success) {
          // Abort was accepted
        }
        break;
      }

      case "message_update": {
        // pi's message_update structure:
        // { type: "message_update", message: {...}, assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "text" } }
        const assistantMessageEvent = event.assistantMessageEvent as
          | Record<string, unknown>
          | undefined;
        if (!assistantMessageEvent) break;

        const eventType = assistantMessageEvent.type as string;
        const contentIndex = (assistantMessageEvent.contentIndex ??
          assistantMessageEvent.index ??
          0) as number;

        if (eventType === "text_start" || eventType === "textStart") {
          messages.push({
            type: "stream_event",
            event: {
              type: "content_block_start",
              index: contentIndex,
              content_block: {
                type: "text",
                fallbackText:
                  this.messageUpdateText(assistantMessageEvent, event, contentIndex, "text") ?? "",
              },
            },
            session_id: (event.sessionId as string) ?? "",
          } as unknown as SDKMessage);
        }

        if (eventType === "text_delta") {
          const deltaText = (assistantMessageEvent.delta as string) ?? "";
          if (deltaText) {
            messages.push({
              type: "stream_event",
              event: {
                type: "content_block_delta",
                index: contentIndex,
                delta: {
                  type: "text_delta",
                  text: deltaText,
                },
              },
              session_id: (event.sessionId as string) ?? "",
            } as unknown as SDKMessage);
          }
        }

        if (eventType === "thinking_delta") {
          const deltaText = (assistantMessageEvent.delta as string) ?? "";
          if (deltaText) {
            messages.push({
              type: "stream_event",
              event: {
                type: "content_block_delta",
                index: contentIndex,
                delta: {
                  type: "thinking_delta",
                  text: deltaText,
                  thinking: deltaText,
                },
              },
              session_id: (event.sessionId as string) ?? "",
            } as unknown as SDKMessage);
          }
        }

        if (eventType === "toolcall_delta") {
          const partialJson =
            (assistantMessageEvent.partialJson as string) ??
            (assistantMessageEvent.delta as string);
          if (partialJson) {
            messages.push({
              type: "stream_event",
              event: {
                type: "content_block_delta",
                index: contentIndex,
                delta: {
                  type: "input_json_delta",
                  partial_json: partialJson,
                },
              },
              session_id: (event.sessionId as string) ?? "",
            } as unknown as SDKMessage);
          }
        }

        if (eventType === "toolcall_start" || eventType === "toolcallStart") {
          const toolCall = this.messageUpdateToolCall(assistantMessageEvent, event, contentIndex);
          if (toolCall) {
            messages.push({
              type: "stream_event",
              event: {
                type: "content_block_start",
                index: contentIndex,
                content_block: {
                  type: "tool_use",
                  id: toolCall.id,
                  name: toolCall.name,
                  input: toolCall.input,
                },
              },
              session_id: (event.sessionId as string) ?? "",
            } as unknown as SDKMessage);
          }
        }

        if (
          eventType === "done" ||
          eventType === "text_end" ||
          eventType === "thinking_end" ||
          eventType === "toolcall_end"
        ) {
          // Message streaming complete - emit content_block_stop for all indices
          // This signals end of streaming for tool_use blocks
          messages.push({
            type: "stream_event",
            event: {
              type: "content_block_stop",
              index: contentIndex,
            },
            session_id: (event.sessionId as string) ?? "",
          } as unknown as SDKMessage);
        }
        break;
      }

      case "tool_execution_start": {
        const toolName = event.toolName as string;
        const toolCallId = event.toolCallId as string;
        const args = (event.args as Record<string, unknown>) ?? {};

        messages.push({
          type: "stream_event",
          event: {
            type: "content_block_start",
            index: 0,
            content_block: {
              type: "tool_use",
              id: toolCallId ?? crypto.randomUUID(),
              name: toolName ?? "unknown",
              input: args,
            },
          },
          session_id: (event.sessionId as string) ?? "",
        } as unknown as SDKMessage);
        break;
      }

      case "tool_execution_end": {
        messages.push({
          type: "stream_event",
          event: {
            type: "content_block_stop",
            index: 0,
          },
          session_id: (event.sessionId as string) ?? "",
        } as unknown as SDKMessage);
        break;
      }

      case "error": {
        messages.push({
          type: "result",
          subtype: "error_during_execution",
          is_error: true,
          errors: [(event.message as string) ?? "Unknown error"],
          session_id: (event.sessionId as string) ?? "",
        } as unknown as SDKMessage);
        break;
      }

      case "status": {
        messages.push({
          type: "system",
          subtype: "status",
          status: (event.status as string) ?? "active",
          session_id: (event.sessionId as string) ?? "",
        } as unknown as SDKMessage);
        break;
      }
    }

    return messages;
  }

  private convertContentToBlocks(
    content: Array<Record<string, unknown>>,
  ): Array<{ type: string; id?: string; name?: string; input?: unknown; text?: string }> {
    const blocks: Array<{
      type: string;
      id?: string;
      name?: string;
      input?: unknown;
      text?: string;
    }> = [];

    for (const [index, block] of content.entries()) {
      const blockType = block.type as string;
      if (blockType === "text") {
        blocks.push({ type: "text", text: (block.text as string) ?? "" });
        continue;
      }
      if (blockType === "tool_use" || blockType === "toolCall") {
        blocks.push({
          type: "tool_use",
          id: (block.id as string) ?? `tool_${index}`,
          name: (block.name as string) ?? "",
          input: ((block.input ?? block.arguments) as Record<string, unknown> | undefined) ?? {},
        });
      }
    }

    return blocks;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<SDKMessage> {
    while (!this.isDone) {
      // If we have queued messages, yield them
      if (this.messageQueue.length > 0) {
        const message = this.messageQueue.shift()!;
        yield message;
        continue;
      }

      // If we're done, exit
      if (this.isDone) {
        break;
      }

      // Wait for a new message via a promise
      await new Promise<void>((resolve) => {
        this.resolveQueue.push(() => resolve());
        // Timeout to allow checking isDone
        setTimeout(() => {
          resolve();
        }, 100);
      });
    }

    // Drain remaining messages
    while (this.messageQueue.length > 0) {
      yield this.messageQueue.shift()!;
    }
  }
}

interface ClaudeQueryRuntime extends AsyncIterable<SDKMessage> {
  readonly interrupt: () => Promise<void>;
  readonly abort: () => Promise<void>;
  readonly prompt: (message: string) => Promise<void>;
  readonly setModel: (model?: string) => Promise<void>;
  readonly setPermissionMode: (mode: string) => Promise<void>;
  readonly setMaxThinkingTokens: (maxThinkingTokens: number | null) => Promise<void>;
  readonly close: () => void;
}
