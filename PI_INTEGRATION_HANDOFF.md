# t3code + pi Integration: Full Context Handoff

## Overview

Goal: Integrate pi (badlogic/pi-mono) as a provider in t3code (pingdotgg/t3code)

## Repositories

### t3code (fork)

- **Location:** `~/t3code-fork/`
- **Source:** https://github.com/pingdotgg/t3code
- **Purpose:** Desktop web GUI for coding agents (Codex, Claude, etc.)
- **Stack:** Effect framework, WebSocket server, React UI, SQLite

### pi-mono (source)

- **Location:** `~/pi-mono/`
- **Source:** https://github.com/badlogic/pi-mono
- **Purpose:** Terminal coding harness with modular providers
- **Stack:** TypeScript, RPC mode, extensible provider system

## Key Architecture Files

### t3code Structure

```
apps/server/src/
├── provider/
│   ├── Services/
│   │   ├── ClaudeAdapter.ts      # Service tag definition
│   │   ├── ProviderAdapter.ts    # Base interface
│   │   └── ProviderAdapterRegistry.ts
│   ├── Layers/
│   │   ├── ClaudeAdapter.ts      # Live implementation (3218 lines)
│   │   └── ProviderAdapterRegistry.ts
│   └── Errors.ts
├── server.ts                     # Entry point, wires layers
└── serverSettings.ts             # Configuration schema

packages/contracts/
└── src/
    ├── events.ts                 # ProviderRuntimeEvent types
    └── types.ts                  # Shared type definitions
```

### pi Structure

```
packages/coding-agent/
├── src/
│   ├── cli.ts                    # CLI entry point
│   ├── main.ts                   # Main agent logic
│   ├── modes/
│   │   └── rpc/
│   │       ├── rpc-client.ts    # Programmatic RPC client
│   │       ├── rpc-types.ts     # Type definitions
│   │       └── jsonl.ts         # JSON-Lines protocol
│   └── core/
│       ├── agent-session.ts     # Session management
│       └── model-registry.ts     # Provider/model resolution
└── docs/
    ├── providers.md             # Provider documentation
    ├── rpc.md                   # RPC mode docs
    └── custom-provider.md       # Extension docs
```

## Integration Points

### 1. Primary: `createQuery` Injection (Option A - Recommended)

**File:** `apps/server/src/provider/Layers/ClaudeAdapter.ts`

**Lines 174-181:** Interface definition

```typescript
export interface ClaudeAdapterLiveOptions {
  readonly createQuery?: (input: {
    readonly prompt: AsyncIterable<SDKUserMessage>;
    readonly options: ClaudeQueryOptions;
  }) => ClaudeQueryRuntime;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
}
```

**Lines 969-978:** Default implementation

```typescript
const createQuery =
  options?.createQuery ??
  ((input: {
    readonly prompt: AsyncIterable<SDKUserMessage>;
    readonly options: ClaudeQueryOptions;
  }) =>
    query({
      prompt: input.prompt,
      options: input.options,
    }) as ClaudeQueryRuntime);
```

**Key:** Inject custom `createQuery` that routes to pi instead of using `@anthropic-ai/claude-agent-sdk`

### 2. Secondary: Add PiAdapter (Option B)

**File:** `apps/server/src/provider/Layers/ProviderAdapterRegistry.ts`

**Lines 27-57:** Adapter registration

```typescript
const makeProviderAdapterRegistry = Effect.fn("makeProviderAdapterRegistry")(function* (
  options?: ProviderAdapterRegistryLiveOptions,
) {
  const adapters = options?.adapters ?? [
    yield* CodexAdapter,
    yield* ClaudeAdapter,
    yield* OpenCodeAdapter,
    ...(cursorAdapterOption._tag === "Some" ? [cursorAdapterOption.value] : []),
  ];
  const byProvider = new Map(adapters.map((adapter) => [adapter.provider, adapter]));
  // ...
});
```

**Would add:** `yield* PiAdapter` to the adapters list

### 3. Provider Adapter Registry Interface

**File:** `apps/server/src/provider/Services/ProviderAdapter.ts`

Defines `ProviderAdapterShape` interface that all adapters must implement:

- `provider: string` - Provider identifier
- `startSession(input)` - Start a new session
- `sendTurn(input)` - Send user input
- `interruptTurn(threadId, turnId)` - Interrupt current turn
- `readThread(threadId)` - Get thread state
- `stopSession(threadId)` - Stop session
- `streamEvents` - Event stream

## Required Protocol Mapping

### t3's Expected Message Types (SDKMessage)

```typescript
type SDKMessage =
  | { type: "stream_event", event: StreamEvent, ... }
  | { type: "user", message: SDKUserMessage, ... }
  | { type: "assistant", message: SDKAssistantMessage, ... }
  | { type: "result", subtype: "success" | "error_during_execution", ... }
  | { type: "system", subtype: SystemSubtype, ... }
  | { type: "tool_progress", ... }
  | { type: "tool_use_summary", ... }
  | { type: "auth_status", ... }
  | { type: "rate_limit_event", ... }
```

### StreamEvent Types (content_block_delta, content_block_start, etc.)

```typescript
type StreamEvent =
  | {
      type: "content_block_delta";
      index: number;
      delta: TextDelta | ThinkingDelta | InputJsonDelta;
    }
  | { type: "content_block_start"; index: number; content_block: ToolUseBlock | TextBlock }
  | { type: "content_block_stop"; index: number };
```

### pi's Event Types (from AgentEvent)

Check `packages/coding-agent/src/modes/rpc/rpc-types.ts` for pi's event schema

### ClaudeQueryRuntime Interface (must implement)

```typescript
interface ClaudeQueryRuntime extends AsyncIterable<SDKMessage> {
  readonly interrupt: () => Promise<void>;
  readonly setModel: (model?: string) => Promise<void>;
  readonly setPermissionMode: (mode: PermissionMode) => Promise<void>;
  readonly setMaxThinkingTokens: (maxThinkingTokens: number | null) => Promise<void>;
  readonly close: () => void;
}
```

## Implementation Tasks

### Phase 1: Create PiQueryRuntime adapter

1. Create `apps/server/src/provider/Layers/PiQueryRuntime.ts`
2. Implement `ClaudeQueryRuntime` interface
3. Use pi's RPC client to spawn agent and stream events
4. Map pi's event types to SDKMessage format

### Phase 2: Create PiAdapter

1. Create `apps/server/src/provider/Services/PiAdapter.ts` (service tag)
2. Create `apps/server/src/provider/Layers/PiAdapter.ts` (implementation)
3. Use PiQueryRuntime for session management
4. Implement all ProviderAdapterShape methods

### Phase 3: Register PiAdapter

1. Update `ProviderAdapterRegistry.ts` to include PiAdapter
2. Add "pi" to ProviderKind schema if needed
3. Add Pi settings to ServerSettings

### Phase 4: Testing

1. Build server: `cd apps/server && bun build`
2. Test with: `t3 --provider pi --model <model>`
3. Verify events map correctly (turn.started, content.delta, item.completed, etc.)

## Configuration

### t3 Settings

- **File:** `~/.t3/userdata/settings.json`
- **Current:** Uses claudeAgent provider with Opus 4-7

### pi Settings

- **Auth:** `~/.pi/agent/auth.json`
- **Providers:** See `docs/providers.md` for supported providers

## Reference Documentation

- t3 AGENTS.md: `~/t3code-fork/AGENTS.md`
- pi providers: `~/pi-mono/packages/coding-agent/docs/providers.md`
- pi RPC docs: `~/pi-mono/packages/coding-agent/docs/rpc.md`
- t3 ClaudeAdapter: Full implementation in `~/t3code-fork/apps/server/src/provider/Layers/ClaudeAdapter.ts`

## Notes

1. **Effect Framework:** t3 uses Effect for dependency injection. Learn Effect patterns before modifying.
2. **Event Streaming:** t3's strength is its event system. Preserve all event types for UI compatibility.
3. **pi RPC Mode:** Check if pi's RPC mode can spawn without TTY and stream JSON events
4. **Permission Handling:** t3 has approval flow; pi may need similar abstraction
5. **Session Management:** t3 persists sessions in SQLite; pi uses different mechanism

## Commands

```bash
# Build t3
cd ~/t3code-fork && bun install && bun build

# Run t3
npx t3

# Run pi (for testing)
cd ~/pi-mono && bun run src/cli.ts --help

# Test pi RPC mode
cd ~/pi-mono && bun run src/cli.ts --rpc --help
```
