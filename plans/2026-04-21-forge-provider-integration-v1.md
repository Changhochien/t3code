# Forge Provider Integration Plan

## Overview

Add **Forge** (by tailcallhq) as a new provider in T3 Code. Forge is a CLI-based coding agent with multi-provider support (Claude, GPT, OpenAI, Gemini, etc.) and three built-in agents (forge, sage, muse).

## Forge Architecture Summary

| Attribute               | Value                                                          |
| ----------------------- | -------------------------------------------------------------- |
| **Type**                | CLI-based coding agent (Rust)                                  |
| **Protocol**            | Child process + JSON conversation files                        |
| **Install**             | `curl -fsSL https://forgecode.dev/cli \| sh`                   |
| **Multi-provider**      | Yes (Claude, OpenAI, Google, Grok, 300+ models)                |
| **Agents**              | `forge` (implementation), `sage` (research), `muse` (planning) |
| **Session persistence** | Via conversation IDs                                           |
| **MCP support**         | Yes (`.mcp.json`)                                              |

## Integration Pattern: Codex + JSON Conversation

Since Forge uses JSON conversation files rather than raw JSON-RPC over stdio, we'll use a **hybrid approach**:

```
┌─────────────────────────────────────────────────────────────┐
│                    Forge Adapter Architecture                │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ForgeAdapter (ProviderAdapterShape)                        │
│    │                                                        │
│    ├── ForgeSessionRuntime                                  │
│    │   │                                                    │
│    │   ├── Spawn: forge --conversation <path>              │
│    │   │                                                    │
│    │   ├── Write conversation JSON to temp file              │
│    │   │   { messages: [...], model: "...", agent: "..." }  │
│    │   │                                                    │
│    │   └── Parse stdout for events                         │
│    │       ├── [stream] delta events                        │
│    │       ├── tool.use / tool.result                       │
│    │       └── permission.request                           │
│    │                                                        │
│    └── Conversation ID persistence                           │
│        └── Store/retrieve via conversation-id               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Implementation Steps

### Phase 1: Schema Updates (`packages/contracts/`)

#### Step 1.1: Add "forge" to ProviderKind

**File:** `packages/contracts/src/orchestration.ts:34`

```typescript
// Before
type ProviderKind = "codex" | "claude" | "opencode" | "pi" | "cursor";

// After
type ProviderKind = "codex" | "claude" | "opencode" | "pi" | "cursor" | "forge";
```

#### Step 1.2: Create ForgeSettings schema

**File:** `packages/contracts/src/settings.ts`

```typescript
// Add after CodexSettings
export const ForgeSettings = Schema.Struct({
  binaryPath: Schema.optional(Schema.String),
  defaultAgent: Schema.optional(Schema.Literal("forge", "sage", "muse")),
  conversationDir: Schema.optional(Schema.String),
});

export type ForgeSettings = Schema.Schema.Type<typeof ForgeSettings>;
```

#### Step 1.3: Update ServerSettings

**File:** `packages/contracts/src/settings.ts:146-152`

```typescript
// Add to providers union
providers: Schema.Struct({
  codex: Schema.optional(CodexSettings),
  claude: Schema.optional(ClaudeSettings),
  opencode: Schema.optional(OpenCodeSettings),
  pi: Schema.optional(PiSettings),
  cursor: Schema.optional(CursorSettings),
  forge: Schema.optional(ForgeSettings),  // NEW
}),
```

---

### Phase 2: Service Definition (`apps/server/src/provider/`)

#### Step 2.1: Create ForgeAdapter service

**File:** `apps/server/src/provider/Services/ForgeAdapter.ts`

```typescript
import { Context, Effect, Stream } from "effect";
import type { ProviderRuntimeEvent } from "@t3tools/contracts";
import type { ProviderAdapterShape, ProviderAdapterCapabilities } from "./ProviderAdapter.ts";

export interface ForgeAdapterShape extends ProviderAdapterShape<never> {
  readonly provider: "forge";
  readonly capabilities: ProviderAdapterCapabilities;
}

export const ForgeAdapter = Context.GenericTag<ForgeAdapterShape>("@t3tools/server/ForgeAdapter");

declare module "../Services.ts" {
  interface ServiceRegistry {
    readonly [ForgeAdapter.key]: ForgeAdapterShape;
  }
}
```

---

### Phase 3: Runtime Implementation (`apps/server/src/provider/`)

#### Step 3.1: Create ForgeSessionRuntime

**File:** `apps/server/src/provider/Layers/ForgeSessionRuntime.ts`

Key responsibilities:

- Spawn child process: `forge --conversation <path>`
- Write conversation JSON to temp file
- Parse stdout for streaming events
- Handle conversation ID persistence
- Clean up temp files on completion

```typescript
// Key types
interface ForgeConversationMessage {
  role: "user" | "assistant" | "system";
  content: string;
  timestamp?: string;
}

interface ForgeConversation {
  messages: ForgeConversationMessage[];
  model?: string;
  agent?: "forge" | "sage" | "muse";
  // ... other fields from forge conversation dump
}

interface ForgeStreamEvent {
  type: "message.delta" | "tool.use" | "tool.result" | "permission.request" | "error";
  data: Record<string, unknown>;
}
```

#### Step 3.2: Create ForgeAdapter live implementation

**File:** `apps/server/src/provider/Layers/ForgeAdapter.ts`

Based on CodexAdapter pattern with modifications for:

- JSON conversation file management
- Conversation ID tracking
- Multi-agent support (forge/sage/muse)

```typescript
export const ForgeAdapterLive = Layer.effect(ForgeAdapter, makeForgeAdapter());

function makeForgeAdapter() {
  return Effect.gen(function* () {
    // Session management
    const sessions = new Map<ThreadId, ForgeSessionContext>();

    // Implement all ProviderAdapterShape methods
    return {
      provider: "forge" as const,
      capabilities: { sessionModelSwitch: "in-session" },
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
      streamEvents,
    };
  });
}
```

---

### Phase 4: Adapter Registration

#### Step 4.1: Register in ProviderAdapterRegistry

**File:** `apps/server/src/provider/Layers/ProviderAdapterRegistry.ts`

```typescript
import { ForgeAdapter } from "../Services/ForgeAdapter.ts";
import { ForgeAdapterLive } from "./ForgeAdapter.ts";

// Add to Layer.produce merge
Layer.produce(registryLayer, (registry) => registry.add(ForgeAdapter, ForgeAdapterLive)),

// Add to getByProvider switch
case "forge":
  return ForgeAdapter;
```

---

### Phase 5: Configuration & Error Handling

#### Step 5.1: Add Forge-specific errors

**File:** `apps/server/src/provider/Errors.ts`

```typescript
export class ForgeConversationError extends ProviderAdapterError {
  readonly _tag = "ForgeConversationError";
  // ...
}

export class ForgeBinaryNotFoundError extends ProviderAdapterError {
  readonly _tag = "ForgeBinaryNotFoundError";
  // ...
}
```

#### Step 5.2: Update server settings defaults

**File:** `apps/server/src/serverSettings.ts`

```typescript
// Add default Forge settings
const DEFAULT_FORGE_SETTINGS: ForgeSettings = {
  defaultAgent: "forge",
};
```

---

## File Structure

```
apps/server/src/provider/
├── Services/
│   └── ForgeAdapter.ts          # Service definition
├── Layers/
│   ├── ForgeAdapter.ts          # Live adapter implementation
│   └── ForgeSessionRuntime.ts   # Child process + conversation management
└── Errors.ts                    # Add Forge-specific errors

packages/contracts/src/
├── orchestration.ts             # Add "forge" to ProviderKind
└── settings.ts                  # Add ForgeSettings schema
```

---

## Event Mapping

Forge's output needs to be mapped to `ProviderRuntimeEvent`:

| Forge Event          | T3 Code Event           |
| -------------------- | ----------------------- |
| `message.delta`      | `content.delta`         |
| `tool.use`           | `item.started` (tool)   |
| `tool.result`        | `item.completed` (tool) |
| `permission.request` | `request.opened`        |
| `error`              | `runtime.error`         |

---

## Verification

After implementation, run:

```bash
bun fmt
bun lint
bun typecheck
```

---

## Estimated Effort

| Phase                 | Complexity  | Time Estimate |
| --------------------- | ----------- | ------------- |
| Phase 1: Schema       | Low         | 30 min        |
| Phase 2: Service      | Low         | 15 min        |
| Phase 3: Runtime      | Medium-High | 3-4 hours     |
| Phase 4: Registration | Low         | 15 min        |
| Phase 5: Config       | Low         | 30 min        |
| **Total**             |             | **~5 hours**  |

---

## Future Enhancements

1. **Conversation dump import**: Use `forge conversation dump <id>` to restore sessions
2. **MCP integration**: Pass `.mcp.json` config to Forge
3. **Workspace sync**: Integrate with `forge workspace sync` for semantic search
4. **Multi-agent**: Support switching between forge/sage/muse agents
