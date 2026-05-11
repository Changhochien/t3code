/**
 * PiAdapter - pi implementation of the generic provider adapter contract.
 *
 * This service owns pi runtime/session semantics and emits canonical
 * provider runtime events. It does not perform cross-provider routing, shared
 * event fan-out, or checkpoint orchestration.
 *
 * Uses Effect `Context.Service` for dependency injection and returns the
 * shared provider-adapter error channel with `provider: "pi"` context.
 *
 * @module PiAdapter
 */
import * as Context from "effect/Context";

import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * PiAdapterShape - Service API for the pi provider adapter.
 */
export interface PiAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}

/**
 * PiAdapter - Service tag for pi provider adapter operations.
 */
export class PiAdapter extends Context.Service<PiAdapter, PiAdapterShape>()(
  "t3/provider/Services/PiAdapter",
) {}
