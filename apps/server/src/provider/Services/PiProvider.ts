import { Context } from "effect";

import type { ServerProviderShape } from "./ServerProvider.ts";

export interface PiProviderShape extends ServerProviderShape {}

export class PiProvider extends Context.Service<PiProvider, PiProviderShape>()(
  "t3/provider/Services/PiProvider",
) {}
