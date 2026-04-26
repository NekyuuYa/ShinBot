import { inject, type InjectionKey } from "vue";

import type { useModelRuntimePage } from "@/composables/useModelRuntimePage";

export type ModelRuntimePageContext = ReturnType<typeof useModelRuntimePage>;

export const modelRuntimePageKey: InjectionKey<ModelRuntimePageContext> =
  Symbol("model-runtime-page");

export function useModelRuntimeContext() {
  const context = inject(modelRuntimePageKey);
  if (!context) {
    throw new Error("ModelRuntime page context is not available");
  }
  return context;
}
