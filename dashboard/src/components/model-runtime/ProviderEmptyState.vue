<template>
  <div class="d-flex flex-column ga-4">
    <v-sheet rounded="xl" class="empty-state-panel empty-provider-panel pa-8">
      <div class="text-overline section-label mb-3">
        {{ $t("pages.modelRuntime.sidebar.providers") }}
      </div>
      <div class="text-h6 mb-2">
        {{ $t("pages.modelRuntime.hints.selectProviderSourceTitle") }}
      </div>
      <div class="text-body-2 text-medium-emphasis">
        {{ $t("pages.modelRuntime.hints.selectProviderSource") }}
      </div>
    </v-sheet>
  </div>
</template>

<style scoped lang="scss">
@use "@/styles/mixins" as *;

.section-label {
  font-size: $font-size-xs;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: rgb(var(--v-theme-primary));
  opacity: 0.82;
}

.empty-state-panel {
  border: 2px dashed $border-color-soft;
  background: $surface-subtle;
  border-radius: $radius-lg;
}

.empty-provider-panel {
  min-height: 400px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  text-align: center;
}
</style>
