<template>
  <section class="analysis-panel" :class="panelClass">
    <div class="panel-head" :class="compact ? 'panel-head--compact' : ''">
      <div>
        <div class="panel-kicker">{{ kicker }}</div>
        <h2 class="panel-title">{{ title }}</h2>
        <p class="panel-subtitle">{{ subtitle }}</p>
      </div>

      <div v-if="totalLabel && totalValue" class="chart-total">
        <span>{{ totalLabel }}</span>
        <strong>{{ totalValue }}</strong>
      </div>
    </div>

    <div v-if="hasPoints" class="line-chart" :class="chartClass" :style="chartStyle">
      <div class="line-chart__plot">
        <svg
          class="line-chart__svg"
          viewBox="0 0 100 100"
          preserveAspectRatio="none"
          aria-hidden="true"
        >
          <defs>
            <linearGradient :id="areaGradientId" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stop-color="currentColor" :stop-opacity="areaStartOpacity" />
              <stop offset="100%" stop-color="currentColor" :stop-opacity="areaEndOpacity" />
            </linearGradient>
          </defs>
          <line
            v-for="guide in guideYs"
            :key="`${areaGradientId}-guide-${guide}`"
            class="line-chart__guide"
            x1="0"
            x2="100"
            :y1="guide"
            :y2="guide"
          />
          <path class="line-chart__area" :d="areaPath" />
          <path class="line-chart__path" :d="linePath" />
        </svg>

        <v-tooltip v-for="point in points" :key="point.key" location="top">
          <template #activator="{ props }">
            <button
              v-bind="props"
              type="button"
              class="line-chart__point"
              :aria-label="String(point.key)"
              :style="{ left: `${point.x}%`, top: `${point.y}%` }"
            />
          </template>

          <slot name="tooltip" :point="point" />
        </v-tooltip>
      </div>

      <div class="line-chart__axis">
        <span v-for="label in axisLabels" :key="label.key">{{ label.text }}</span>
      </div>
    </div>

    <v-empty-state
      v-else
      icon="mdi-chart-line"
      :title="emptyTitle"
      :text="emptyText"
      variant="plain"
    />
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { CostAnalysisBucket } from '@/api/modelRuntime'

export interface CostTrendPoint {
  key: string
  x: number
  y: number
  bucket: CostAnalysisBucket
}

export interface AxisLabel {
  key: string
  text: string
}

const props = withDefaults(
  defineProps<{
    kicker: string
    title: string
    subtitle: string
    totalLabel?: string
    totalValue?: string
    panelClass?: string
    chartClass?: string
    areaGradientId: string
    areaStartOpacity?: number
    areaEndOpacity?: number
    guideYs: number[]
    points: CostTrendPoint[]
    linePath: string
    areaPath: string
    axisLabels: AxisLabel[]
    emptyTitle: string
    emptyText: string
    compact?: boolean
  }>(),
  {
    totalLabel: '',
    totalValue: '',
    panelClass: '',
    chartClass: '',
    areaStartOpacity: 0.22,
    areaEndOpacity: 0.02,
    compact: false,
  }
)

defineSlots<{
  tooltip(props: { point: CostTrendPoint }): unknown
}>()

const hasPoints = computed(() => props.points.length > 0 && Boolean(props.linePath))
const chartStyle = computed(() => ({
  '--line-chart-label-count': String(Math.max(props.axisLabels.length, 1)),
}))
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.analysis-panel {
  @include analysis-section-panel;
  min-width: 0;
}

.panel-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 20px;
  margin-bottom: 22px;

  &--compact {
    margin-bottom: 14px;
  }
}

.panel-kicker {
  margin-bottom: 8px;
  color: rgb(var(--v-theme-primary));
  font-size: $font-size-xs;
  font-weight: 700;
  letter-spacing: 0;
  text-transform: uppercase;
}

.panel-title {
  margin: 0;
  color: rgba(var(--v-theme-on-surface), 0.94);
  font-size: $font-size-lg;
  font-weight: 800;
  line-height: 1.2;
}

.panel-subtitle {
  margin: 8px 0 0;
  max-width: 720px;
  color: rgba(var(--v-theme-on-surface), 0.66);
  font-size: 0.92rem;
  line-height: 1.55;
}

.chart-total {
  display: grid;
  justify-items: end;
  gap: 4px;
  flex: 0 0 auto;
  min-width: 140px;

  span {
    color: rgba(var(--v-theme-on-surface), 0.56);
    font-size: $font-size-xs;
    font-weight: 700;
  }

  strong {
    color: rgba(var(--v-theme-on-surface), 0.92);
    font-size: 1.15rem;
  }
}

.line-chart {
  display: grid;
  grid-template-rows: minmax(0, 1fr) 18px;
  gap: 12px;
  min-height: 270px;

  &--cost {
    color: rgb(var(--v-theme-warning));
  }

  &--tokens {
    color: rgb(var(--v-theme-primary));
  }

  &--hourly {
    min-height: 230px;
  }
}

.line-chart__plot {
  position: relative;
  min-height: 220px;
  overflow: hidden;
  border: 1px solid $border-color-soft;
  border-radius: $radius-sm;
  background:
    linear-gradient(
      180deg,
      rgba(var(--v-theme-on-surface), 0.018) 0%,
      rgba(var(--v-theme-on-surface), 0.006) 100%
    ),
    rgb(var(--v-theme-surface));
}

.line-chart--hourly .line-chart__plot {
  min-height: 180px;
}

.line-chart__svg {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
}

.line-chart__guide {
  stroke: rgba(var(--v-theme-on-surface), 0.08);
  stroke-width: 0.45;
  vector-effect: non-scaling-stroke;
}

.line-chart__area {
  stroke: none;
}

.line-chart--cost .line-chart__area {
  fill: url('#daily-cost-area');
}

.line-chart--tokens .line-chart__area {
  fill: url('#hourly-token-area');
}

.line-chart__path {
  fill: none;
  stroke: currentColor;
  stroke-linecap: round;
  stroke-linejoin: round;
  stroke-width: 2.4;
  filter: drop-shadow(0 5px 10px rgba(var(--v-theme-on-surface), 0.08));
  vector-effect: non-scaling-stroke;
}

.line-chart__point {
  position: absolute;
  width: 14px;
  height: 14px;
  padding: 0;
  border: 2px solid rgb(var(--v-theme-surface));
  border-radius: 999px;
  background: currentColor;
  box-shadow: 0 0 0 3px rgba(var(--v-theme-surface), 0.58);
  cursor: pointer;
  transform: translate(-50%, -50%);
  transition:
    box-shadow $transition-fast,
    transform $transition-fast;

  &:hover,
  &:focus-visible {
    box-shadow:
      0 0 0 4px rgba(var(--v-theme-surface), 0.8),
      0 0 0 8px rgba(var(--v-theme-primary), 0.14);
    outline: none;
    transform: translate(-50%, -50%) scale(1.12);
  }
}

.line-chart__axis {
  display: grid;
  grid-template-columns: repeat(var(--line-chart-label-count, 1), minmax(0, 1fr));
  min-width: 0;
  color: rgba(var(--v-theme-on-surface), 0.54);
  font-size: 0.68rem;
  text-align: center;
}

.line-chart__axis span {
  overflow: hidden;
  text-overflow: clip;
  white-space: nowrap;
}

@media (max-width: 1280px) {
  .panel-head {
    flex-direction: column;
  }

  .chart-total {
    justify-items: start;
  }
}
</style>
