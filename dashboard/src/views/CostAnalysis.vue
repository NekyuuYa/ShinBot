<template>
  <v-container fluid class="pa-0 cost-analysis-page">
    <app-page-header
      :title="$t('pages.costAnalysis.title')"
      :subtitle="$t('pages.costAnalysis.subtitle')"
      :kicker="$t('pages.costAnalysis.kicker')"
    >
      <template #actions>
        <v-btn-toggle
          v-model="activeWindow"
          mandatory
          density="comfortable"
          class="window-toggle"
        >
          <v-btn
            v-for="option in windowOptions"
            :key="option.value"
            :value="option.value"
            rounded="lg"
          >
            {{ option.label }}
          </v-btn>
        </v-btn-toggle>

        <v-btn
          color="primary"
          variant="tonal"
          prepend-icon="mdi-refresh"
          rounded="lg"
          :loading="isLoading"
          @click="refreshPage"
        >
          {{ $t("pages.costAnalysis.actions.refresh") }}
        </v-btn>
      </template>
    </app-page-header>

    <v-alert v-if="error" type="error" variant="tonal" class="mb-6">
      {{ error }}
    </v-alert>

    <v-progress-linear
      v-if="isLoading"
      indeterminate
      color="primary"
      rounded
      class="mb-6"
    />

    <section class="overview-band mb-6">
      <div class="overview-head">
        <div class="overview-head__copy">
          <div class="panel-kicker">
            {{ $t("pages.costAnalysis.sections.overview.kicker") }}
          </div>
          <h2 class="panel-title">
            {{ $t("pages.costAnalysis.sections.overview.title") }}
          </h2>
          <p class="panel-subtitle">
            {{ $t("pages.costAnalysis.sections.overview.subtitle") }}
          </p>
        </div>

        <div class="panel-meta">
          <v-chip variant="tonal" color="primary" size="small">
            {{
              $t("pages.costAnalysis.labels.dailySince", {
                value: formatDateRangeStart(analysis?.since ?? null),
              })
            }}
          </v-chip>
          <v-chip variant="tonal" color="info" size="small">
            {{
              $t("pages.costAnalysis.labels.hourlySince", {
                value: formatDateTime(analysis?.hourlySince ?? null),
              })
            }}
          </v-chip>
        </div>
      </div>

      <div class="summary-strip">
        <article
          v-for="item in summaryStats"
          :key="item.key"
          class="summary-cell"
          :class="`summary-cell--${item.tone}`"
        >
          <div class="summary-cell__label">
            <v-icon :icon="item.icon" size="18" />
            <span>{{ item.label }}</span>
          </div>
          <div class="summary-cell__value">
            {{ item.value }}
          </div>
          <div class="summary-cell__meta">
            {{ item.meta }}
          </div>
        </article>
      </div>

      <div class="overview-context">
        <div
          v-for="item in overviewSignals"
          :key="item.key"
          class="context-item"
        >
          <span>{{ item.label }}</span>
          <strong>{{ item.value }}</strong>
          <small>{{ item.meta }}</small>
        </div>
      </div>
    </section>

    <div class="analysis-layout mb-6">
      <section class="analysis-panel analysis-panel--wide">
        <div class="panel-head">
          <div>
            <div class="panel-kicker">
              {{ $t("pages.costAnalysis.sections.timeline.kicker") }}
            </div>
            <h2 class="panel-title">
              {{ $t("pages.costAnalysis.sections.timeline.title") }}
            </h2>
            <p class="panel-subtitle">
              {{ $t("pages.costAnalysis.sections.timeline.subtitle") }}
            </p>
          </div>

          <div class="chart-total">
            <span>{{ $t("pages.costAnalysis.summary.totalCost") }}</span>
            <strong>{{ formatCurrency(summary.estimatedCost) }}</strong>
          </div>
        </div>

        <div
          v-if="dailyCostChart.points.length > 0"
          class="line-chart line-chart--cost"
          :style="{
            '--line-chart-label-count': String(
              Math.max(dailyBuckets.length, 1),
            ),
          }"
        >
          <div class="line-chart__plot">
            <svg
              class="line-chart__svg"
              viewBox="0 0 100 100"
              preserveAspectRatio="none"
              aria-hidden="true"
            >
              <defs>
                <linearGradient
                  id="daily-cost-area"
                  x1="0"
                  x2="0"
                  y1="0"
                  y2="1"
                >
                  <stop
                    offset="0%"
                    stop-color="currentColor"
                    stop-opacity="0.22"
                  />
                  <stop
                    offset="100%"
                    stop-color="currentColor"
                    stop-opacity="0.02"
                  />
                </linearGradient>
              </defs>
              <line
                v-for="guide in lineChartGuideYs"
                :key="`daily-guide-${guide}`"
                class="line-chart__guide"
                x1="0"
                x2="100"
                :y1="guide"
                :y2="guide"
              />
              <path class="line-chart__area" :d="dailyCostChart.areaPath" />
              <path class="line-chart__path" :d="dailyCostChart.linePath" />
            </svg>

            <v-tooltip
              v-for="point in dailyCostChart.points"
              :key="point.key"
              location="top"
            >
              <template #activator="{ props }">
                <button
                  v-bind="props"
                  type="button"
                  class="line-chart__point"
                  :aria-label="formatDate(point.bucket.bucketStart)"
                  :style="{ left: `${point.x}%`, top: `${point.y}%` }"
                />
              </template>

              <div class="tooltip-stack">
                <div class="font-weight-medium">
                  {{ formatDate(point.bucket.bucketStart) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.cost") }}:
                  {{ formatCurrency(point.bucket.estimatedCost) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.calls") }}:
                  {{ formatNumber(point.bucket.totalCalls) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.totalTokens") }}:
                  {{ formatCompactNumber(point.bucket.totalTokens) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.cacheHitRate") }}:
                  {{
                    formatPercent(
                      bucketRate(
                        point.bucket.cacheHits,
                        point.bucket.totalCalls,
                      ),
                    )
                  }}
                </div>
              </div>
            </v-tooltip>
          </div>

          <div class="line-chart__axis">
            <span
              v-for="(bucket, index) in dailyBuckets"
              :key="`daily-label-${bucket.bucketStart}`"
            >
              {{
                timelineLabel(
                  index,
                  dailyBuckets.length,
                  bucket.bucketStart,
                  "daily",
                )
              }}
            </span>
          </div>
        </div>

        <v-empty-state
          v-else
          icon="mdi-chart-line"
          :title="$t('pages.costAnalysis.emptyState.title')"
          :text="$t('pages.costAnalysis.emptyState.subtitle')"
          variant="plain"
        />
      </section>

      <section class="analysis-panel analysis-panel--rank">
        <div class="panel-head panel-head--compact">
          <div>
            <div class="panel-kicker">
              {{ $t("pages.costAnalysis.sections.leaderboard.kicker") }}
            </div>
            <h2 class="panel-title">
              {{ $t("pages.costAnalysis.sections.leaderboard.title") }}
            </h2>
            <p class="panel-subtitle">
              {{ $t("pages.costAnalysis.sections.leaderboard.subtitle") }}
            </p>
          </div>
        </div>

        <div v-if="topCostModels.length > 0" class="rank-list">
          <article
            v-for="(model, index) in topCostModels"
            :key="modelKey(model)"
            class="rank-row"
          >
            <div class="rank-row__index">{{ index + 1 }}</div>
            <div class="rank-row__body">
              <div class="rank-row__top">
                <div class="rank-row__name">
                  <strong>{{ modelName(model) }}</strong>
                  <span>{{ providerName(model) }}</span>
                </div>
                <div class="rank-row__value">
                  {{ formatCurrency(model.estimatedCost) }}
                </div>
              </div>
              <div class="rank-row__meta">
                {{
                  $t("pages.costAnalysis.labels.modelRankMeta", {
                    calls: formatCompactNumber(model.totalCalls),
                    tokens: formatCompactNumber(model.totalTokens),
                    share: formatPercent(model.costShare),
                  })
                }}
              </div>
              <div class="share-track">
                <span :style="{ width: shareWidth(model.costShare) }" />
              </div>
            </div>
          </article>
        </div>

        <v-empty-state
          v-else
          icon="mdi-chart-box-outline"
          :title="$t('pages.costAnalysis.emptyState.title')"
          :text="$t('pages.costAnalysis.emptyState.subtitle')"
          variant="plain"
        />
      </section>
    </div>

    <div class="analysis-layout analysis-layout--secondary mb-6">
      <section class="analysis-panel">
        <div class="panel-head">
          <div>
            <div class="panel-kicker">
              {{ $t("pages.costAnalysis.sections.hourly.kicker") }}
            </div>
            <h2 class="panel-title">
              {{ $t("pages.costAnalysis.sections.hourly.title") }}
            </h2>
            <p class="panel-subtitle">
              {{ $t("pages.costAnalysis.sections.hourly.subtitle") }}
            </p>
          </div>

          <div class="chart-total">
            <span>{{ $t("pages.costAnalysis.table.totalTokens") }}</span>
            <strong>{{ formatCompactNumber(hourlyTokenTotal) }}</strong>
          </div>
        </div>

        <div
          v-if="hourlyTokenChart.points.length > 0"
          class="line-chart line-chart--tokens line-chart--hourly"
          :style="{
            '--line-chart-label-count': String(
              Math.max(hourlyBuckets.length, 1),
            ),
          }"
        >
          <div class="line-chart__plot">
            <svg
              class="line-chart__svg"
              viewBox="0 0 100 100"
              preserveAspectRatio="none"
              aria-hidden="true"
            >
              <defs>
                <linearGradient
                  id="hourly-token-area"
                  x1="0"
                  x2="0"
                  y1="0"
                  y2="1"
                >
                  <stop
                    offset="0%"
                    stop-color="currentColor"
                    stop-opacity="0.2"
                  />
                  <stop
                    offset="100%"
                    stop-color="currentColor"
                    stop-opacity="0.02"
                  />
                </linearGradient>
              </defs>
              <line
                v-for="guide in lineChartGuideYs"
                :key="`hourly-guide-${guide}`"
                class="line-chart__guide"
                x1="0"
                x2="100"
                :y1="guide"
                :y2="guide"
              />
              <path class="line-chart__area" :d="hourlyTokenChart.areaPath" />
              <path class="line-chart__path" :d="hourlyTokenChart.linePath" />
            </svg>

            <v-tooltip
              v-for="point in hourlyTokenChart.points"
              :key="point.key"
              location="top"
            >
              <template #activator="{ props }">
                <button
                  v-bind="props"
                  type="button"
                  class="line-chart__point"
                  :aria-label="formatDateTime(point.bucket.bucketStart)"
                  :style="{ left: `${point.x}%`, top: `${point.y}%` }"
                />
              </template>

              <div class="tooltip-stack">
                <div class="font-weight-medium">
                  {{ formatDateTime(point.bucket.bucketStart) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.totalTokens") }}:
                  {{ formatCompactNumber(point.bucket.totalTokens) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.calls") }}:
                  {{ formatNumber(point.bucket.totalCalls) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.cacheWrites") }}:
                  {{ formatCompactNumber(point.bucket.cacheWriteTokens) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.cost") }}:
                  {{ formatCurrency(point.bucket.estimatedCost) }}
                </div>
              </div>
            </v-tooltip>
          </div>

          <div class="line-chart__axis">
            <span
              v-for="(bucket, index) in hourlyBuckets"
              :key="`hourly-label-${bucket.bucketStart}`"
            >
              {{
                timelineLabel(
                  index,
                  hourlyBuckets.length,
                  bucket.bucketStart,
                  "hourly",
                )
              }}
            </span>
          </div>
        </div>

        <v-empty-state
          v-else
          icon="mdi-chart-line"
          :title="$t('pages.costAnalysis.emptyState.title')"
          :text="$t('pages.costAnalysis.emptyState.subtitle')"
          variant="plain"
        />
      </section>

      <section class="analysis-panel">
        <div class="panel-head panel-head--compact">
          <div>
            <div class="panel-kicker">
              {{ $t("pages.costAnalysis.sections.providers.kicker") }}
            </div>
            <h2 class="panel-title">
              {{ $t("pages.costAnalysis.sections.providers.title") }}
            </h2>
            <p class="panel-subtitle">
              {{ $t("pages.costAnalysis.sections.providers.subtitle") }}
            </p>
          </div>
        </div>

        <div v-if="providerRows.length > 0" class="provider-list">
          <article
            v-for="provider in providerRows"
            :key="provider.key"
            class="provider-row"
          >
            <div class="provider-row__head">
              <div class="provider-row__name">
                <strong>{{ provider.name }}</strong>
                <span>{{
                  $t("pages.costAnalysis.labels.providerMeta", {
                    models: provider.modelCount,
                    calls: formatCompactNumber(provider.totalCalls),
                  })
                }}</span>
              </div>
              <div class="provider-row__value">
                {{ formatCurrency(provider.estimatedCost) }}
              </div>
            </div>
            <div class="provider-row__metrics">
              <span
                >{{ $t("pages.costAnalysis.table.totalTokens") }}
                {{ formatCompactNumber(provider.totalTokens) }}</span
              >
              <span
                >{{ $t("pages.costAnalysis.table.cacheHitRate") }}
                {{ formatPercent(provider.cacheHitRate) }}</span
              >
              <span>{{ formatPercent(provider.costShare) }}</span>
            </div>
            <div class="share-track share-track--provider">
              <span :style="{ width: shareWidth(provider.costShare) }" />
            </div>
          </article>
        </div>

        <v-empty-state
          v-else
          icon="mdi-cloud-outline"
          :title="$t('pages.costAnalysis.emptyState.title')"
          :text="$t('pages.costAnalysis.emptyState.subtitle')"
          variant="plain"
        />
      </section>
    </div>

    <section class="analysis-panel mb-6">
      <div class="panel-head panel-head--dense">
        <div>
          <div class="panel-kicker">
            {{ $t("pages.costAnalysis.sections.focus.kicker") }}
          </div>
          <h2 class="panel-title">
            {{ $t("pages.costAnalysis.sections.focus.title") }}
          </h2>
          <p class="panel-subtitle">
            {{ $t("pages.costAnalysis.sections.focus.subtitle") }}
          </p>
        </div>

        <div class="focus-toolbar">
          <v-btn-toggle
            v-model="focusGranularity"
            mandatory
            density="comfortable"
            class="focus-toggle"
          >
            <v-btn value="daily" rounded="lg">
              {{ $t("pages.costAnalysis.actions.daily") }}
            </v-btn>
            <v-btn value="hourly" rounded="lg">
              {{ $t("pages.costAnalysis.actions.hourly") }}
            </v-btn>
          </v-btn-toggle>

          <v-btn-toggle
            v-model="focusMetric"
            mandatory
            density="comfortable"
            class="focus-toggle"
          >
            <v-btn
              v-for="metric in focusMetricOptions"
              :key="metric.value"
              :value="metric.value"
              rounded="lg"
            >
              {{ metric.label }}
            </v-btn>
          </v-btn-toggle>
        </div>
      </div>

      <div v-if="focusModels.length > 0" class="heatmap-shell">
        <div
          class="heatmap-grid"
          :style="{
            '--heatmap-columns': String(Math.max(focusBuckets.length, 1)),
          }"
        >
          <div class="heatmap-corner sticky-col">
            <div class="font-weight-medium">
              {{ $t("pages.costAnalysis.table.model") }}
            </div>
            <div class="text-caption text-medium-emphasis">
              {{
                $t("pages.costAnalysis.labels.focusModels", {
                  count: focusModels.length,
                })
              }}
            </div>
          </div>

          <div
            v-for="bucket in focusBuckets"
            :key="`header-${bucket.bucketStart}`"
            class="heatmap-header"
          >
            {{ formatBucketHeader(bucket.bucketStart, focusGranularity) }}
          </div>

          <template v-for="model in focusModels" :key="modelKey(model)">
            <div class="heatmap-model sticky-col">
              <div class="heatmap-model__title">{{ modelName(model) }}</div>
              <div class="heatmap-model__meta">
                {{ providerName(model) }}
              </div>
            </div>

            <v-tooltip
              v-for="bucket in model[focusGranularity]"
              :key="`${modelKey(model)}-${bucket.bucketStart}`"
              location="top"
            >
              <template #activator="{ props }">
                <div
                  v-bind="props"
                  class="heatmap-cell"
                  :class="`heatmap-cell--${focusMetric}`"
                  :style="{
                    '--cell-intensity': String(bucketIntensity(bucket)),
                  }"
                >
                  <span>{{ formatHeatmapValue(bucket) }}</span>
                </div>
              </template>

              <div class="tooltip-stack">
                <div class="font-weight-medium">{{ modelName(model) }}</div>
                <div>{{ formatDateTime(bucket.bucketStart) }}</div>
                <div>
                  {{ $t("pages.costAnalysis.table.calls") }}:
                  {{ formatNumber(bucket.totalCalls) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.totalTokens") }}:
                  {{ formatCompactNumber(bucket.totalTokens) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.cost") }}:
                  {{ formatCurrency(bucket.estimatedCost) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.cacheReads") }}:
                  {{ formatCompactNumber(bucket.cacheReadTokens) }}
                </div>
                <div>
                  {{ $t("pages.costAnalysis.table.cacheWrites") }}:
                  {{ formatCompactNumber(bucket.cacheWriteTokens) }}
                </div>
              </div>
            </v-tooltip>
          </template>
        </div>
      </div>

      <v-empty-state
        v-else
        icon="mdi-chart-box-outline"
        :title="$t('pages.costAnalysis.emptyState.title')"
        :text="$t('pages.costAnalysis.emptyState.subtitle')"
        variant="plain"
      />
    </section>

    <section class="analysis-panel">
      <div class="panel-head panel-head--dense">
        <div>
          <div class="panel-kicker">
            {{ $t("pages.costAnalysis.sections.table.kicker") }}
          </div>
          <h2 class="panel-title">
            {{ $t("pages.costAnalysis.sections.table.title") }}
          </h2>
          <p class="panel-subtitle">
            {{ $t("pages.costAnalysis.sections.table.subtitle") }}
          </p>
        </div>

        <div class="panel-meta">
          <v-chip variant="tonal" color="secondary" size="small">
            {{
              $t("pages.costAnalysis.labels.modelCount", {
                count: allModels.length,
              })
            }}
          </v-chip>
        </div>
      </div>

      <div v-if="allModels.length > 0" class="table-shell">
        <v-table
          fixed-header
          height="480"
          density="compact"
          class="analysis-table"
        >
          <thead>
            <tr>
              <th>{{ $t("pages.costAnalysis.table.model") }}</th>
              <th>{{ $t("pages.costAnalysis.table.cost") }}</th>
              <th>{{ $t("pages.costAnalysis.table.calls") }}</th>
              <th>{{ $t("pages.costAnalysis.table.successRate") }}</th>
              <th>{{ $t("pages.costAnalysis.table.cacheHitRate") }}</th>
              <th>{{ $t("pages.costAnalysis.table.inputTokens") }}</th>
              <th>{{ $t("pages.costAnalysis.table.outputTokens") }}</th>
              <th>{{ $t("pages.costAnalysis.table.cacheReads") }}</th>
              <th>{{ $t("pages.costAnalysis.table.cacheWrites") }}</th>
              <th>{{ $t("pages.costAnalysis.table.avgLatency") }}</th>
              <th>{{ $t("pages.costAnalysis.table.lastSeen") }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="model in allModels" :key="modelKey(model)">
              <td>
                <div class="model-cell">
                  <div class="model-cell__title">{{ modelName(model) }}</div>
                  <div class="model-cell__meta">{{ providerName(model) }}</div>
                </div>
              </td>
              <td>{{ formatCurrency(model.estimatedCost) }}</td>
              <td>{{ formatNumber(model.totalCalls) }}</td>
              <td>
                <v-chip size="small" variant="tonal" color="info">
                  {{ formatPercent(model.successRate) }}
                </v-chip>
              </td>
              <td>
                <v-chip size="small" variant="tonal" color="success">
                  {{ formatPercent(model.cacheHitRate) }}
                </v-chip>
              </td>
              <td>{{ formatCompactNumber(model.inputTokens) }}</td>
              <td>{{ formatCompactNumber(model.outputTokens) }}</td>
              <td>{{ formatCompactNumber(model.cacheReadTokens) }}</td>
              <td>{{ formatCompactNumber(model.cacheWriteTokens) }}</td>
              <td>{{ formatDuration(model.averageLatencyMs) }}</td>
              <td>{{ formatDateTime(model.lastSeenAt) }}</td>
            </tr>
          </tbody>
        </v-table>
      </div>

      <v-empty-state
        v-else
        icon="mdi-table-search"
        :title="$t('pages.costAnalysis.emptyState.title')"
        :text="$t('pages.costAnalysis.emptyState.subtitle')"
        variant="plain"
      />
    </section>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { storeToRefs } from "pinia";
import { useI18n } from "vue-i18n";

import AppPageHeader from "@/components/AppPageHeader.vue";
import { useFormatters } from "@/composables/useFormatters";
import {
  COST_ANALYSIS_WINDOWS,
  type CostAnalysisWindow,
  useCostAnalysisStore,
} from "@/stores/costAnalysis";
import { useSystemSettingsStore } from "@/stores/systemSettings";
import type { CostAnalysisBucket, CostAnalysisModel } from "@/api/modelRuntime";

type FocusGranularity = "daily" | "hourly";
type FocusMetric = "tokens" | "cost" | "calls" | "cache";
type SummaryTone = "primary" | "warning" | "info" | "success" | "secondary";

interface ProviderCostRow {
  key: string;
  name: string;
  modelCount: number;
  totalCalls: number;
  totalTokens: number;
  cacheHits: number;
  estimatedCost: number;
  costShare: number;
  cacheHitRate: number;
}

interface RankedCostModel extends CostAnalysisModel {
  costShare: number;
}

interface LineChartPoint {
  key: string;
  x: number;
  y: number;
  bucket: CostAnalysisBucket;
}

interface LineChartSeries {
  points: LineChartPoint[];
  linePath: string;
  areaPath: string;
}

const LINE_CHART_TOP = 10;
const LINE_CHART_BOTTOM = 88;
const lineChartGuideYs = [10, 29.5, 49, 68.5, 88];

const costAnalysisStore = useCostAnalysisStore();
const systemSettingsStore = useSystemSettingsStore();
const { analysis, error, isLoading, selectedDays } =
  storeToRefs(costAnalysisStore);
const { locale, t } = useI18n();

const focusGranularity = ref<FocusGranularity>("daily");
const focusMetric = ref<FocusMetric>("tokens");
const displayCurrency = computed(
  () =>
    systemSettingsStore.pricingCurrency || analysis.value?.currency || "CNY",
);

const {
  formatNumber,
  formatCompactNumber,
  formatCurrency,
  formatPercent,
  formatDate,
  formatDateRangeStart,
  formatDateTime,
  formatHour,
  formatShortDate,
  formatDuration,
} = useFormatters(locale, displayCurrency);

const activeWindow = computed<CostAnalysisWindow>({
  get: () => selectedDays.value,
  set: (value) => {
    if (!value || value === selectedDays.value) return;
    costAnalysisStore.setSelectedDays(value);
    void costAnalysisStore.fetchAnalysis(value);
  },
});

const windowOptions = computed(() =>
  COST_ANALYSIS_WINDOWS.map((value) => ({
    value,
    label: t("pages.costAnalysis.windowOption", { days: value }),
  })),
);

const focusMetricOptions = computed(() => [
  { value: "tokens" as const, label: t("pages.costAnalysis.metrics.tokens") },
  { value: "cost" as const, label: t("pages.costAnalysis.metrics.cost") },
  { value: "calls" as const, label: t("pages.costAnalysis.metrics.calls") },
  { value: "cache" as const, label: t("pages.costAnalysis.metrics.cache") },
]);

const summary = computed(
  () =>
    analysis.value?.summary ?? {
      totalCalls: 0,
      successfulCalls: 0,
      failedCalls: 0,
      successRate: 0,
      cacheHits: 0,
      cacheHitRate: 0,
      inputTokens: 0,
      outputTokens: 0,
      totalTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
      estimatedCost: 0,
      averageLatencyMs: null,
      averageTimeToFirstTokenMs: null,
    },
);

const dailyBuckets = computed(() => analysis.value?.timeline.daily ?? []);
const hourlyBuckets = computed(() => analysis.value?.timeline.hourly ?? []);
const focusModels = computed(() => analysis.value?.focusModels ?? []);
const allModels = computed(() => analysis.value?.models ?? []);

const focusBuckets = computed(() => {
  const firstModel = focusModels.value[0];
  return firstModel ? firstModel[focusGranularity.value] : [];
});

const dailyCostMax = computed(() =>
  Math.max(...dailyBuckets.value.map((b) => b.estimatedCost), 0.01),
);
const hourlyTokenMax = computed(() =>
  Math.max(...hourlyBuckets.value.map((b) => b.totalTokens), 1),
);
const dailyCostChart = computed(() =>
  createLineChartSeries(
    dailyBuckets.value,
    (bucket) => bucket.estimatedCost,
    dailyCostMax.value,
  ),
);
const hourlyTokenChart = computed(() =>
  createLineChartSeries(
    hourlyBuckets.value,
    (bucket) => bucket.totalTokens,
    hourlyTokenMax.value,
  ),
);
const hourlyTokenTotal = computed(() =>
  hourlyBuckets.value.reduce((s, b) => s + b.totalTokens, 0),
);
const averageCostPerCall = computed(() =>
  summary.value.totalCalls > 0
    ? summary.value.estimatedCost / summary.value.totalCalls
    : 0,
);
const averageTokensPerCall = computed(() =>
  summary.value.totalCalls > 0
    ? summary.value.totalTokens / summary.value.totalCalls
    : 0,
);

const peakDailyBucket = computed(() =>
  dailyBuckets.value.reduce<CostAnalysisBucket | null>(
    (best, bucket) =>
      !best || bucket.estimatedCost > best.estimatedCost ? bucket : best,
    null,
  ),
);

const providerRows = computed<ProviderCostRow[]>(() => {
  const providers = new Map<
    string,
    Omit<ProviderCostRow, "costShare" | "cacheHitRate">
  >();

  for (const model of allModels.value) {
    const key = model.providerId || "unknown";
    const existing = providers.get(key) ?? {
      key,
      name: providerName(model),
      modelCount: 0,
      totalCalls: 0,
      totalTokens: 0,
      cacheHits: 0,
      estimatedCost: 0,
    };

    existing.modelCount += 1;
    existing.totalCalls += model.totalCalls;
    existing.totalTokens += model.totalTokens;
    existing.cacheHits += model.cacheHits;
    existing.estimatedCost += model.estimatedCost;
    providers.set(key, existing);
  }

  const totalCost = summary.value.estimatedCost;
  return Array.from(providers.values())
    .sort((a, b) => b.estimatedCost - a.estimatedCost)
    .map((row) => ({
      ...row,
      costShare: totalCost > 0 ? row.estimatedCost / totalCost : 0,
      cacheHitRate: row.totalCalls > 0 ? row.cacheHits / row.totalCalls : 0,
    }));
});

const topCostModels = computed<RankedCostModel[]>(() => {
  const totalCost = summary.value.estimatedCost;
  return [...allModels.value]
    .sort((a, b) => b.estimatedCost - a.estimatedCost)
    .slice(0, 6)
    .map((model) => ({
      ...model,
      costShare: totalCost > 0 ? model.estimatedCost / totalCost : 0,
    }));
});

const heatmapMetricMax = computed(() => {
  if (focusMetric.value === "cache") return 1;
  const values = focusModels.value.flatMap((m) =>
    m[focusGranularity.value].map(metricValue),
  );
  return Math.max(...values, 1);
});

const summaryStats = computed(() => [
  {
    key: "cost",
    tone: "warning" as SummaryTone,
    icon: "mdi-cash-multiple",
    label: t("pages.costAnalysis.summary.totalCost"),
    value: formatCurrency(summary.value.estimatedCost),
    meta: t("pages.costAnalysis.summary.costPerCall", {
      value: formatCurrency(averageCostPerCall.value),
    }),
  },
  {
    key: "calls",
    tone: "info" as SummaryTone,
    icon: "mdi-phone-in-talk-outline",
    label: t("pages.costAnalysis.summary.totalCalls"),
    value: formatCompactNumber(summary.value.totalCalls),
    meta: t("pages.costAnalysis.summary.successFailed", {
      rate: formatPercent(summary.value.successRate),
      failed: formatCompactNumber(summary.value.failedCalls),
    }),
  },
  {
    key: "tokens",
    tone: "primary" as SummaryTone,
    icon: "mdi-counter",
    label: t("pages.costAnalysis.summary.totalTokens"),
    value: formatCompactNumber(summary.value.totalTokens),
    meta: t("pages.costAnalysis.summary.tokensPerCall", {
      value: formatCompactNumber(averageTokensPerCall.value),
    }),
  },
  {
    key: "cache",
    tone: "success" as SummaryTone,
    icon: "mdi-database-sync-outline",
    label: t("pages.costAnalysis.summary.cacheHitRate"),
    value: formatPercent(summary.value.cacheHitRate),
    meta: t("pages.costAnalysis.summary.cacheReadWrite", {
      read: formatCompactNumber(summary.value.cacheReadTokens),
      write: formatCompactNumber(summary.value.cacheWriteTokens),
    }),
  },
  {
    key: "latency",
    tone: "secondary" as SummaryTone,
    icon: "mdi-timer-sand",
    label: t("pages.costAnalysis.summary.avgLatency"),
    value: formatDuration(summary.value.averageLatencyMs),
    meta: t("pages.costAnalysis.summary.ttft", {
      value: formatDuration(summary.value.averageTimeToFirstTokenMs),
    }),
  },
]);

const overviewSignals = computed(() => [
  {
    key: "models",
    label: t("pages.costAnalysis.labels.activeModels"),
    value: formatNumber(allModels.value.length),
    meta: t("pages.costAnalysis.labels.focusModels", {
      count: focusModels.value.length,
    }),
  },
  {
    key: "providers",
    label: t("pages.costAnalysis.labels.providerCount"),
    value: formatNumber(providerRows.value.length),
    meta: providerRows.value[0]
      ? t("pages.costAnalysis.labels.topProvider", {
          name: providerRows.value[0].name,
          value: formatCurrency(providerRows.value[0].estimatedCost),
        })
      : t("pages.costAnalysis.labels.noData"),
  },
  {
    key: "peak",
    label: t("pages.costAnalysis.labels.peakCostDay"),
    value: peakDailyBucket.value
      ? formatDate(peakDailyBucket.value.bucketStart)
      : t("pages.costAnalysis.labels.noData"),
    meta: peakDailyBucket.value
      ? formatCurrency(peakDailyBucket.value.estimatedCost)
      : t("pages.costAnalysis.labels.noData"),
  },
]);

const refreshPage = () =>
  void costAnalysisStore.fetchAnalysis(selectedDays.value, { force: true });
const bucketRate = (h: number, t: number) => (t > 0 ? h / t : 0);
const shareWidth = (share: number) => {
  if (share <= 0) return "0%";
  return `${Math.min(Math.max(share * 100, 4), 100)}%`;
};
const clampChartValue = (value: number, min = 0, max = 100) =>
  Math.min(Math.max(value, min), max);
const chartNumber = (value: number) => Number(value.toFixed(3));
const chartY = (value: number, max: number) => {
  if (value <= 0 || max <= 0) return LINE_CHART_BOTTOM;

  const ratio = Math.min(value / max, 1);
  return chartNumber(
    LINE_CHART_BOTTOM - ratio * (LINE_CHART_BOTTOM - LINE_CHART_TOP),
  );
};
const createSmoothLinePath = (points: LineChartPoint[]) => {
  if (points.length === 0) return "";
  if (points.length === 1) {
    return `M 0 ${points[0].y} L 100 ${points[0].y}`;
  }
  if (points.length === 2) {
    return `M ${points[0].x} ${points[0].y} L ${points[1].x} ${points[1].y}`;
  }

  let path = `M ${points[0].x} ${points[0].y}`;
  for (let i = 0; i < points.length - 1; i += 1) {
    const previous = points[i - 1] ?? points[i];
    const current = points[i];
    const next = points[i + 1];
    const afterNext = points[i + 2] ?? next;
    const controlStartX = current.x + (next.x - previous.x) / 6;
    const controlStartY = current.y + (next.y - previous.y) / 6;
    const controlEndX = next.x - (afterNext.x - current.x) / 6;
    const controlEndY = next.y - (afterNext.y - current.y) / 6;

    path += ` C ${chartNumber(clampChartValue(controlStartX))} ${chartNumber(
      clampChartValue(controlStartY, LINE_CHART_TOP, LINE_CHART_BOTTOM),
    )}, ${chartNumber(clampChartValue(controlEndX))} ${chartNumber(
      clampChartValue(controlEndY, LINE_CHART_TOP, LINE_CHART_BOTTOM),
    )}, ${next.x} ${next.y}`;
  }

  return path;
};
const createLineChartSeries = (
  buckets: CostAnalysisBucket[],
  valueOf: (bucket: CostAnalysisBucket) => number,
  max: number,
): LineChartSeries => {
  const points = buckets.map((bucket, index) => ({
    key: bucket.bucketStart,
    x:
      buckets.length === 1
        ? 50
        : chartNumber((index / Math.max(buckets.length - 1, 1)) * 100),
    y: chartY(valueOf(bucket), max),
    bucket,
  }));
  const linePath = createSmoothLinePath(points);
  const firstX = points.length === 1 ? 0 : (points[0]?.x ?? 0);
  const lastX = points.length === 1 ? 100 : (points.at(-1)?.x ?? 100);

  return {
    points,
    linePath,
    areaPath: linePath
      ? `${linePath} L ${lastX} ${LINE_CHART_BOTTOM} L ${firstX} ${LINE_CHART_BOTTOM} Z`
      : "",
  };
};
const metricValue = (b: CostAnalysisBucket) => {
  if (focusMetric.value === "cost") return b.estimatedCost;
  if (focusMetric.value === "calls") return b.totalCalls;
  if (focusMetric.value === "cache")
    return bucketRate(b.cacheHits, b.totalCalls);
  return b.totalTokens;
};
const bucketIntensity = (b: CostAnalysisBucket) => {
  const v = metricValue(b);
  const m = heatmapMetricMax.value;
  return !v || m <= 0 ? 0.06 : Math.min(0.16 + (v / m) * 0.74, 0.9);
};
const formatHeatmapValue = (b: CostAnalysisBucket) => {
  if (focusMetric.value === "cost") return formatCurrency(b.estimatedCost);
  if (focusMetric.value === "calls") return formatNumber(b.totalCalls);
  if (focusMetric.value === "cache")
    return formatPercent(bucketRate(b.cacheHits, b.totalCalls));
  return formatCompactNumber(b.totalTokens);
};
const modelKey = (model: Pick<CostAnalysisModel, "providerId" | "modelId">) =>
  `${model.providerId}:${model.modelId}`;
const modelName = (
  model: Pick<CostAnalysisModel, "modelDisplayName" | "modelId">,
) => model.modelDisplayName || model.modelId;
const providerName = (
  model: Pick<CostAnalysisModel, "providerDisplayName" | "providerId">,
) => model.providerDisplayName || model.providerId;

const timelineLabel = (
  index: number,
  length: number,
  value: string,
  granularity: FocusGranularity,
) => {
  if (granularity === "hourly") {
    return index % 4 !== 0 && index !== length - 1 ? "" : formatHour(value);
  }
  const divider = length > 45 ? 9 : length > 21 ? 5 : 1;
  return index % divider !== 0 && index !== length - 1
    ? ""
    : formatShortDate(value);
};

const formatBucketHeader = (value: string, granularity: FocusGranularity) =>
  granularity === "hourly" ? formatHour(value) : formatShortDate(value);

onMounted(() => {
  void costAnalysisStore.fetchAnalysis(selectedDays.value);
});
</script>

<style scoped lang="scss">
@use "@/styles/mixins" as *;

.window-toggle,
.focus-toggle {
  padding: 2px;
  overflow: visible;
  border: 1px solid $border-color-soft;
  border-radius: $radius-base;
  background: rgba(var(--v-theme-surface), 0.72);
}

.window-toggle :deep(.v-btn),
.focus-toggle :deep(.v-btn) {
  min-width: 64px;
  border-radius: 14px;
  margin: 2px;
}

.overview-band {
  @include surface-card;
  padding: 24px;
}

.overview-head,
.panel-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 20px;
  margin-bottom: 22px;

  &--dense {
    margin-bottom: 18px;
  }

  &--compact {
    margin-bottom: 14px;
  }
}

.overview-head__copy {
  min-width: 0;
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

.panel-meta,
.focus-toolbar {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  flex-wrap: wrap;
}

.summary-strip {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  overflow: hidden;
  border: 1px solid $border-color-soft;
  border-radius: $radius-sm;
  background: rgba(var(--v-theme-on-surface), 0.018);
}

.summary-cell {
  min-width: 0;
  padding: 16px;
  border-inline-end: 1px solid $border-color-soft;

  &:last-child {
    border-inline-end: 0;
  }

  &__label {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    color: rgba(var(--v-theme-on-surface), 0.62);
    font-size: $font-size-xs;
    font-weight: 800;
    letter-spacing: 0;
    text-transform: uppercase;

    span {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
  }

  &__value {
    margin-top: 12px;
    overflow: hidden;
    color: rgba(var(--v-theme-on-surface), 0.94);
    font-size: 1.45rem;
    font-weight: 850;
    line-height: 1.05;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  &__meta {
    margin-top: 8px;
    overflow: hidden;
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: $font-size-xs;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
}

.summary-cell--primary .v-icon {
  color: rgb(var(--v-theme-primary));
}
.summary-cell--warning .v-icon {
  color: rgb(var(--v-theme-warning));
}
.summary-cell--info .v-icon {
  color: rgb(var(--v-theme-info));
}
.summary-cell--success .v-icon {
  color: rgb(var(--v-theme-success));
}
.summary-cell--secondary .v-icon {
  color: rgb(var(--v-theme-secondary));
}

.overview-context {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 18px;
  margin-top: 18px;
  padding-top: 18px;
  border-top: 1px solid $border-color-soft;
}

.context-item {
  display: grid;
  gap: 4px;
  min-width: 0;

  span,
  small {
    overflow: hidden;
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: $font-size-xs;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  strong {
    overflow: hidden;
    color: rgba(var(--v-theme-on-surface), 0.9);
    font-size: 1rem;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
}

.analysis-layout {
  display: grid;
  grid-template-columns: minmax(0, 1.45fr) minmax(320px, 0.78fr);
  align-items: stretch;
  gap: 16px;

  &--secondary {
    grid-template-columns: minmax(0, 1fr) minmax(320px, 0.86fr);
  }
}

.analysis-panel {
  @include analysis-section-panel;
  min-width: 0;
}

.analysis-panel--wide,
.analysis-panel--rank {
  min-height: 390px;
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
  fill: url("#daily-cost-area");
}

.line-chart--tokens .line-chart__area {
  fill: url("#hourly-token-area");
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

.line-chart--cost .line-chart__point:hover,
.line-chart--cost .line-chart__point:focus-visible {
  box-shadow:
    0 0 0 4px rgba(var(--v-theme-surface), 0.8),
    0 0 0 8px rgba(var(--v-theme-warning), 0.16);
}

.line-chart__axis {
  display: grid;
  grid-template-columns: repeat(
    var(--line-chart-label-count, 1),
    minmax(0, 1fr)
  );
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

.rank-list,
.provider-list {
  display: grid;
}

.rank-row {
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr);
  gap: 12px;
  padding: 14px 0;
  border-bottom: 1px solid $border-color-soft;

  &:last-child {
    border-bottom: 0;
  }

  &__index {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 28px;
    height: 28px;
    border-radius: 999px;
    color: rgb(var(--v-theme-primary));
    background: rgba(var(--v-theme-primary), 0.1);
    font-size: $font-size-xs;
    font-weight: 800;
  }

  &__body {
    min-width: 0;
  }

  &__top {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
    min-width: 0;
  }

  &__name {
    display: grid;
    gap: 3px;
    min-width: 0;

    strong,
    span {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    strong {
      color: rgba(var(--v-theme-on-surface), 0.92);
      font-size: 0.92rem;
    }

    span {
      color: rgba(var(--v-theme-on-surface), 0.58);
      font-size: $font-size-xs;
    }
  }

  &__value {
    flex: 0 0 auto;
    color: rgba(var(--v-theme-on-surface), 0.9);
    font-size: 0.92rem;
    font-weight: 800;
    white-space: nowrap;
  }

  &__meta {
    margin-top: 8px;
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: $font-size-xs;
  }
}

.provider-row {
  padding: 14px 0;
  border-bottom: 1px solid $border-color-soft;

  &:last-child {
    border-bottom: 0;
  }

  &__head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
  }

  &__name {
    display: grid;
    gap: 3px;
    min-width: 0;

    strong,
    span {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    strong {
      color: rgba(var(--v-theme-on-surface), 0.92);
      font-size: 0.94rem;
    }

    span {
      color: rgba(var(--v-theme-on-surface), 0.58);
      font-size: $font-size-xs;
    }
  }

  &__value {
    flex: 0 0 auto;
    color: rgba(var(--v-theme-on-surface), 0.9);
    font-weight: 800;
    white-space: nowrap;
  }

  &__metrics {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    margin-top: 10px;
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: $font-size-xs;
  }
}

.share-track {
  width: 100%;
  height: 6px;
  overflow: hidden;
  margin-top: 10px;
  border-radius: 999px;
  background: rgba(var(--v-theme-on-surface), 0.06);

  span {
    display: block;
    height: 100%;
    border-radius: inherit;
    background: rgb(var(--v-theme-warning));
  }

  &--provider span {
    background: rgb(var(--v-theme-primary));
  }
}

.heatmap-shell {
  overflow-x: auto;
  padding-bottom: 4px;
}

.heatmap-grid {
  @include heatmap-grid-layout(230px, 64px);
  gap: 6px;
}

.heatmap-header,
.heatmap-cell {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 44px;
  border: 1px solid $border-color-soft;
  border-radius: $radius-xs;
  text-align: center;
}

.heatmap-header {
  color: rgba(var(--v-theme-on-surface), 0.62);
  font-size: 0.7rem;
  font-weight: 800;
  background: rgba(var(--v-theme-on-surface), 0.02);
}

.heatmap-model {
  display: grid;
  align-content: center;
  min-height: 48px;
  padding: 8px 10px;
  border-radius: $radius-xs;

  &__title,
  &__meta {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  &__title {
    color: rgba(var(--v-theme-on-surface), 0.92);
    font-size: 0.86rem;
    font-weight: 800;
  }

  &__meta {
    margin-top: 2px;
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: 0.74rem;
  }
}

.heatmap-corner {
  display: grid;
  align-content: center;
  min-height: 44px;
  padding: 8px 10px;
  border-radius: $radius-xs;
}

.heatmap-cell {
  padding: 6px;
  color: rgba(var(--v-theme-on-surface), 0.9);
  font-size: 0.68rem;
  font-weight: 800;

  span {
    overflow: hidden;
    max-width: 100%;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  &--tokens {
    background: rgba(var(--v-theme-primary), var(--cell-intensity, 0.1));
  }

  &--cost {
    background: rgba(var(--v-theme-warning), var(--cell-intensity, 0.1));
  }

  &--calls {
    background: rgba(var(--v-theme-info), var(--cell-intensity, 0.1));
  }

  &--cache {
    background: rgba(var(--v-theme-success), var(--cell-intensity, 0.1));
  }
}

.table-shell {
  overflow: auto;
  border: 1px solid $border-color-soft;
  border-radius: $radius-sm;
}

.analysis-table {
  min-width: 1120px;
  background: transparent;
}

.analysis-table :deep(th),
.analysis-table :deep(td) {
  white-space: nowrap;
}

.analysis-table :deep(th:not(:first-child)),
.analysis-table :deep(td:not(:first-child)) {
  text-align: right;
}

.analysis-table :deep(thead th) {
  background: rgb(var(--v-theme-surface));
  color: rgba(var(--v-theme-on-surface), 0.62);
  font-size: 0.76rem;
  font-weight: 800;
}

.analysis-table :deep(tbody tr:hover) {
  background: rgba(var(--v-theme-primary), 0.035);
}

.model-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 220px;
  max-width: 320px;

  &__title,
  &__meta {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  &__title {
    color: rgba(var(--v-theme-on-surface), 0.94);
    font-weight: 800;
  }

  &__meta {
    color: rgba(var(--v-theme-on-surface), 0.62);
    font-size: 0.8rem;
  }
}

.tooltip-stack {
  display: grid;
  gap: 4px;
}

@include respond-to("tablet") {
  .overview-band,
  .analysis-panel {
    padding: 20px;
    border-radius: 20px;
  }

  .overview-head,
  .panel-head {
    flex-direction: column;
  }

  .panel-meta,
  .focus-toolbar {
    width: 100%;
    justify-content: flex-start;
  }

  .summary-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
    border: 0;
    gap: 10px;
    background: transparent;
  }

  .summary-cell {
    border: 1px solid $border-color-soft;
    border-radius: $radius-sm;
    background: rgba(var(--v-theme-on-surface), 0.018);
  }

  .overview-context {
    grid-template-columns: 1fr;
    gap: 12px;
  }

  .analysis-layout,
  .analysis-layout--secondary {
    grid-template-columns: minmax(0, 1fr);
  }

  .chart-total {
    justify-items: start;
  }

  .heatmap-grid {
    grid-template-columns: 220px repeat(
        var(--heatmap-columns, 1),
        minmax(62px, 1fr)
      );
  }
}

@include respond-to("mobile") {
  .summary-strip {
    grid-template-columns: 1fr;
  }

  .window-toggle,
  .focus-toggle {
    width: 100%;
  }

  .focus-toggle :deep(.v-btn),
  .window-toggle :deep(.v-btn) {
    flex: 1 1 0;
    min-width: 0;
    padding-inline: 8px;
  }
}
</style>
