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

    <cost-overview-band
      class="mb-6"
      :kicker="$t('pages.costAnalysis.sections.overview.kicker')"
      :title="$t('pages.costAnalysis.sections.overview.title')"
      :subtitle="$t('pages.costAnalysis.sections.overview.subtitle')"
      :daily-label="$t('pages.costAnalysis.labels.dailySince', { value: formatDateRangeStart(analysis?.since ?? null) })"
      :hourly-label="$t('pages.costAnalysis.labels.hourlySince', { value: formatDateTime(analysis?.hourlySince ?? null) })"
      :summary-stats="summaryStats"
      :overview-signals="overviewSignals"
    />

    <div class="analysis-layout mb-6">
      <cost-trend-chart
        :kicker="$t('pages.costAnalysis.sections.timeline.kicker')"
        :title="$t('pages.costAnalysis.sections.timeline.title')"
        :subtitle="$t('pages.costAnalysis.sections.timeline.subtitle')"
        :total-label="$t('pages.costAnalysis.summary.totalCost')"
        :total-value="formatCurrency(summary.estimatedCost)"
        panel-class="analysis-panel--wide"
        chart-class="line-chart--cost"
        area-gradient-id="daily-cost-area"
        :guide-ys="lineChartGuideYs"
        :points="dailyCostChart.points"
        :line-path="dailyCostChart.linePath"
        :area-path="dailyCostChart.areaPath"
        :axis-labels="dailyBuckets.map((bucket, index) => ({ key: `daily-label-${bucket.bucketStart}`, text: timelineLabel(index, dailyBuckets.length, bucket.bucketStart, 'daily') }))"
        :empty-title="$t('pages.costAnalysis.emptyState.title')"
        :empty-text="$t('pages.costAnalysis.emptyState.subtitle')"
      >
        <template #tooltip="{ point }">
          <div class="tooltip-stack">
            <div class="font-weight-medium">{{ formatDate(point.bucket.bucketStart) }}</div>
            <div>{{ $t('pages.costAnalysis.table.cost') }}: {{ formatCurrency(point.bucket.estimatedCost) }}</div>
            <div>{{ $t('pages.costAnalysis.table.calls') }}: {{ formatNumber(point.bucket.totalCalls) }}</div>
            <div>{{ $t('pages.costAnalysis.table.totalTokens') }}: {{ formatCompactNumber(point.bucket.totalTokens) }}</div>
            <div>{{ $t('pages.costAnalysis.table.cacheHitRate') }}: {{ formatPercent(bucketRate(point.bucket.cacheHits, point.bucket.totalCalls)) }}</div>
          </div>
        </template>
      </cost-trend-chart>

      <cost-rank-panel
        :kicker="$t('pages.costAnalysis.sections.leaderboard.kicker')"
        :title="$t('pages.costAnalysis.sections.leaderboard.title')"
        :subtitle="$t('pages.costAnalysis.sections.leaderboard.subtitle')"
        :empty-title="$t('pages.costAnalysis.emptyState.title')"
        :empty-text="$t('pages.costAnalysis.emptyState.subtitle')"
        :items="topCostModelRows"
      />
    </div>

    <div class="analysis-layout analysis-layout--secondary mb-6">
      <cost-trend-chart
        :kicker="$t('pages.costAnalysis.sections.hourly.kicker')"
        :title="$t('pages.costAnalysis.sections.hourly.title')"
        :subtitle="$t('pages.costAnalysis.sections.hourly.subtitle')"
        :total-label="$t('pages.costAnalysis.table.totalTokens')"
        :total-value="formatCompactNumber(hourlyTokenTotal)"
        panel-class="analysis-panel--compact"
        chart-class="line-chart--tokens line-chart--hourly"
        area-gradient-id="hourly-token-area"
        :guide-ys="lineChartGuideYs"
        :points="hourlyTokenChart.points"
        :line-path="hourlyTokenChart.linePath"
        :area-path="hourlyTokenChart.areaPath"
        :axis-labels="hourlyBuckets.map((bucket, index) => ({ key: `hourly-label-${bucket.bucketStart}`, text: timelineLabel(index, hourlyBuckets.length, bucket.bucketStart, 'hourly') }))"
        :empty-title="$t('pages.costAnalysis.emptyState.title')"
        :empty-text="$t('pages.costAnalysis.emptyState.subtitle')"
      >
        <template #tooltip="{ point }">
          <div class="tooltip-stack">
            <div class="font-weight-medium">{{ formatDateTime(point.bucket.bucketStart) }}</div>
            <div>{{ $t('pages.costAnalysis.table.totalTokens') }}: {{ formatCompactNumber(point.bucket.totalTokens) }}</div>
            <div>{{ $t('pages.costAnalysis.table.calls') }}: {{ formatNumber(point.bucket.totalCalls) }}</div>
            <div>{{ $t('pages.costAnalysis.table.cacheWrites') }}: {{ formatCompactNumber(point.bucket.cacheWriteTokens) }}</div>
            <div>{{ $t('pages.costAnalysis.table.cost') }}: {{ formatCurrency(point.bucket.estimatedCost) }}</div>
          </div>
        </template>
      </cost-trend-chart>

      <cost-provider-panel
        :kicker="$t('pages.costAnalysis.sections.providers.kicker')"
        :title="$t('pages.costAnalysis.sections.providers.title')"
        :subtitle="$t('pages.costAnalysis.sections.providers.subtitle')"
        :empty-title="$t('pages.costAnalysis.emptyState.title')"
        :empty-text="$t('pages.costAnalysis.emptyState.subtitle')"
        :total-tokens-label="$t('pages.costAnalysis.table.totalTokens')"
        :cache-hit-rate-label="$t('pages.costAnalysis.table.cacheHitRate')"
        :items="providerRows"
        :format-compact-number="formatCompactNumber"
        :format-currency="formatCurrency"
        :format-percent="formatPercent"
        :share-width="shareWidth"
        :provider-meta-label="providerMetaLabel"
      />
    </div>

    <cost-focus-heatmap
      :kicker="$t('pages.costAnalysis.sections.focus.kicker')"
      :title="$t('pages.costAnalysis.sections.focus.title')"
      :subtitle="$t('pages.costAnalysis.sections.focus.subtitle')"
      :daily-label="$t('pages.costAnalysis.actions.daily')"
      :hourly-label="$t('pages.costAnalysis.actions.hourly')"
      :model-label="$t('pages.costAnalysis.table.model')"
      :model-count-label="$t('pages.costAnalysis.labels.focusModels', { count: focusModels.length })"
      :calls-label="$t('pages.costAnalysis.table.calls')"
      :total-tokens-label="$t('pages.costAnalysis.table.totalTokens')"
      :cost-label="$t('pages.costAnalysis.table.cost')"
      :cache-reads-label="$t('pages.costAnalysis.table.cacheReads')"
      :cache-writes-label="$t('pages.costAnalysis.table.cacheWrites')"
      :empty-title="$t('pages.costAnalysis.emptyState.title')"
      :empty-text="$t('pages.costAnalysis.emptyState.subtitle')"
      :models="focusModels"
      :metric-options="focusMetricOptions"
      :format-bucket-header="formatBucketHeader"
      :format-date-time="formatDateTime"
      :format-number="formatNumber"
      :format-compact-number="formatCompactNumber"
      :format-currency="formatCurrency"
      :model-key="modelKey"
      :model-name="modelName"
      :provider-name="providerName"
      :bucket-intensity="bucketIntensity"
      :format-heatmap-value="formatHeatmapValue"
      v-model:granularity="focusGranularity"
      v-model:metric="focusMetric"
    />

    <cost-model-table
      class="mt-6"
      :kicker="$t('pages.costAnalysis.sections.table.kicker')"
      :title="$t('pages.costAnalysis.sections.table.title')"
      :subtitle="$t('pages.costAnalysis.sections.table.subtitle')"
      :model-count-label="$t('pages.costAnalysis.labels.modelCount', { count: allModels.length })"
      :model-label="$t('pages.costAnalysis.table.model')"
      :cost-label="$t('pages.costAnalysis.table.cost')"
      :calls-label="$t('pages.costAnalysis.table.calls')"
      :success-rate-label="$t('pages.costAnalysis.table.successRate')"
      :cache-hit-rate-label="$t('pages.costAnalysis.table.cacheHitRate')"
      :input-tokens-label="$t('pages.costAnalysis.table.inputTokens')"
      :output-tokens-label="$t('pages.costAnalysis.table.outputTokens')"
      :cache-reads-label="$t('pages.costAnalysis.table.cacheReads')"
      :cache-writes-label="$t('pages.costAnalysis.table.cacheWrites')"
      :avg-latency-label="$t('pages.costAnalysis.table.avgLatency')"
      :last-seen-label="$t('pages.costAnalysis.table.lastSeen')"
      :empty-title="$t('pages.costAnalysis.emptyState.title')"
      :empty-text="$t('pages.costAnalysis.emptyState.subtitle')"
      :items="allModels"
      :format-number="formatNumber"
      :format-compact-number="formatCompactNumber"
      :format-currency="formatCurrency"
      :format-percent="formatPercent"
      :format-duration="formatDuration"
      :format-date-time="formatDateTime"
      :model-key="modelKey"
      :model-name="modelName"
      :provider-name="providerName"
    />
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { storeToRefs } from "pinia";
import { useI18n } from "vue-i18n";

import AppPageHeader from "@/components/AppPageHeader.vue";
import CostOverviewBand from "@/components/cost-analysis/CostOverviewBand.vue";
import CostTrendChart from "@/components/cost-analysis/CostTrendChart.vue";
import CostFocusHeatmap from "@/components/cost-analysis/CostFocusHeatmap.vue";
import CostRankPanel from "@/components/cost-analysis/CostRankPanel.vue";
import CostProviderPanel from "@/components/cost-analysis/CostProviderPanel.vue";
import CostModelTable from "@/components/cost-analysis/CostModelTable.vue";
import { useFormatters } from "@/composables/useFormatters";
import { useCostAnalysisViewModel, lineChartGuideYs } from "@/composables/useCostAnalysisViewModel";
import {
  type CostAnalysisWindow,
  useCostAnalysisStore,
} from "@/stores/costAnalysis";
import { useSystemSettingsStore } from "@/stores/systemSettings";

type FocusGranularity = "daily" | "hourly";
type FocusMetric = "tokens" | "cost" | "calls" | "cache";

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

const formatters = useFormatters(locale, displayCurrency);
const vm = useCostAnalysisViewModel({
  analysis,
  focusGranularity,
  focusMetric,
  t,
  formatters,
});

const {
  windowOptions,
  focusMetricOptions,
  summary,
  dailyBuckets,
  hourlyBuckets,
  focusModels,
  allModels,
  dailyCostChart,
  hourlyTokenChart,
  hourlyTokenTotal,
  providerRows,
  topCostModelRows,
  summaryStats,
  overviewSignals,
  bucketIntensity,
  formatHeatmapValue,
  modelKey,
  modelName,
  providerName,
  bucketRate,
  shareWidth,
  timelineLabel,
  formatBucketHeader,
} = vm;

const {
  formatNumber,
  formatCompactNumber,
  formatCurrency,
  formatPercent,
  formatDate,
  formatDateRangeStart,
  formatDateTime,
  formatDuration,
} = formatters;

const activeWindow = computed<CostAnalysisWindow>({
  get: () => selectedDays.value,
  set: (value) => {
    if (!value || value === selectedDays.value) return;
    costAnalysisStore.setSelectedDays(value);
    void costAnalysisStore.fetchAnalysis(value);
  },
});

const refreshPage = () =>
  void costAnalysisStore.fetchAnalysis(selectedDays.value, { force: true });

const providerMetaLabel = (args: { models: number; calls: string }) =>
  t("pages.costAnalysis.labels.providerMeta", args);

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

.analysis-layout {
  display: grid;
  grid-template-columns: minmax(0, 1.45fr) minmax(320px, 0.78fr);
  align-items: stretch;
  gap: 16px;

  &--secondary {
    grid-template-columns: minmax(0, 1fr) minmax(320px, 0.86fr);
  }
}

.tooltip-stack {
  display: grid;
  gap: 4px;
}

@include respond-to("tablet") {
  .analysis-layout,
  .analysis-layout--secondary {
    grid-template-columns: minmax(0, 1fr);
  }
}

@include respond-to("mobile") {
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
