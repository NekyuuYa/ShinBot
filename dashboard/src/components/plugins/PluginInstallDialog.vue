<template>
  <v-dialog :model-value="modelValue" max-width="920" persistent @update:model-value="updateVisible">
    <v-card>
      <v-card-title class="d-flex align-center ga-3">
        <v-icon icon="mdi-puzzle-plus" color="primary" />
        <span>{{ $t('pages.plugins.install.title') }}</span>
      </v-card-title>

      <v-card-text>
        <v-tabs v-model="sourceMode" color="primary" density="comfortable" class="mb-4">
          <v-tab value="github" prepend-icon="mdi-github">
            {{ $t('pages.plugins.install.githubTab') }}
          </v-tab>
          <v-tab value="archive" prepend-icon="mdi-folder-zip">
            {{ $t('pages.plugins.install.archiveTab') }}
          </v-tab>
        </v-tabs>

        <v-window v-model="sourceMode">
          <v-window-item value="github">
            <v-row>
              <v-col cols="12" md="8">
                <v-text-field
                  v-model.trim="githubUrl"
                  :label="$t('pages.plugins.install.githubUrl')"
                  prepend-inner-icon="mdi-link-variant"
                  variant="outlined"
                  density="comfortable"
                  hide-details="auto"
                />
              </v-col>
              <v-col cols="12" md="4">
                <v-text-field
                  v-model.trim="githubRef"
                  :label="$t('pages.plugins.install.githubRef')"
                  prepend-inner-icon="mdi-source-branch"
                  variant="outlined"
                  density="comfortable"
                  hide-details="auto"
                />
              </v-col>
              <v-col cols="12">
                <v-text-field
                  v-model.trim="githubPluginPath"
                  :label="$t('pages.plugins.install.githubPluginPath')"
                  :hint="$t('pages.plugins.install.githubPluginPathHint')"
                  prepend-inner-icon="mdi-folder-search-outline"
                  variant="outlined"
                  density="comfortable"
                  persistent-hint
                />
              </v-col>
            </v-row>
          </v-window-item>

          <v-window-item value="archive">
            <v-file-input
              v-model="archiveInput"
              :label="$t('pages.plugins.install.archiveFile')"
              accept=".zip,application/zip,application/x-zip-compressed"
              prepend-icon=""
              prepend-inner-icon="mdi-folder-zip"
              variant="outlined"
              density="comfortable"
              clearable
              hide-details="auto"
            />
          </v-window-item>
        </v-window>

        <div class="d-flex flex-wrap align-center ga-4 mt-2">
          <v-switch
            v-model="enableAfterInstall"
            color="primary"
            density="compact"
            hide-details
            :label="$t('pages.plugins.install.enableAfterInstall')"
          />
          <v-switch
            v-model="allowOverwrite"
            color="warning"
            density="compact"
            hide-details
            :disabled="!preview?.target_exists"
            :label="$t('pages.plugins.install.allowOverwrite')"
          />
          <v-spacer />
          <v-btn
            color="primary"
            prepend-icon="mdi-eye"
            :disabled="!canPreview"
            :loading="pluginsStore.isSaving"
            @click="handlePreview"
          >
            {{ $t('pages.plugins.install.preview') }}
          </v-btn>
        </div>

        <v-alert v-if="pluginsStore.installError" type="error" variant="tonal" class="mt-4">
          <div class="font-weight-medium">{{ pluginsStore.installError.code }}</div>
          <div>{{ pluginsStore.installError.message }}</div>
        </v-alert>

        <v-alert v-if="preview && preview.target_exists" type="warning" variant="tonal" class="mt-4">
          <span v-if="preview.target_managed_by_webui">
            {{ $t('pages.plugins.install.overwriteManagedWarning') }}
          </span>
          <span v-else>
            {{ $t('pages.plugins.install.overwriteUnmanagedWarning') }}
          </span>
        </v-alert>

        <v-alert
          v-if="preview && preview.missing_required_dependencies.length > 0"
          type="error"
          variant="tonal"
          class="mt-4"
        >
          {{ $t('pages.plugins.install.missingRequiredBlock') }}
        </v-alert>

        <div v-if="preview" class="preview-panel mt-4">
          <div class="d-flex flex-wrap align-start ga-3 mb-4">
            <div class="flex-grow-1">
              <div class="text-h6 text-break">{{ preview.name }}</div>
              <div class="text-caption text-medium-emphasis text-break">{{ preview.plugin_id }}</div>
            </div>
            <v-chip color="primary" variant="tonal" size="small">
              {{ sourceLabel(preview.source_type) }}
            </v-chip>
            <v-chip :color="preview.can_install ? 'success' : 'error'" variant="tonal" size="small">
              {{
                preview.can_install
                  ? $t('pages.plugins.install.installable')
                  : $t('pages.plugins.install.notInstallable')
              }}
            </v-chip>
          </div>

          <v-row dense>
            <v-col cols="12" md="6">
              <div class="meta-line">
                <span>{{ $t('pages.plugins.form.version') }}</span>
                <strong>{{ preview.version }}</strong>
              </div>
              <div class="meta-line">
                <span>{{ $t('pages.plugins.form.author') }}</span>
                <strong>{{ preview.author || '-' }}</strong>
              </div>
              <div class="meta-line">
                <span>{{ $t('pages.plugins.install.role') }}</span>
                <strong>{{ preview.role }}</strong>
              </div>
              <div class="meta-line">
                <span>{{ $t('pages.plugins.install.entry') }}</span>
                <strong>{{ preview.entry }}</strong>
              </div>
            </v-col>
            <v-col cols="12" md="6">
              <div class="meta-line">
                <span>{{ $t('pages.plugins.install.source') }}</span>
                <strong class="text-break">{{ preview.source_url }}</strong>
              </div>
              <div class="meta-line">
                <span>{{ $t('pages.plugins.install.ref') }}</span>
                <strong>{{ preview.ref || '-' }}</strong>
              </div>
              <div class="meta-line">
                <span>{{ $t('pages.plugins.install.resolvedRef') }}</span>
                <strong class="text-break">{{ preview.resolved_ref || '-' }}</strong>
              </div>
              <div class="meta-line">
                <span>{{ $t('pages.plugins.install.pluginPath') }}</span>
                <strong class="text-break">{{ preview.plugin_path || '-' }}</strong>
              </div>
              <div class="meta-line">
                <span>{{ $t('pages.plugins.install.sha256') }}</span>
                <strong class="text-break">{{ preview.archive_sha256 || '-' }}</strong>
              </div>
            </v-col>
          </v-row>

          <p v-if="preview.description" class="text-body-2 mt-3 mb-0">
            {{ preview.description }}
          </p>

          <v-divider class="my-4" />

          <v-row dense>
            <v-col cols="12" md="6">
              <div class="dependency-block">
                <div class="text-subtitle-2 mb-2">
                  {{ $t('pages.plugins.install.requiredDependencies') }}
                </div>
                <div v-if="preview.required_dependencies.length > 0" class="d-flex flex-wrap ga-2">
                  <v-chip
                    v-for="item in preview.required_dependencies"
                    :key="item"
                    :color="preview.missing_required_dependencies.includes(item) ? 'error' : 'success'"
                    variant="tonal"
                    size="small"
                  >
                    {{ dependencyLabel(item, preview.missing_required_dependencies) }}
                  </v-chip>
                </div>
                <div v-else class="text-caption text-medium-emphasis">
                  {{ $t('pages.plugins.install.none') }}
                </div>
              </div>
            </v-col>
            <v-col cols="12" md="6">
              <div class="dependency-block">
                <div class="text-subtitle-2 mb-2">
                  {{ $t('pages.plugins.install.optionalDependencies') }}
                </div>
                <div v-if="preview.optional_dependencies.length > 0" class="d-flex flex-wrap ga-2">
                  <v-chip
                    v-for="item in preview.optional_dependencies"
                    :key="item"
                    :color="preview.missing_optional_dependencies.includes(item) ? 'warning' : 'success'"
                    variant="tonal"
                    size="small"
                  >
                    {{ dependencyLabel(item, preview.missing_optional_dependencies) }}
                  </v-chip>
                </div>
                <div v-else class="text-caption text-medium-emphasis">
                  {{ $t('pages.plugins.install.none') }}
                </div>
              </div>
            </v-col>
            <v-col cols="12" md="6">
              <div class="dependency-block">
                <div class="text-subtitle-2 mb-2">
                  {{ $t('pages.plugins.install.legacyDependencies') }}
                </div>
                <div v-if="preview.legacy_dependencies.length > 0" class="d-flex flex-wrap ga-2">
                  <v-chip
                    v-for="item in preview.legacy_dependencies"
                    :key="item"
                    color="secondary"
                    variant="tonal"
                    size="small"
                  >
                    {{ item }}
                  </v-chip>
                </div>
                <div v-else class="text-caption text-medium-emphasis">
                  {{ $t('pages.plugins.install.none') }}
                </div>
              </div>
            </v-col>
            <v-col cols="12" md="6">
              <div class="dependency-block">
                <div class="text-subtitle-2 mb-2">
                  {{ $t('pages.plugins.install.permissions') }}
                </div>
                <div v-if="preview.permissions.length > 0" class="d-flex flex-wrap ga-2">
                  <v-chip
                    v-for="item in preview.permissions"
                    :key="item"
                    color="info"
                    variant="tonal"
                    size="small"
                  >
                    {{ item }}
                  </v-chip>
                </div>
                <div v-else class="text-caption text-medium-emphasis">
                  {{ $t('pages.plugins.install.none') }}
                </div>
              </div>
            </v-col>
          </v-row>

          <v-alert v-if="preview.warnings.length > 0" type="warning" variant="tonal" class="mt-4">
            <ul class="warning-list">
              <li v-for="warning in preview.warnings" :key="warning">{{ warning }}</li>
            </ul>
          </v-alert>
        </div>

        <v-alert v-if="task" :type="task.status === 'failed' ? 'error' : 'success'" variant="tonal" class="mt-4">
          <div class="d-flex flex-wrap align-center ga-2">
            <v-chip :color="taskColor(task.status)" variant="tonal" size="small">
              {{ task.status }}
            </v-chip>
            <strong>{{ task.stage }}</strong>
            <span>{{ task.message }}</span>
            <v-spacer />
            <v-btn size="small" variant="text" @click="refreshTask">
              {{ $t('pages.plugins.install.refreshTask') }}
            </v-btn>
          </div>
          <div v-if="task.error" class="mt-2">
            <strong>{{ task.error.code }}</strong>
            <span class="ms-2">{{ task.error.message }}</span>
          </div>
        </v-alert>
      </v-card-text>

      <v-card-actions>
        <v-spacer />
        <v-btn variant="text" @click="close">
          {{ $t('common.actions.action.cancel') }}
        </v-btn>
        <v-btn
          color="primary"
          prepend-icon="mdi-download"
          :disabled="!canInstall"
          :loading="pluginsStore.isSaving"
          @click="handleInstall"
        >
          {{ $t('pages.plugins.install.install') }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { usePluginsStore } from '@/stores/plugins'
import type { PluginInstallPreview, PluginInstallSourceType, PluginInstallTask } from '@/api/plugins'

type InstallSourceMode = 'github' | 'archive'

interface Props {
  modelValue: boolean
}

const props = defineProps<Props>()
const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  completed: []
}>()

const { t } = useI18n()
const pluginsStore = usePluginsStore()

const sourceMode = ref<InstallSourceMode>('github')
const githubUrl = ref('')
const githubRef = ref('main')
const githubPluginPath = ref('')
const archiveInput = ref<File | File[] | null>(null)
const enableAfterInstall = ref(true)
const allowOverwrite = ref(false)
const preview = ref<PluginInstallPreview | null>(null)
const previewFingerprint = ref('')
const task = ref<PluginInstallTask | null>(null)

const selectedArchive = computed(() => {
  if (Array.isArray(archiveInput.value)) {
    return archiveInput.value[0] ?? null
  }
  return archiveInput.value
})

const canPreview = computed(() => {
  if (pluginsStore.isSaving) {
    return false
  }
  if (sourceMode.value === 'github') {
    return githubUrl.value.trim().length > 0 && githubRef.value.trim().length > 0
  }
  return selectedArchive.value !== null
})

const requiresOverwrite = computed(() =>
  Boolean(preview.value?.target_exists && preview.value.target_managed_by_webui)
)

const currentFingerprint = computed(() => buildSourceFingerprint())

const canInstall = computed(() => {
  if (!preview.value || pluginsStore.isSaving) {
    return false
  }
  if (!previewFingerprint.value || previewFingerprint.value !== currentFingerprint.value) {
    return false
  }
  if (!preview.value.can_install || preview.value.missing_required_dependencies.length > 0) {
    return false
  }
  if (preview.value.target_exists && !preview.value.target_managed_by_webui) {
    return false
  }
  return !requiresOverwrite.value || allowOverwrite.value
})

const resetResult = () => {
  preview.value = null
  previewFingerprint.value = ''
  task.value = null
  pluginsStore.clearInstallError()
}

const resetForm = () => {
  sourceMode.value = 'github'
  githubUrl.value = ''
  githubRef.value = 'main'
  githubPluginPath.value = ''
  archiveInput.value = null
  enableAfterInstall.value = true
  allowOverwrite.value = false
  resetResult()
}

watch([sourceMode, githubUrl, githubRef, githubPluginPath, archiveInput], () => {
  resetResult()
  allowOverwrite.value = false
})

watch(
  () => props.modelValue,
  (visible) => {
    if (!visible) {
      resetForm()
    }
  }
)

const updateVisible = (value: boolean) => {
  emit('update:modelValue', value)
}

const close = () => {
  emit('update:modelValue', false)
}

const buildSourceFingerprint = () => {
  if (sourceMode.value === 'github') {
    return JSON.stringify({
      source: 'github',
      url: githubUrl.value.trim(),
      ref: githubRef.value.trim() || 'main',
      pluginPath: githubPluginPath.value.trim(),
    })
  }

  const file = selectedArchive.value
  return JSON.stringify({
    source: 'archive',
    name: file?.name ?? '',
    size: file?.size ?? 0,
    lastModified: file?.lastModified ?? 0,
  })
}

const handlePreview = async () => {
  const fingerprint = buildSourceFingerprint()
  task.value = null
  preview.value = null
  previewFingerprint.value = ''
  allowOverwrite.value = false

  if (sourceMode.value === 'github') {
    const result = await pluginsStore.previewGithubInstall({
      url: githubUrl.value.trim(),
      ref: githubRef.value.trim() || 'main',
      plugin_path: githubPluginPath.value.trim(),
    })
    if (fingerprint === buildSourceFingerprint()) {
      preview.value = result
      previewFingerprint.value = result ? fingerprint : ''
    }
    return
  }

  if (selectedArchive.value) {
    const result = await pluginsStore.previewArchiveInstall(selectedArchive.value)
    if (fingerprint === buildSourceFingerprint()) {
      preview.value = result
      previewFingerprint.value = result ? fingerprint : ''
    }
  }
}

const handleInstall = async () => {
  if (!preview.value) {
    return
  }
  if (previewFingerprint.value !== buildSourceFingerprint()) {
    resetResult()
    return
  }

  if (sourceMode.value === 'github') {
    task.value = await pluginsStore.installGithub({
      url: githubUrl.value.trim(),
      ref: githubRef.value.trim() || 'main',
      plugin_path: githubPluginPath.value.trim(),
      enable_after_install: enableAfterInstall.value,
      allow_overwrite: allowOverwrite.value,
    })
  } else if (selectedArchive.value) {
    task.value = await pluginsStore.installArchive(selectedArchive.value, {
      enable_after_install: enableAfterInstall.value,
      allow_overwrite: allowOverwrite.value,
    })
  }

  if (task.value?.status === 'succeeded') {
    emit('completed')
  }
}

const refreshTask = async () => {
  if (!task.value) {
    return
  }
  const refreshed = await pluginsStore.fetchInstallTask(task.value.task_id)
  if (refreshed) {
    task.value = refreshed
  }
}

const sourceLabel = (sourceType: PluginInstallSourceType) => {
  return sourceType === 'github'
    ? t('pages.plugins.install.githubSource')
    : t('pages.plugins.install.archiveSource')
}

const taskColor = (status: PluginInstallTask['status']) => {
  if (status === 'succeeded') {
    return 'success'
  }
  if (status === 'failed') {
    return 'error'
  }
  if (status === 'running') {
    return 'primary'
  }
  return 'grey'
}

const dependencyLabel = (item: string, missingItems: string[]) => {
  return missingItems.includes(item)
    ? `${item} (${t('pages.plugins.install.missing')})`
    : item
}
</script>

<style scoped lang="scss">
.preview-panel {
  border: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  border-radius: 8px;
  padding: 16px;
}

.meta-line {
  display: grid;
  grid-template-columns: minmax(96px, 0.35fr) minmax(0, 1fr);
  gap: 12px;
  align-items: start;
  margin-bottom: 8px;
  font-size: 0.875rem;
}

.meta-line span {
  color: rgba(var(--v-theme-on-surface), 0.62);
}

.dependency-block {
  min-height: 64px;
}

.warning-list {
  margin: 0;
  padding-left: 18px;
}
</style>
