import { ref } from 'vue'
import { promptsApi, type PromptCatalogItem } from '@/api/prompts'
import { toolsApi, type ToolDefinition } from '@/api/tools'
import { createCachedRequest, type CachedRequestOptions } from '@/utils/requestCache'

const AGENT_RESOURCES_STALE_TIME_MS = 30_000

const loadPromptCatalog = createCachedRequest(async () => {
  const response = await promptsApi.list()
  return response.data.success ? response.data.data || [] : []
}, AGENT_RESOURCES_STALE_TIME_MS)

const loadToolCatalog = createCachedRequest(async () => {
  const response = await toolsApi.list()
  return response.data.success ? response.data.data || [] : []
}, AGENT_RESOURCES_STALE_TIME_MS)

export function useAgentResources() {
  const promptCatalog = ref<PromptCatalogItem[]>([])
  const toolCatalog = ref<ToolDefinition[]>([])

  const isLoadingResources = ref(false)
  const resourceError = ref('')

  const fetchAllResources = async (options: CachedRequestOptions = {}) => {
    isLoadingResources.value = true
    resourceError.value = ''
    try {
      const [prompts, tools] = await Promise.all([
        loadPromptCatalog(options),
        loadToolCatalog(options),
      ])
      promptCatalog.value = prompts
      toolCatalog.value = tools
    } catch (err: unknown) {
      resourceError.value = err instanceof Error ? err.message : String(err)
    } finally {
      isLoadingResources.value = false
    }
  }

  const promptOptions = (selectedIds: string[]) => {
    const options = promptCatalog.value
      .map((p) => ({ title: `${p.displayName} (${p.id})`, value: p.id }))
      .sort((a, b) => a.title.localeCompare(b.title))

    selectedIds.forEach((id) => {
      if (!options.some((o) => o.value === id)) options.push({ title: id, value: id })
    })
    return options
  }

  const toolOptions = (selectedIds: string[]) => {
    const options = toolCatalog.value
      .map((t) => ({ title: `${t.displayName || t.name} (${t.id})`, value: t.id }))
      .sort((a, b) => a.title.localeCompare(b.title))

    selectedIds.forEach((id) => {
      if (!options.some((o) => o.value === id)) options.push({ title: id, value: id })
    })
    return options
  }

  return {
    promptCatalog,
    toolCatalog,
    isLoadingResources,
    resourceError,
    fetchAllResources,
    promptOptions,
    toolOptions,
  }
}
