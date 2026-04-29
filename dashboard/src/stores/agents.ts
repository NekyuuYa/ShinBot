import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

import { agentsApi, type Agent, type AgentPayload } from '@/api/agents'
import { createCrudStore } from './crud'

const AGENTS_LIST_STALE_TIME_MS = 30_000

export const useAgentsStore = defineStore('agents', () => {
  const agents = ref<Agent[]>([])
  const crud = createCrudStore<Agent, AgentPayload, Partial<AgentPayload>, string>({
    api: agentsApi,
    i18nKey: 'pages.agents.messages',
    idOf: (agent) => agent.uuid,
    items: agents,
    listStaleTimeMs: AGENTS_LIST_STALE_TIME_MS,
  })

  const allTags = computed(() => {
    const tagSet = new Set<string>()
    for (const agent of agents.value) {
      for (const tag of agent.tags) {
        const trimmed = tag.trim()
        if (!trimmed) {
          continue
        }
        tagSet.add(trimmed)
      }
    }
    return Array.from(tagSet).sort((a, b) => a.localeCompare(b))
  })

  return {
    agents,
    allTags,
    isLoading: crud.isLoading,
    isSaving: crud.isSaving,
    error: crud.error,
    fetchAgents: crud.fetchItems,
    createAgent: crud.createItem,
    updateAgent: crud.updateItem,
    deleteAgent: crud.deleteItem,
  }
})
