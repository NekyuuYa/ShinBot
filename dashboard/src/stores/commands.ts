import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import { commandsApi, type CommandDefinition } from '@/api/commands'
import { createCrudStore } from './crud'

export type CommandLayoutMode = 'list' | 'card'

const COMMANDS_LIST_STALE_TIME_MS = 30_000

export const useCommandsStore = defineStore(
  'commands',
  () => {
    const commands = ref<CommandDefinition[]>([])
    const crud = createCrudStore<CommandDefinition, never, Pick<CommandDefinition, 'enabled'>, string>({
      api: {
        list: commandsApi.list,
        update: commandsApi.update,
      },
      i18nKey: {
        loadFailed: 'pages.commands.loadFailed',
        updateFailed: 'pages.commands.updateFailed',
        updated: 'pages.commands.updated',
      },
      idOf: (command) => command.name,
      items: commands,
      listStaleTimeMs: COMMANDS_LIST_STALE_TIME_MS,
    })
    const layoutMode = ref<CommandLayoutMode>('list')

    const enabledCount = computed(() => commands.value.filter((item) => item.enabled).length)
    const pluginOwnedCount = computed(() => commands.value.filter((item) => Boolean(item.owner)).length)
    const aliasedCount = computed(() => commands.value.filter((item) => item.aliases.length > 0).length)

    const setLayoutMode = (mode: CommandLayoutMode) => {
      layoutMode.value = mode
    }

    const updateCommandEnabled = async (name: string, enabled: boolean) => {
      const command = await crud.updateItem(name, { enabled })
      return command !== null
    }

    return {
      commands,
      isLoading: crud.isLoading,
      isSaving: crud.isSaving,
      error: crud.error,
      layoutMode,
      enabledCount,
      pluginOwnedCount,
      aliasedCount,
      fetchCommands: crud.fetchItems,
      updateCommandEnabled,
      setLayoutMode,
    }
  },
  {
    persist: true,
  }
)
