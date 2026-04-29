import { defineStore } from 'pinia'

import { personasApi, type Persona, type PersonaPayload } from '@/api/personas'
import { createCrudStore } from './crud'

const PERSONAS_LIST_STALE_TIME_MS = 30_000

export const usePersonasStore = defineStore('personas', () => {
  const crud = createCrudStore<Persona, PersonaPayload, Partial<PersonaPayload>, string>({
    api: personasApi,
    i18nKey: 'pages.personas.messages',
    idOf: (persona) => persona.uuid,
    listStaleTimeMs: PERSONAS_LIST_STALE_TIME_MS,
  })
  const personas = crud.items

  return {
    personas,
    isLoading: crud.isLoading,
    isSaving: crud.isSaving,
    error: crud.error,
    fetchPersonas: crud.fetchItems,
    createPersona: crud.createItem,
    updatePersona: crud.updateItem,
    deletePersona: crud.deleteItem,
  }
})
