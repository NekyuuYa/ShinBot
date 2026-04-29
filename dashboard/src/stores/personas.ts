import { defineStore } from 'pinia'

import { personasApi, type Persona, type PersonaPayload } from '@/api/personas'
import { createCrudStore } from './crud'

export const usePersonasStore = defineStore('personas', () => {
  const crud = createCrudStore<Persona, PersonaPayload, Partial<PersonaPayload>, string>({
    api: personasApi,
    i18nKey: 'pages.personas.messages',
    idOf: (persona) => persona.uuid,
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
