import { apiClient } from './client'

export interface Persona {
  uuid: string
  name: string
  promptDefinitionUuid: string
  promptText: string
  tags: string[]
  enabled: boolean
  createdAt: string
  lastModified: string
}

export interface PersonaPayload {
  name: string
  promptText: string
  tags: string[]
  enabled: boolean
}

export const personasApi = {
  list() {
    return apiClient.get<Persona[]>('/personas')
  },

  get(uuid: string) {
    return apiClient.get<Persona>(`/personas/${encodeURIComponent(uuid)}`)
  },

  create(payload: PersonaPayload) {
    return apiClient.post<Persona>('/personas', payload)
  },

  update(uuid: string, payload: Partial<PersonaPayload>) {
    return apiClient.patch<Persona>(`/personas/${encodeURIComponent(uuid)}`, payload)
  },

  delete(uuid: string) {
    return apiClient.delete<{ deleted: boolean; uuid: string }>(
      `/personas/${encodeURIComponent(uuid)}`
    )
  },
}
