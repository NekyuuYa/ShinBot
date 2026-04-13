import { apiClient } from './client'

export interface Instance {
  id: string
  name: string
  adapterType: string
  status: 'running' | 'stopped'
  config: InstanceConfig
  createdAt: number
  lastModified: number
}

export interface InstanceConfig {
  host?: string
  token?: string
  port?: number
  [key: string]: unknown
}

export interface CreateInstanceRequest {
  name: string
  adapterType: string
  config: InstanceConfig
}

export interface UpdateInstanceRequest {
  name?: string
  config?: InstanceConfig
}

export interface InstanceControlRequest {
  action: 'start' | 'stop'
}

export const instancesApi = {
  list() {
    return apiClient.get<Instance[]>('/instances')
  },

  create(data: CreateInstanceRequest) {
    return apiClient.post<Instance>('/instances', data)
  },

  update(id: string, data: UpdateInstanceRequest) {
    return apiClient.patch<Instance>(`/instances/${id}`, data)
  },

  delete(id: string) {
    return apiClient.delete<void>(`/instances/${id}`)
  },

  start(id: string) {
    return apiClient.post<void>(`/instances/${id}/control`, { action: 'start' } satisfies InstanceControlRequest)
  },

  stop(id: string) {
    return apiClient.post<void>(`/instances/${id}/control`, { action: 'stop' } satisfies InstanceControlRequest)
  },
}
