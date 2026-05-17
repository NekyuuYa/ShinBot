import type { AxiosResponse } from 'axios'

import { apiClient, type ApiResponse } from './client'
import type { BotConfigSummary } from './botConfigs'

export interface Instance {
  id: string
  name: string
  adapterType: string
  status: 'running' | 'stopped'
  config: InstanceConfig
  botConfig: BotConfigSummary | null
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

interface BackendInstance {
  id: string
  name: string
  adapter?: string
  adapterType?: string
  status: 'running' | 'stopped'
  config: InstanceConfig
  instanceConfig?: BotConfigSummary | null
  botConfig?: BotConfigSummary | null
  createdAt: number
  lastModified: number
}

interface BackendCreateInstanceRequest {
  name: string
  adapter: string
  config: InstanceConfig
}

const normalizeInstance = (item: BackendInstance): Instance => ({
  id: item.id,
  name: item.name,
  adapterType: item.adapterType ?? item.adapter ?? '',
  status: item.status,
  config: item.config,
  botConfig: item.botConfig ?? item.instanceConfig ?? null,
  createdAt: item.createdAt,
  lastModified: item.lastModified,
})

const mapResponseData = <Raw, Normalized>(
  response: AxiosResponse<ApiResponse<Raw>>,
  mapper: (raw: Raw) => Normalized
): AxiosResponse<ApiResponse<Normalized>> => ({
  ...response,
  data: {
    ...response.data,
    data: response.data.data === undefined ? undefined : mapper(response.data.data),
  },
})

const mapInstanceResponse = (
  response: AxiosResponse<ApiResponse<BackendInstance>>
): AxiosResponse<ApiResponse<Instance>> => mapResponseData(response, normalizeInstance)

const mapInstanceListResponse = (
  response: AxiosResponse<ApiResponse<BackendInstance[]>>
): AxiosResponse<ApiResponse<Instance[]>> =>
  mapResponseData(response, (items) => items.map(normalizeInstance))

const toBackendCreatePayload = (data: CreateInstanceRequest): BackendCreateInstanceRequest => ({
  name: data.name,
  adapter: data.adapterType,
  config: data.config,
})

export const instancesApi = {
  list() {
    return apiClient.get<BackendInstance[]>('/instances').then(mapInstanceListResponse)
  },

  create(data: CreateInstanceRequest) {
    return apiClient.post<BackendInstance>('/instances', toBackendCreatePayload(data)).then(mapInstanceResponse)
  },

  update(id: string, data: UpdateInstanceRequest) {
    return apiClient.patch<BackendInstance>(`/instances/${id}`, data).then(mapInstanceResponse)
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
