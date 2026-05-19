import { apiClient } from './client'

export interface RestartRequestPayload {
  reason: 'manual' | 'update'
  requested_at: number
  requested_by?: string
  source?: string
}

export interface SystemUpdateStatus {
  enabled: boolean
  workdir: string
  command: string
  restartAfterSuccess: boolean
  canUpdate: boolean
  blockCode: string | null
  blockMessage: string | null
  updateInProgress: boolean
  credentialsChangeRequired: boolean
  restartRequested: boolean
  restartRequest: RestartRequestPayload | null
}

export interface SystemUpdateResult {
  accepted: boolean
  updated: boolean
  restartRequested: boolean
  restartRequest: RestartRequestPayload | null
  workdir: string
  command: string
  output: string
}

export interface DashboardBuildStatus {
  enabled: boolean
  dashboardPath: string
  distPath: string
  command: string
  canBuild: boolean
  blockCode: string | null
  blockMessage: string | null
  buildInProgress: boolean
  credentialsChangeRequired: boolean
}

export interface DashboardBuildResult {
  accepted: boolean
  built: boolean
  dashboardPath: string
  distPath: string
  command: string
  output: string
}

export const systemApi = {
  getUpdateStatus() {
    return apiClient.get<SystemUpdateStatus>('/system/update', {
      suppressErrorNotify: true,
    })
  },

  runFrameworkUpdate() {
    return apiClient.post<SystemUpdateResult>('/system/update', undefined, {
      timeout: 300000,
      suppressErrorNotify: true,
    })
  },

  getDashboardBuildStatus() {
    return apiClient.get<DashboardBuildStatus>('/system/dashboard-build', {
      suppressErrorNotify: true,
    })
  },

  buildDashboard() {
    return apiClient.post<DashboardBuildResult>('/system/dashboard-build', undefined, {
      timeout: 180000,
      suppressErrorNotify: true,
    })
  },
}
