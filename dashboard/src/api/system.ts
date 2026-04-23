import { apiClient } from './client'

export interface RestartRequestPayload {
  reason: 'manual' | 'update'
  requested_at: number
  requested_by?: string
  source?: string
}

export interface SystemUpdateStatus {
  repoDetected: boolean
  repoPath: string
  branch: string
  upstream: string
  remoteUrl: string
  currentCommit: string
  currentCommitShort: string
  dirty: boolean
  dirtyCount: number
  dirtyEntries: string[]
  allowedBranches: string[]
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
  alreadyUpToDate: boolean
  restartRequested: boolean
  restartRequest: RestartRequestPayload | null
  repoPath: string
  branch: string
  upstream: string
  beforeCommit: string
  beforeCommitShort: string
  afterCommit: string
  afterCommitShort: string
  output: string
}

export const systemApi = {
  getUpdateStatus() {
    return apiClient.get<SystemUpdateStatus>('/system/update', {
      suppressErrorNotify: true,
    })
  },

  pullAndRestart() {
    return apiClient.post<SystemUpdateResult>('/system/update', undefined, {
      timeout: 120000,
      suppressErrorNotify: true,
    })
  },
}
