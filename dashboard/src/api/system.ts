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
  upstreamRef: string
  upstreamTrackingCommit: string
  upstreamTrackingCommitShort: string
  remoteName: string
  remoteUrl: string
  remoteHeadCommit: string
  remoteHeadCommitShort: string
  remoteCheckOk: boolean
  updateAvailable: boolean
  aheadCount: number
  behindCount: number
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

export interface DashboardDistUpdateStatus {
  enabled: boolean
  sourceType?: 'zip' | 'git'
  packageSource?: string
  packageSha256?: string
  expectedPackageSha256?: string
  expectedPackageSha256Url?: string
  deployedPackageSha256?: string
  sourceRepoPath: string
  sourceSubdir: string
  sourceDistPath: string
  targetDistPath: string
  branch: string
  upstream: string
  upstreamRef: string
  remoteName: string
  remoteUrl: string
  currentCommit: string
  currentCommitShort: string
  remoteHeadCommit: string
  remoteHeadCommitShort: string
  remoteCheckOk: boolean
  updateAvailable: boolean
  replaceRequired: boolean
  deployedSourceCommit: string
  deployedSourceCommitShort: string
  dirty: boolean
  dirtyCount: number
  dirtyEntries: string[]
  allowedBranches: string[]
  canUpdate: boolean
  blockCode: string | null
  blockMessage: string | null
  updateInProgress: boolean
  credentialsChangeRequired: boolean
}

export interface DashboardDistUpdateResult {
  accepted: boolean
  updated: boolean
  copied: boolean
  restartRequired: boolean
  sourceCommit: string
  sourceCommitShort: string
  packageSha256?: string
  packageSha256Short?: string
  targetDistPath: string
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

  getDashboardDistStatus() {
    return apiClient.get<DashboardDistUpdateStatus>('/system/dashboard-dist', {
      suppressErrorNotify: true,
    })
  },

  updateDashboardDist() {
    return apiClient.post<DashboardDistUpdateResult>('/system/dashboard-dist/update', undefined, {
      timeout: 120000,
      suppressErrorNotify: true,
    })
  },
}
