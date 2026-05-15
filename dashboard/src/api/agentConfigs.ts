import { apiClient, type ApiRequestConfig } from "./client";
import type { ConfigRecord, ConfigValidationIssue } from "./config";

export interface AgentConfigProfile {
  fileName: string;
  path: string;
  agentId: string;
  mode: string;
  personaId: string;
  config: ConfigRecord;
  lastModified: number;
  issues: ConfigValidationIssue[];
}

export interface SaveAgentConfigRequest {
  fileName?: string;
  config: ConfigRecord;
  validateBeforeSave?: boolean;
}

export const agentConfigsApi = {
  list(config?: ApiRequestConfig) {
    return apiClient.get<AgentConfigProfile[]>("/agent-configs", config);
  },

  get(fileName: string, config?: ApiRequestConfig) {
    return apiClient.get<AgentConfigProfile>(
      `/agent-configs/${encodeURIComponent(fileName)}`,
      config,
    );
  },

  create(payload: SaveAgentConfigRequest, config?: ApiRequestConfig) {
    return apiClient.post<AgentConfigProfile>(
      "/agent-configs",
      payload,
      config,
    );
  },

  update(
    fileName: string,
    payload: SaveAgentConfigRequest,
    config?: ApiRequestConfig,
  ) {
    return apiClient.put<AgentConfigProfile>(
      `/agent-configs/${encodeURIComponent(fileName)}`,
      payload,
      config,
    );
  },

  delete(fileName: string, config?: ApiRequestConfig) {
    return apiClient.delete<{ deleted: boolean; fileName: string }>(
      `/agent-configs/${encodeURIComponent(fileName)}`,
      config,
    );
  },
};
