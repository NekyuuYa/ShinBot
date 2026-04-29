import { apiClient, type ApiRequestConfig } from './client'

export interface LoginRequest {
  username: string
  password: string
}

export interface LoginResponse {
  expires_in_hours: number
  username: string
  must_change_credentials: boolean
}

export interface AuthProfileResponse {
  username: string
  must_change_credentials: boolean
}

export interface UpdateProfileRequest {
  username: string
  current_password: string
  new_password: string
}

export const authApi = {
  login(credentials: LoginRequest, config?: ApiRequestConfig) {
    return apiClient.post<LoginResponse>('/auth/login', credentials, config)
  },

  getProfile(config?: ApiRequestConfig) {
    return apiClient.get<AuthProfileResponse>('/auth/profile', config)
  },

  updateProfile(payload: UpdateProfileRequest, config?: ApiRequestConfig) {
    return apiClient.patch<LoginResponse>('/auth/profile', payload, config)
  },

  logout(config?: ApiRequestConfig) {
    return apiClient.post('/auth/logout', undefined, config)
  },
}
