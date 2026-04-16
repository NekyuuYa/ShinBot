import { apiClient } from './client'

export interface LoginRequest {
  username: string
  password: string
}

export interface LoginResponse {
  token: string
  token_type: string
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
  login(credentials: LoginRequest) {
    return apiClient.post<LoginResponse>('/auth/login', credentials)
  },

  getProfile() {
    return apiClient.get<AuthProfileResponse>('/auth/profile')
  },

  updateProfile(payload: UpdateProfileRequest) {
    return apiClient.patch<LoginResponse>('/auth/profile', payload)
  },

  logout() {
    localStorage.removeItem('auth_token')
    localStorage.removeItem('auth_username')
    localStorage.removeItem('auth_must_change_credentials')
  },
}
