import { apiClient } from './client'

export interface LoginRequest {
  username: string
  password: string
}

export interface LoginResponse {
  token: string
  username: string
  email?: string
}

export const authApi = {
  login(credentials: LoginRequest) {
    return apiClient.post<LoginResponse>('/auth/login', credentials)
  },

  logout() {
    localStorage.removeItem('auth_token')
  },
}
