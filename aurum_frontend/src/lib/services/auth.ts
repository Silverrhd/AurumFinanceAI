import { apiClient } from '../api';
import { LoginCredentials, AuthTokens, TokenInfo } from '@/types/auth';

export class AuthService {
  static async login(credentials: LoginCredentials): Promise<AuthTokens> {
    return await apiClient.post<AuthTokens>('/auth/login/', credentials);
  }

  static async logout(): Promise<void> {
    try {
      await apiClient.post('/auth/logout/');
    } catch {
      // Even if logout fails on server, we should clear local storage
      console.warn('Server logout failed, but clearing local storage anyway');
    }
  }

  static async getTokenInfo(): Promise<TokenInfo> {
    return await apiClient.get<TokenInfo>('/client/token-info/');
  }

  static async refreshToken(refreshToken: string): Promise<{ access: string }> {
    return await apiClient.post<{ access: string }>('/auth/token/refresh/', {
      refresh: refreshToken
    });
  }
}