export interface LoginCredentials {
  username: string;
  password: string;
}

export interface User {
  id: number;
  username: string;
  email: string;
  first_name: string;
  last_name: string;
  role: 'admin' | 'client';
  client_code?: string;
  date_joined: string;
  last_login?: string;
}

export interface AuthTokens {
  access: string;
  refresh: string;
  user: User;
}

export interface TokenInfo {
  valid: boolean;
  user_id: number;
  username: string;
  role: 'admin' | 'client';
  client_code?: string;
  expires_at: string;
}