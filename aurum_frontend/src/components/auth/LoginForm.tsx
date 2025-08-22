'use client';

import { useState, FormEvent } from 'react';
import { useRouter } from 'next/navigation';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { LoginCredentials } from '@/types/auth';
import { Shield, User, ArrowRight } from 'lucide-react';

type LoginMode = 'select' | 'admin' | 'client';

export default function LoginForm() {
  const [mode, setMode] = useState<LoginMode>('select');
  const [credentials, setCredentials] = useState<LoginCredentials>({
    username: '',
    password: ''
  });
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string>('');
  const router = useRouter();

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setCredentials(prev => ({
      ...prev,
      [name]: value
    }));
    // Clear error when user types
    if (error) setError('');
  };

  const handleRoleSelection = (role: 'admin' | 'client') => {
    setMode(role);
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setError('');

    try {
      // Call the appropriate Django authentication API based on mode
      const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
      const endpoint = mode === 'admin' ? '/api/auth/admin/login/' : '/api/auth/client/login/';
      const response = await fetch(`${API_BASE_URL}${endpoint}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          username: credentials.username,
          password: credentials.password,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        // Handle specific role validation errors
        if (data.non_field_errors?.[0] === 'Not Valid Admin Credentials') {
          setError('Invalid admin credentials. Please use your admin account.');
        } else if (data.non_field_errors?.[0] === 'Not Valid Client Credentials') {
          setError('Invalid client credentials. Please use your client account.');
        } else {
          setError(data.error || data.detail || 'Invalid username or password');
        }
        return;
      }

      // Store the real tokens from Django
      localStorage.setItem('access_token', data.access);
      localStorage.setItem('refresh_token', data.refresh);
      localStorage.setItem('user_data', JSON.stringify(data.user));

      // Redirect based on user role
      if (data.user.role === 'admin') {
        router.push('/admin/dashboard');
      } else {
        router.push('/client/dashboard');
      }
      
    } catch (err) {
      console.error('Login error:', err);
      setError('Network error. Please check your connection and try again.');
    } finally {
      setIsLoading(false);
    }
  };

  if (mode === 'select') {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 flex items-center justify-center p-6">
        <div className="w-full max-w-6xl">
          {/* Header Section */}
          <div className="text-center mb-10">
            <div className="mb-8">
              <img 
                src="/aurum-logo.png" 
                alt="Aurum Logo" 
                className="mx-auto drop-shadow-sm h-28 w-auto"
              />
            </div>
            <h1 className="text-3xl font-bold text-slate-900 mb-3 tracking-tight">
              Portfolio Consolidation and Analytics System
            </h1>
            <p className="text-base text-slate-600 font-medium">Choose your access level</p>
          </div>

          {/* Login Cards */}
          <div className="grid lg:grid-cols-2 gap-5 max-w-3xl mx-auto">
            {/* Administrator Card */}
            <Card className="group relative overflow-hidden border-0 shadow-lg hover:shadow-xl transition-all duration-300 bg-white">
              <div className="absolute inset-0 bg-gradient-to-br from-blue-50/50 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
              <CardContent className="relative p-5 flex flex-col h-full">
                {/* Header */}
                <div className="flex items-start justify-between mb-3">
                  <div className="flex items-center space-x-3">
                    <div className="w-10 h-10 bg-gradient-to-br from-blue-500 to-blue-600 rounded-xl flex items-center justify-center shadow-lg">
                      <Shield className="w-5 h-5 text-white" />
                    </div>
                    <div>
                      <h3 className="text-lg font-bold text-slate-900 mb-1">Administrator</h3>
                      <Badge className="bg-blue-100 text-blue-700 text-xs font-medium">
                        Full Access
                      </Badge>
                    </div>
                  </div>
                </div>

                {/* Description */}
                <div className="mb-3 flex-1">
                  <p className="text-slate-700 text-xs mb-5 leading-relaxed">
                    Complete system access with comprehensive administrative privileges and portfolio management capabilities.
                  </p>
                  <div className="space-y-2">
                    <h4 className="font-semibold text-slate-900 text-xs">Key Capabilities</h4>
                    <div className="grid gap-1.5">
                      <div className="flex items-center space-x-1.5">
                        <div className="w-1 h-1 bg-blue-500 rounded-full" />
                        <span className="text-slate-600 text-xs">Upload and process bank files</span>
                      </div>
                      <div className="flex items-center space-x-1.5">
                        <div className="w-1 h-1 bg-blue-500 rounded-full" />
                        <span className="text-slate-600 text-xs">View all client portfolios</span>
                      </div>
                      <div className="flex items-center space-x-1.5">
                        <div className="w-1 h-1 bg-blue-500 rounded-full" />
                        <span className="text-slate-600 text-xs">Generate comprehensive reports</span>
                      </div>
                      <div className="flex items-center space-x-1.5">
                        <div className="w-1 h-1 bg-blue-500 rounded-full" />
                        <span className="text-slate-600 text-xs">Database management controls</span>
                      </div>
                      <div className="flex items-center space-x-1.5">
                        <div className="w-1 h-1 bg-blue-500 rounded-full" />
                        <span className="text-slate-600 text-xs">System configuration access</span>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Button */}
                <Button 
                  className="w-full h-9 bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800 text-white font-semibold rounded-xl shadow-lg hover:shadow-xl transition-all duration-300 group mt-3 text-sm"
                  onClick={() => handleRoleSelection('admin')}
                >
                  <span>Login as Administrator</span>
                  <ArrowRight className="w-3 h-3 ml-2 group-hover:translate-x-1 transition-transform duration-300" />
                </Button>
              </CardContent>
            </Card>

            {/* Client Card */}
            <Card className="group relative overflow-hidden border-0 shadow-lg hover:shadow-xl transition-all duration-300 bg-white">
              <div className="absolute inset-0 bg-gradient-to-br from-emerald-50/50 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
              <CardContent className="relative p-5 flex flex-col h-full">
                {/* Header */}
                <div className="flex items-start justify-between mb-3">
                  <div className="flex items-center space-x-3">
                    <div className="w-10 h-10 bg-gradient-to-br from-emerald-500 to-emerald-600 rounded-xl flex items-center justify-center shadow-lg">
                      <User className="w-5 h-5 text-white" />
                    </div>
                    <div>
                      <h3 className="text-lg font-bold text-slate-900 mb-1">Client</h3>
                      <Badge className="bg-emerald-100 text-emerald-700 text-xs font-medium">
                        Portfolio Access
                      </Badge>
                    </div>
                  </div>
                </div>

                {/* Description */}
                <div className="mb-3 flex-1">
                  <p className="text-slate-700 text-xs mb-9 leading-relaxed">
                    Secure access to your personal portfolio data, analytics, and comprehensive financial reports.
                  </p>
                  <div className="space-y-2">
                    <h4 className="font-semibold text-slate-900 text-xs">Your Access Includes</h4>
                    <div className="grid gap-1.5">
                      <div className="flex items-center space-x-1.5">
                        <div className="w-1 h-1 bg-emerald-500 rounded-full" />
                        <span className="text-slate-600 text-xs">Personal portfolio dashboard</span>
                      </div>
                      <div className="flex items-center space-x-1.5">
                        <div className="w-1 h-1 bg-emerald-500 rounded-full" />
                        <span className="text-slate-600 text-xs">Advanced portfolio analytics</span>
                      </div>
                      <div className="flex items-center space-x-1.5">
                        <div className="w-1 h-1 bg-emerald-500 rounded-full" />
                        <span className="text-slate-600 text-xs">Historical performance reports</span>
                      </div>
                      <div className="flex items-center space-x-1.5">
                        <div className="w-1 h-1 bg-emerald-500 rounded-full" />
                        <span className="text-slate-600 text-xs">Asset allocation insights</span>
                      </div>
                      <div className="flex items-center space-x-1.5">
                        <div className="w-1 h-1 bg-emerald-500 rounded-full" />
                        <span className="text-slate-600 text-xs">Performance benchmarking</span>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Button */}
                <Button 
                  className="w-full h-9 bg-gradient-to-r from-emerald-600 to-emerald-700 hover:from-emerald-700 hover:to-emerald-800 text-white font-semibold rounded-xl shadow-lg hover:shadow-xl transition-all duration-300 group mt-3 text-sm"
                  onClick={() => handleRoleSelection('client')}
                >
                  <span>Login as Client</span>
                  <ArrowRight className="w-3 h-3 ml-2 group-hover:translate-x-1 transition-transform duration-300" />
                </Button>
              </CardContent>
            </Card>
          </div>

          {/* Footer */}
          <div className="text-center mt-6">
            <p className="text-slate-500 text-sm">Secure access to your financial portfolio management system</p>
          </div>
        </div>
      </div>
    );
  }

  // Individual login form for selected role
  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-gray-50 via-blue-50 to-gray-100 p-4">
      <div className="max-w-md w-full space-y-6">
        <div className="text-center">
          <div className="flex items-center justify-center mb-4">
            <img 
              src="/aurum-logo.png" 
              alt="Aurum" 
              className="h-12 w-auto"
            />
          </div>
          <h2 className="text-2xl font-bold text-gray-800">
            {mode === 'admin' ? 'Administrator Login' : 'Client Login'}
          </h2>
          <p className="mt-1 text-sm text-gray-600">Aurum Portfolio Analytics</p>
        </div>
        
        <Card>
          <CardContent className="p-6">
            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label htmlFor="username" className="block text-sm font-medium text-gray-700 mb-1">
                  Username
                </label>
                <Input
                  id="username"
                  name="username"
                  type="text"
                  required
                  value={credentials.username}
                  onChange={handleChange}
                  disabled={isLoading}
                  placeholder="Enter username"
                  className="w-full"
                />
              </div>
              
              <div>
                <label htmlFor="password" className="block text-sm font-medium text-gray-700 mb-1">
                  Password
                </label>
                <Input
                  id="password"
                  name="password"
                  type="password"
                  required
                  value={credentials.password}
                  onChange={handleChange}
                  disabled={isLoading}
                  placeholder="Enter password"
                  className="w-full"
                />
              </div>

              {error && (
                <div className="bg-red-50 border border-red-200 rounded-md p-3">
                  <div className="text-sm text-red-700">{error}</div>
                </div>
              )}

              <Button
                type="submit"
                size="lg"
                disabled={isLoading}
                className="w-full"
              >
                {isLoading ? 'Signing in...' : `Sign in as ${mode === 'admin' ? 'Administrator' : 'Client'}`}
              </Button>
              
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setMode('select')}
                className="w-full"
              >
                ← Back to Access Selection
              </Button>
            </form>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}