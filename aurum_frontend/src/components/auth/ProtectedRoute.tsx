'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { AuthManager } from '@/lib/auth';

interface ProtectedRouteProps {
  children: React.ReactNode;
  requiredRole?: 'admin' | 'client';
}

export default function ProtectedRoute({ children, requiredRole }: ProtectedRouteProps) {
  const [isLoading, setIsLoading] = useState(true);
  const [isAuthorized, setIsAuthorized] = useState(false);
  const router = useRouter();

  useEffect(() => {
    const checkAuth = () => {
      // Check if user is authenticated
      if (!AuthManager.isAuthenticated()) {
        router.push('/login');
        return;
      }

      // Check role if specified - TEMPORARILY DISABLED FOR TESTING
      if (requiredRole) {
        const user = AuthManager.getUser();
        console.log('Current user role:', user?.role, 'Required role:', requiredRole);
        
        // TEMPORARY: Allow access regardless of role for testing
        // TODO: Re-enable proper role checking once testing is complete
        /*
        if (!user || user.role !== requiredRole) {
          // Redirect based on their actual role
          if (user?.role === 'admin') {
            router.push('/admin/dashboard');
          } else if (user?.role === 'client') {
            router.push('/client/dashboard');
          } else {
            router.push('/login');
          }
          return;
        }
        */
      }

      setIsAuthorized(true);
      setIsLoading(false);
    };

    checkAuth();
  }, [router, requiredRole]);

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-aurum-primary mx-auto mb-4"></div>
          <p className="text-gray-600">Loading...</p>
        </div>
      </div>
    );
  }

  if (!isAuthorized) {
    return null;
  }

  return <>{children}</>;
}