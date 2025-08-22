'use client';

import { useState } from 'react';
import { AuthService } from '@/lib/services/auth';
import { DashboardService } from '@/lib/services/dashboard';
import { Button } from '@/components/ui/button';

export default function TestPage() {
  const [testResults, setTestResults] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  const addResult = (result: string) => {
    setTestResults(prev => [...prev, result]);
  };

  const testApiConnection = async () => {
    setIsLoading(true);
    setTestResults([]);
    
    try {
      addResult('🧪 Testing API connection...');
      
      // Test 1: Try to call a public endpoint
      try {
        const response = await fetch('http://localhost:8000/api/health/');
        if (response.ok) {
          addResult('✅ Django server is running and accessible');
        } else {
          addResult(`❌ Django server responded with status: ${response.status}`);
        }
      } catch (error) {
        addResult('❌ Cannot connect to Django server - make sure it\'s running on port 8000');
      }

      // Test 2: Test CORS headers
      try {
        const response = await fetch('http://localhost:8000/api/health/', {
          headers: {
            'Content-Type': 'application/json',
          }
        });
        
        if (response.headers.get('Access-Control-Allow-Origin')) {
          addResult('✅ CORS is configured correctly');
        } else {
          addResult('⚠️ CORS might need configuration');
        }
      } catch (error) {
        addResult('❌ CORS test failed');
      }

      // Test 3: Test login endpoint structure (without actual login)
      try {
        const response = await fetch('http://localhost:8000/api/auth/login/', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ username: 'test', password: 'test' })
        });
        
        // We expect either 400 (validation error) or 401 (invalid credentials)
        // Both indicate the endpoint exists and is working
        if (response.status === 400 || response.status === 401) {
          addResult('✅ Login endpoint is accessible');
        } else if (response.status === 404) {
          addResult('❌ Login endpoint not found');
        } else {
          addResult(`⚠️ Login endpoint returned status: ${response.status}`);
        }
      } catch (error) {
        addResult('❌ Cannot access login endpoint');
      }

      addResult('🎉 API connection test completed!');
      
    } catch (error) {
      addResult(`❌ Test failed: ${error}`);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 p-8">
      <div className="max-w-4xl mx-auto">
        <h1 className="text-3xl font-bold text-gray-900 mb-8">
          Aurum Frontend - Integration Test
        </h1>
        
        <div className="bg-white rounded-lg shadow p-6 mb-6">
          <h2 className="text-xl font-semibold mb-4">API Connection Test</h2>
          <p className="text-gray-600 mb-4">
            This test checks if the Next.js frontend can communicate with the Django backend.
          </p>
          
          <Button
            onClick={testApiConnection}
            disabled={isLoading}
            className="mb-4"
          >
            Test API Connection
          </Button>
          
          {testResults.length > 0 && (
            <div className="bg-gray-50 rounded p-4">
              <h3 className="font-medium mb-2">Test Results:</h3>
              <div className="space-y-1">
                {testResults.map((result, index) => (
                  <div key={index} className="font-mono text-sm">
                    {result}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Implementation Status</h2>
          <div className="space-y-2">
            <div className="flex items-center space-x-2">
              <span className="text-green-600">✅</span>
              <span>Next.js 14+ project with TypeScript</span>
            </div>
            <div className="flex items-center space-x-2">
              <span className="text-green-600">✅</span>
              <span>Tailwind CSS with Aurum color scheme</span>
            </div>
            <div className="flex items-center space-x-2">
              <span className="text-green-600">✅</span>
              <span>Authentication flow (login/logout/protected routes)</span>
            </div>
            <div className="flex items-center space-x-2">
              <span className="text-green-600">✅</span>
              <span>API client with Axios and interceptors</span>
            </div>
            <div className="flex items-center space-x-2">
              <span className="text-green-600">✅</span>
              <span>Service layer for Django API integration</span>
            </div>
            <div className="flex items-center space-x-2">
              <span className="text-green-600">✅</span>
              <span>TypeScript types for API responses</span>
            </div>
            <div className="flex items-center space-x-2">
              <span className="text-green-600">✅</span>
              <span>Error handling with automatic token refresh</span>
            </div>
            <div className="flex items-center space-x-2">
              <span className="text-green-600">✅</span>
              <span>Project structure for scalable development</span>
            </div>
          </div>
          
          <div className="mt-6 p-4 bg-blue-50 rounded">
            <h3 className="font-semibold text-blue-900 mb-2">Ready for Next Steps:</h3>
            <ul className="text-blue-800 space-y-1">
              <li>• Task 10: Build admin dashboard UI components</li>
              <li>• Task 11: Build client dashboard UI components</li>
              <li>• Integration with Django backend APIs (Task 8)</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}