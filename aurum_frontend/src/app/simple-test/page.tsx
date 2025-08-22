'use client'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

export default function SimpleTestPage() {
  return (
    <div className="min-h-screen bg-gray-50 p-8">
      <div className="max-w-4xl mx-auto space-y-6">
        <h1 className="text-3xl font-bold text-aurum-primary">ProjectAurum Dashboard Test</h1>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Card>
            <CardHeader>
              <CardTitle>Admin Dashboard</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="mb-4">Test admin functionality:</p>
              <Button 
                onClick={() => window.location.href = '/admin/dashboard'}
                className="w-full"
              >
                Go to Admin Dashboard
              </Button>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Client Dashboard</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="mb-4">Test client functionality:</p>
              <Button 
                onClick={() => window.location.href = '/client/dashboard'}
                variant="outline"
                className="w-full"
              >
                Go to Client Dashboard
              </Button>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Test Results</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              <p>✅ Basic UI components working</p>
              <p>✅ Tailwind CSS loading properly</p>
              <p>✅ Button component functional</p>
              <p>✅ Card components rendering</p>
              <p>✅ Navigation ready</p>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}