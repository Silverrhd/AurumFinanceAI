'use client';

import React, { useState, useEffect } from 'react';
import ProtectedRoute from '@/components/auth/ProtectedRoute';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { portfolioAPI } from '@/lib/api/portfolio';
import { Client, PortfolioSummary, BankStatus, ProcessingStatus } from '@/types/portfolio';
import { ProjectAurumFileUpload } from '@/components/admin/ProjectAurumFileUpload';
import { ProjectAurumBankStatus } from '@/components/admin/ProjectAurumBankStatus';
import { ProjectAurumProcessingControls } from '@/components/admin/ProjectAurumProcessingControls';
import { ProjectAurumExcelExport } from '@/components/admin/ProjectAurumExcelExport';
import { ProjectAurumReportGeneration } from '@/components/admin/ProjectAurumReportGeneration';
import { 
  BarChart3, 
  Users, 
  Upload, 
  FileText,
  RefreshCw,
  Database,
  Settings,
  Play,
  Building,
  FileDown,
  AlertTriangle,
  LogOut
} from 'lucide-react';
import { DashboardAssetAllocationChart } from '@/components/charts/DashboardAssetAllocationChart';
import { DashboardBankAllocationChart } from '@/components/charts/DashboardBankAllocationChart';
import { DashboardBondMaturityChart } from '@/components/charts/DashboardBondMaturityChart';
import { DashboardPortfolioValueChart } from '@/components/charts/DashboardPortfolioValueChart';
import { DashboardCumulativeReturnChart } from '@/components/charts/DashboardCumulativeReturnChart';
import { DashboardPortfolioMetricsChart } from '@/components/charts/DashboardPortfolioMetricsChart';
import { AuthManager } from '@/lib/auth';
import { useRouter } from 'next/navigation';

export default function AdminDashboardPage() {
  const router = useRouter();
  const [activeTab, setActiveTab] = useState('dashboard');
  const [selectedClient, setSelectedClient] = useState<string>('ALL');
  const [clients, setClients] = useState<Client[]>([]);
  const [summary, setSummary] = useState<PortfolioSummary | null>(null);
  const [chartData, setChartData] = useState<any>(null);
  const [bankStatus, setBankStatus] = useState<BankStatus[]>([]);
  const [processingStatus, setProcessingStatus] = useState<ProcessingStatus>({ status: 'idle' });
  const [loading, setLoading] = useState(true);
  const [filterLoading, setFilterLoading] = useState(false);

  const handleLogout = () => {
    AuthManager.logout();
    router.push('/login');
  };

  useEffect(() => {
    loadInitialData();
  }, []);

  useEffect(() => {
    if (selectedClient !== undefined) {
      loadDashboardData();
    }
  }, [selectedClient]);

  const loadInitialData = async () => {
    try {
      const [clientsResponse, dashboardResponse, bankResponse] = await Promise.all([
        portfolioAPI.getClients(),
        portfolioAPI.getAdminDashboardData(),
        portfolioAPI.getBankStatus()
      ]);

      if (clientsResponse.status === 'success' && clientsResponse.data) {
        setClients([
          { id: 'all', name: 'All Clients', client_code: 'ALL' }, 
          ...clientsResponse.data.filter(client => client.client_code !== 'ALL')
        ]);
      }

      if (dashboardResponse.status === 'success' && dashboardResponse.data) {
        setSummary(dashboardResponse.data.summary);
        setChartData(dashboardResponse.data.charts);
      }

      if (bankResponse.status === 'success' && bankResponse.data) {
        setBankStatus(bankResponse.data);
      }
    } catch (error) {
      console.error('Failed to load initial data:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadDashboardData = async () => {
    setFilterLoading(true);
    try {
      const clientCode = selectedClient === 'ALL' ? undefined : selectedClient;
      const response = await portfolioAPI.getAdminDashboardData(clientCode);
      
      if (response.status === 'success' && response.data) {
        setSummary(response.data.summary);
        setChartData(response.data.charts);
      }
    } catch (error) {
      console.error('Failed to load dashboard data:', error);
    } finally {
      setFilterLoading(false);
    }
  };

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(amount);
  };

  const formatPercentage = (percentage: number) => {
    return `${percentage >= 0 ? '+' : ''}${percentage.toFixed(2)}%`;
  };

  const getBankStatusColor = (status: string) => {
    switch (status) {
      case 'COMPLETE': return 'bg-green-100 text-green-800';
      case 'PARTIAL': return 'bg-yellow-100 text-yellow-800';
      case 'EMPTY': return 'bg-blue-100 text-blue-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getBankStatusIcon = (status: string) => {
    switch (status) {
      case 'COMPLETE': return '✅';
      case 'PARTIAL': return '📋';
      case 'EMPTY': return '🆕';
      default: return '❓';
    }
  };

  const handleProcessBank = async (bankCode: string) => {
    console.log('Processing bank:', bankCode);
    setProcessingStatus({ status: 'preprocessing' });
    
    try {
      // This would call the actual bank processing API
      // const response = await portfolioAPI.processBank(bankCode);
      console.log(`Starting processing for bank: ${bankCode}`);
      
      // Simulate processing for demo
      setTimeout(() => {
        setProcessingStatus({ status: 'complete' });
        setTimeout(() => setProcessingStatus({ status: 'idle' }), 3000);
      }, 2000);
      
    } catch (error) {
      console.error('Failed to process bank:', error);
      setProcessingStatus({ status: 'error' });
    }
  };

  return (
    <ProtectedRoute requiredRole="admin">
      <div className="min-h-screen bg-gray-50">
        {/* Header */}
        <header className="bg-white shadow-sm border-b border-gray-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="flex justify-between items-center h-16">
              <div className="flex items-center">
                <h1 className="text-xl font-semibold aurum-text-dark">Aurum</h1>
                <span className="ml-2 px-2 py-1 text-xs aurum-primary text-white rounded-full">Admin</span>
              </div>
              <div className="flex items-center space-x-4">
                <span className="text-sm font-semibold text-gray-800 px-2 py-1 bg-gray-100 rounded">Admin</span>
                <button onClick={handleLogout} className="text-gray-500 hover:text-gray-700">
                  <LogOut className="h-4 w-4" />
                </button>
              </div>
            </div>
          </div>
        </header>

        {/* Navigation Tabs */}
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="bg-white rounded-lg shadow-sm border">
            <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
              <TabsList className="grid w-full grid-cols-4 h-auto p-1">
                <TabsTrigger 
                  value="dashboard" 
                  className="px-6 py-2 text-sm font-medium data-[state=active]:text-aurum-primary data-[state=active]:bg-white"
                >
                  Dashboard
                </TabsTrigger>
                <TabsTrigger 
                  value="client" 
                  className="px-6 py-2 text-sm font-medium data-[state=active]:text-aurum-primary data-[state=active]:bg-white"
                >
                  Client Management
                </TabsTrigger>
                <TabsTrigger 
                  value="admin" 
                  className="px-6 py-2 text-sm font-medium data-[state=active]:text-aurum-primary data-[state=active]:bg-white"
                >
                  Admin
                </TabsTrigger>
                <TabsTrigger 
                  value="report" 
                  className="px-6 py-2 text-sm font-medium data-[state=active]:text-aurum-primary data-[state=active]:bg-white"
                >
                  Report Generation
                </TabsTrigger>
              </TabsList>

              {/* Dashboard Tab Content */}
              <TabsContent value="dashboard" className="mt-6 space-y-6">
                {/* Client Filter Section */}
                <div className="bg-white shadow-sm border rounded-lg p-4">
                  <div className="flex items-center justify-between">
                    <h3 className="text-lg font-semibold aurum-text-dark">Dashboard View</h3>
                    <div className="flex items-center space-x-4">
                      <label htmlFor="client-filter" className="text-sm font-medium text-gray-700">
                        Filter by Client:
                      </label>
                      <Select value={selectedClient} onValueChange={setSelectedClient} disabled={filterLoading}>
                        <SelectTrigger className="w-48 aurum-border-primary">
                          {filterLoading ? (
                            <div className="flex items-center">
                              <div className="loading-spinner"></div>
                              <span>Filtering...</span>
                            </div>
                          ) : (
                            <SelectValue placeholder={clients.length > 0 ? "Select client..." : "Loading clients..."} />
                          )}
                        </SelectTrigger>
                        <SelectContent>
                          {clients.map((client) => (
                            <SelectItem key={client.id} value={client.client_code}>
                              {client.client_code === 'ALL' ? client.name : client.client_code}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </div>

                {/* Admin Metrics Cards */}
                <div className={`grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 ${filterLoading ? 'opacity-75' : ''}`}>
                  <Card>
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm font-medium text-gray-600">Total AUM</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold aurum-text-dark">
                        {loading || filterLoading ? (
                          <><span className="loading-spinner"></span>Loading...</>
                        ) : (
                          formatCurrency(summary?.total_aum || 0)
                        )}
                      </div>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm font-medium text-gray-600">SINCE INCEPTION $</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold aurum-text-dark">
                        {loading || filterLoading ? (
                          <><span className="loading-spinner"></span>Loading...</>
                        ) : (
                          formatCurrency(summary?.inception_dollar_performance || 0)
                        )}
                      </div>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm font-medium text-gray-600">SINCE INCEPTION %</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold aurum-text-dark">
                        {loading || filterLoading ? (
                          <><span className="loading-spinner"></span>Loading...</>
                        ) : (
                          formatPercentage(summary?.inception_return_pct || 0)
                        )}
                      </div>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm font-medium text-gray-600">Est Annual Income</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold aurum-text-dark">
                        {loading || filterLoading ? (
                          <><span className="loading-spinner"></span>Loading...</>
                        ) : (
                          formatCurrency(summary?.estimated_annual_income || 0)
                        )}
                      </div>
                    </CardContent>
                  </Card>
                </div>

                {/* Charts Grid (2x3) */}
                <div className={`grid grid-cols-1 lg:grid-cols-2 gap-6 ${filterLoading ? 'opacity-75' : ''}`}>
                  <Card className="p-6">
                    <CardTitle className="text-lg font-semibold aurum-text-dark mb-4">Asset Allocation</CardTitle>
                    {filterLoading ? (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        <div className="flex flex-col items-center">
                          <div className="loading-spinner mb-2"></div>
                          <span>Updating chart...</span>
                        </div>
                      </div>
                    ) : chartData?.asset_allocation ? (
                      <DashboardAssetAllocationChart data={chartData.asset_allocation} />
                    ) : (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        Loading chart data...
                      </div>
                    )}
                  </Card>
                  
                  <Card className="p-6">
                    <CardTitle className="text-lg font-semibold aurum-text-dark mb-4">Portfolio Metrics Comparison</CardTitle>
                    {filterLoading ? (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        <div className="flex flex-col items-center">
                          <div className="loading-spinner mb-2"></div>
                          <span>Updating chart...</span>
                        </div>
                      </div>
                    ) : chartData?.portfolio_metrics ? (
                      <DashboardPortfolioMetricsChart data={chartData.portfolio_metrics} />
                    ) : (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        Loading chart data...
                      </div>
                    )}
                  </Card>
                  
                  <Card className="p-6">
                    <CardTitle className="text-lg font-semibold aurum-text-dark mb-4">Portfolio Value Evolution</CardTitle>
                    {filterLoading ? (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        <div className="flex flex-col items-center">
                          <div className="loading-spinner mb-2"></div>
                          <span>Updating chart...</span>
                        </div>
                      </div>
                    ) : chartData?.portfolio_evolution ? (
                      <DashboardPortfolioValueChart data={chartData.portfolio_evolution} />
                    ) : (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        Loading chart data...
                      </div>
                    )}
                  </Card>
                  
                  <Card className="p-6">
                    <CardTitle className="text-lg font-semibold aurum-text-dark mb-4">Cumulative Return</CardTitle>
                    {filterLoading ? (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        <div className="flex flex-col items-center">
                          <div className="loading-spinner mb-2"></div>
                          <span>Updating chart...</span>
                        </div>
                      </div>
                    ) : chartData?.cumulative_return ? (
                      <DashboardCumulativeReturnChart data={chartData.cumulative_return} />
                    ) : (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        Loading chart data...
                      </div>
                    )}
                  </Card>
                  
                  {/* Row 3 - Bank Allocation */}
                  <Card className="p-6">
                    <CardTitle className="text-lg font-semibold aurum-text-dark mb-4">Bank Allocation</CardTitle>
                    {filterLoading ? (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        <div className="flex flex-col items-center">
                          <div className="loading-spinner mb-2"></div>
                          <span>Updating chart...</span>
                        </div>
                      </div>
                    ) : chartData?.bank_allocation ? (
                      <DashboardBankAllocationChart data={chartData.bank_allocation} />
                    ) : (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        Loading chart data...
                      </div>
                    )}
                  </Card>
                  
                  {/* Row 3 - Bond Maturity Distribution */}
                  <Card className="p-6">
                    <CardTitle className="text-lg font-semibold aurum-text-dark mb-4">Bond Maturity Distribution</CardTitle>
                    {filterLoading ? (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        <div className="flex flex-col items-center">
                          <div className="loading-spinner mb-2"></div>
                          <span>Updating chart...</span>
                        </div>
                      </div>
                    ) : chartData?.bond_maturity_distribution ? (
                      <DashboardBondMaturityChart data={chartData.bond_maturity_distribution} />
                    ) : (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        Loading chart data...
                      </div>
                    )}
                  </Card>
                </div>
              </TabsContent>

              {/* Client Management Tab Content */}
              <TabsContent value="client" className="mt-6">
                <Card className="p-6">
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-lg font-semibold aurum-text-dark">
                      <Users className="h-5 w-5" />
                      Client Management
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="text-center py-12">
                      <Users className="h-16 w-16 mx-auto text-gray-400 mb-4" />
                      <h3 className="text-lg font-medium text-gray-900 mb-2">Coming Soon</h3>
                      <p className="text-gray-500">
                        Client management features are under development. This will include adding, editing, and managing client accounts.
                      </p>
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>

              {/* Admin Tab Content - File Upload & Bank Status */}
              <TabsContent value="admin" className="mt-6 space-y-6">
                {/* File Upload Section */}
                <ProjectAurumFileUpload />

                {/* Bank Status Monitor */}
                <ProjectAurumBankStatus onProcessBank={handleProcessBank} />

                {/* Processing Controls */}
                <ProjectAurumProcessingControls />

                {/* Excel Export Section */}
                <ProjectAurumExcelExport />
              </TabsContent>

              {/* Report Generation Tab Content */}
              <TabsContent value="report" className="mt-6 space-y-6">
                <ProjectAurumReportGeneration />
              </TabsContent>
            </Tabs>
          </div>
        </div>
      </div>
    </ProtectedRoute>
  );
}