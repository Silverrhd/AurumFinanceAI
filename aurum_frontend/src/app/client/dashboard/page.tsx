'use client';

import React, { useState, useEffect } from 'react';
import ProtectedRoute from '@/components/auth/ProtectedRoute';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { portfolioAPI } from '@/lib/api/portfolio';
import { PortfolioSummary } from '@/types/portfolio';
import { 
  FileText,
  ExternalLink,
  Building,
  Calendar,
  PieChart,
  LogOut
} from 'lucide-react';
import { DashboardAssetAllocationChart } from '@/components/charts/DashboardAssetAllocationChart';
import { DashboardPortfolioValueChart } from '@/components/charts/DashboardPortfolioValueChart';
import { DashboardCumulativeReturnChart } from '@/components/charts/DashboardCumulativeReturnChart';
import { DashboardPortfolioMetricsChart } from '@/components/charts/DashboardPortfolioMetricsChart';
import { AuthManager } from '@/lib/auth';
import { useRouter } from 'next/navigation';

export default function ClientDashboardPage() {
  const router = useRouter();
  const [activeTab, setActiveTab] = useState('dashboard');
  const [summary, setSummary] = useState<PortfolioSummary | null>(null);
  const [chartData, setChartData] = useState<any>(null);
  const [selectedReportDate, setSelectedReportDate] = useState<string>('');
  const [reportDates, setReportDates] = useState<string[]>([]);
  const [dateToReportId, setDateToReportId] = useState<Record<string, number>>({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadClientData();
  }, []);

  const loadClientData = async () => {
    try {
      const [dashboardResponse, weeklyReportsResponse] = await Promise.all([
        portfolioAPI.getClientDashboardWithCharts(),
        portfolioAPI.getGeneratedWeeklyReports()
      ]);

      if (dashboardResponse.status === 'success' && dashboardResponse.data) {
        // Use admin dashboard data structure directly (no mapping needed)
        setSummary(dashboardResponse.data.summary);
        setChartData(dashboardResponse.data.charts);
      }

      if (weeklyReportsResponse.status === 'success' && weeklyReportsResponse.data) {
        const reports = weeklyReportsResponse.data.reports || [];
        const uniqueDates = Array.from(new Set(reports.map((r: any) => r.report_date))).sort().reverse();
        const mapping: Record<string, number> = {};
        for (const r of reports) {
          // prefer latest id for a given date (if duplicates)
          mapping[r.report_date] = r.id;
        }
        setReportDates(uniqueDates);
        setDateToReportId(mapping);
      }
    } catch (error) {
      console.error('Failed to load client data:', error);
    } finally {
      setLoading(false);
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
    const formatted = (percentage ?? 0).toFixed(2);
    const color = (percentage ?? 0) >= 0 ? 'text-green-600' : 'text-red-600';
    return (
      <span className={color}>
        {(percentage ?? 0) >= 0 ? '+' : ''}{formatted}%
      </span>
    );
  };

  const openReportById = async (reportId: number) => {
    const response = await portfolioAPI.getReportFile(reportId);
    if (response.status === 'success' && response.data) {
      const html = response.data.html_content;
      const newWindow = window.open('', '_blank');
      if (newWindow) {
        newWindow.document.open();
        newWindow.document.write(html);
        newWindow.document.close();
      }
    }
  };

  const handleOpenInvestmentReport = async () => {
    if (!selectedReportDate) return;
    const reportId = dateToReportId[selectedReportDate];
    if (reportId) {
      await openReportById(reportId);
    }
  };

  const handleOpenBondIssuerReport = async () => {
    const resp = await portfolioAPI.getGeneratedReportsByType('bond_issuer_weight');
    if (resp.status === 'success' && resp.data && resp.data.reports && resp.data.reports.length > 0) {
      // assume latest-first; if not, sort by created_at desc
      const reports = resp.data.reports.slice().sort((a: any, b: any) => (b.created_at || '').localeCompare(a.created_at || ''));
      await openReportById(reports[0].id);
    }
  };

  const clientCode = AuthManager.getClientCode() || '';

  const handleLogout = () => {
    AuthManager.logout();
    router.push('/login');
  };

  return (
    <ProtectedRoute requiredRole="client">
      <div className="min-h-screen bg-gray-50">
        {/* Header */}
        <header className="bg-white shadow-sm border-b border-gray-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="flex justify-between items-center h-16">
              <div className="flex items-center">
                <h1 className="text-xl font-semibold aurum-text-dark">Aurum</h1>
                <span className="ml-2 px-2 py-1 text-xs aurum-primary text-white rounded-full">Client</span>
              </div>
              <div className="flex items-center space-x-4">
                <span className="text-sm font-semibold text-gray-800 px-2 py-1 bg-gray-100 rounded">{clientCode || '—'}</span>
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
              <TabsList className="grid w-full grid-cols-2 h-auto p-1">
                <TabsTrigger 
                  value="dashboard" 
                  className="px-6 py-2 text-sm font-medium data-[state=active]:text-aurum-primary data-[state=active]:bg-white"
                >
                  Dashboard
                </TabsTrigger>
                <TabsTrigger 
                  value="report" 
                  className="px-6 py-2 text-sm font-medium data-[state=active]:text-aurum-primary data-[state=active]:bg-white"
                >
                  Reports
                </TabsTrigger>
              </TabsList>

              {/* Dashboard Tab Content */}
              <TabsContent value="dashboard" className="mt-6 space-y-6">
                {/* Client Metrics Cards */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                  <Card>
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm font-medium text-gray-600">Total Portfolio Value</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="text-2xl font-bold aurum-text-dark">
                        {loading ? (
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
                        {loading ? (
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
                        {loading ? (
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
                        {loading ? (
                          <><span className="loading-spinner"></span>Loading...</>
                        ) : (
                          formatCurrency(summary?.estimated_annual_income || 0)
                        )}
                      </div>
                    </CardContent>
                  </Card>
                </div>

                {/* Charts Grid (2x2) */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  <Card className="p-6">
                    <CardTitle className="text-lg font-semibold aurum-text-dark mb-4">Asset Allocation Chart</CardTitle>
                    {loading ? (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        <div className="flex flex-col items-center">
                          <div className="loading-spinner mb-2"></div>
                          <span>Loading chart...</span>
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
                    <CardTitle className="text-lg font-semibold aurum-text-dark mb-4">Portfolio Metrics Chart</CardTitle>
                    {loading ? (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        <div className="flex flex-col items-center">
                          <div className="loading-spinner mb-2"></div>
                          <span>Loading chart...</span>
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
                    <CardTitle className="text-lg font-semibold aurum-text-dark mb-4">Cumulative Return Chart</CardTitle>
                    {loading ? (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        <div className="flex flex-col items-center">
                          <div className="loading-spinner mb-2"></div>
                          <span>Loading chart...</span>
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
                  
                  <Card className="p-6">
                    <CardTitle className="text-lg font-semibold aurum-text-dark mb-4">Portfolio Value Chart</CardTitle>
                    {loading ? (
                      <div className="h-64 flex items-center justify-center text-gray-500">
                        <div className="flex flex-col items-center">
                          <div className="loading-spinner mb-2"></div>
                          <span>Loading chart...</span>
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
                </div>
              </TabsContent>

              {/* Reports Tab Content */}
              <TabsContent value="report" className="mt-6">
                {/* 2x3 Grid Layout for Report Cards - Added Cash Position Report */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                  {/* Investment Reports Card */}
                  <Card className="p-6">
                    <CardHeader>
                      <CardTitle className="text-lg font-semibold aurum-text-dark mb-4 flex items-center gap-2">
                        <FileText className="h-5 w-5" />
                        📋 Weekly Investment Reports
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div>
                        <label htmlFor="report-date-selector" className="block text-sm font-medium text-gray-700 mb-2">
                          Select Date:
                        </label>
                        <Select value={selectedReportDate} onValueChange={setSelectedReportDate}>
                          <SelectTrigger className="w-full">
                            <SelectValue placeholder={loading ? 'Loading dates...' : 'Select a date'} />
                          </SelectTrigger>
                          <SelectContent>
                            {reportDates.map((date) => (
                              <SelectItem key={date} value={date}>
                                {(() => {
                                  // Parse YYYY-MM-DD string directly to avoid timezone issues
                                  // Backend sends: "2025-07-24", we want: "24/07/2025"
                                  const parts = date.split('-');
                                  if (parts.length === 3) {
                                    const [year, month, day] = parts;
                                    return `${day}/${month}/${year}`;
                                  }
                                  return date; // fallback to original if format is unexpected
                                })()}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      
                      <Button 
                        onClick={handleOpenInvestmentReport}
                        disabled={!selectedReportDate}
                        className="w-full"
                      >
                        <ExternalLink className="h-4 w-4 mr-2" />
                        Open Report
                      </Button>
                      
                      <div className="pt-4 border-t border-gray-200">
                        <p className="text-xs text-gray-500">
                          Access your personalized weekly investment reports with comprehensive portfolio analysis and performance metrics.
                        </p>
                      </div>
                    </CardContent>
                  </Card>

                  {/* Bond Issuer Weight Reports Card */}
                  <Card className="p-6">
                    <CardHeader>
                      <CardTitle className="text-lg font-semibold aurum-text-dark mb-4 flex items-center gap-2">
                        <Building className="h-5 w-5" />
                        🏢 Bond Issuer Weight Reports
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <Button 
                        onClick={handleOpenBondIssuerReport}
                        className="w-full"
                      >
                        <Building className="h-4 w-4 mr-2" />
                        Open Bond Issuer Weight Report
                      </Button>
                      
                      <div className="text-xs text-center text-gray-500">
                        View your bond portfolio grouped by issuer with concentration analysis
                      </div>
                      
                      <div className="pt-4 border-t border-gray-200">
                        <p className="text-xs text-gray-500">
                          Comprehensive bond issuer weight analysis with concentration risk assessment, showing your bond portfolio grouped by issuer with weight percentages.
                        </p>
                      </div>
                    </CardContent>
                  </Card>

                  {/* Bond Maturity Reports Card */}
                  <Card className="p-6 opacity-60 pointer-events-none">
                    <CardHeader>
                      <CardTitle className="text-lg font-semibold aurum-text-dark mb-4 flex items-center gap-2">
                        <Calendar className="h-5 w-5" />
                        📅 Bond Maturity Reports (coming soon)
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <Button className="w-full" disabled>
                        <Calendar className="h-4 w-4 mr-2" />
                        Open Bond Maturity Report
                      </Button>
                      <div className="text-xs text-center text-gray-500">
                        Pending implementation
                      </div>
                    </CardContent>
                  </Card>

                  {/* Equity Breakdown Reports Card */}
                  <Card className="p-6 opacity-60 pointer-events-none">
                    <CardHeader>
                      <CardTitle className="text-lg font-semibold aurum-text-dark mb-4 flex items-center gap-2">
                        <PieChart className="h-5 w-5" />
                        📊 Equity Breakdown Reports (coming soon)
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <Button className="w-full" disabled>
                        <PieChart className="h-4 w-4 mr-2" />
                        Open Equity Breakdown Report
                      </Button>
                      <div className="text-xs text-center text-gray-500">
                        Pending implementation
                      </div>
                    </CardContent>
                  </Card>

                  {/* Cash Position Reports Card */}
                  <Card className="p-6 opacity-60 pointer-events-none">
                    <CardHeader>
                      <CardTitle className="text-lg font-semibold aurum-text-dark mb-4 flex items-center gap-2">
                        <DollarSign className="h-5 w-5" />
                        💰 Cash Position Reports (coming soon)
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <Button className="w-full" disabled>
                        <DollarSign className="h-4 w-4 mr-2" />
                        Open Cash Position Report
                      </Button>
                      <div className="text-xs text-center text-gray-500">
                        Pending implementation
                      </div>
                    </CardContent>
                  </Card>
                </div>
              </TabsContent>
            </Tabs>
          </div>
        </div>
      </div>
    </ProtectedRoute>
  );
}