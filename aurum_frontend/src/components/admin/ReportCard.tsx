'use client';

import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Label } from '@/components/ui/label';
import { FileText, FolderOpen } from 'lucide-react';
import { portfolioAPI } from '@/lib/api/portfolio';
import { Client } from '@/types/portfolio';
import toast from 'react-hot-toast';

interface ReportCardProps {
  title: string;
  icon: React.ReactNode;
  reportType: string;
  generateLabel: string;
  openLabel: string;
}

interface WeeklyReport {
  id: number;
  client_code: string;
  client_name: string;
  report_date: string;
  file_path: string;
  file_size: number;
  generation_time: number;
  created_at: string;
}

export function ReportCard({ title, icon, reportType, generateLabel, openLabel }: ReportCardProps) {
  const [generateExpanded, setGenerateExpanded] = useState(false);
  const [openExpanded, setOpenExpanded] = useState(false);
  const [selectedDate, setSelectedDate] = useState('');
  const [selectedClient, setSelectedClient] = useState('');
  const [selectedReportId, setSelectedReportId] = useState('');
  const [selectedClientForBrowse, setSelectedClientForBrowse] = useState('');
  const [selectedYear, setSelectedYear] = useState('2025');
  const [selectedMonth, setSelectedMonth] = useState('08');
  
  // Helper function to determine if this is a latest-only report type
  const isLatestOnlyReport = (reportType: string) => {
    return ['bond_issuer_weight', 'bond_maturity', 'equity_breakdown', 'cash_position', 'total_positions'].includes(reportType);
  };
  
  // Data states
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [clients, setClients] = useState<Client[]>([]);
  const [existingReports, setExistingReports] = useState<WeeklyReport[]>([]);
  const [filteredReports, setFilteredReports] = useState<WeeklyReport[]>([]);
  
  const uniqueClients = React.useMemo(() => {
    const clientMap = new Map<string, { code: string; name: string }>();
    for (const report of existingReports) {
      if (!clientMap.has(report.client_code)) {
        clientMap.set(report.client_code, { code: report.client_code, name: report.client_name });
      }
    }
    return Array.from(clientMap.values());
  }, [existingReports]);
  
  // Loading states
  const [isGenerating, setIsGenerating] = useState(false);
  const [loadingData, setLoadingData] = useState(false);
  
  // Progress tracking state
  const [bulkProgress, setBulkProgress] = useState<{
    total: number;
    completed: number;
    current: string;
    results: Array<{client: string, status: string}>;
  }>({ total: 0, completed: 0, current: '', results: [] });

  // Filter reports by client selection
  useEffect(() => {
    const filtered = (selectedClientForBrowse && selectedClientForBrowse !== 'all-clients')
      ? existingReports.filter(r => r.client_code === selectedClientForBrowse)
      : existingReports;
    setFilteredReports(filtered);
    setSelectedReportId(''); // Reset selection when filtering
  }, [selectedClientForBrowse, existingReports]);

  const loadAvailableDates = async () => {
    // Skip loading dates for latest-only report types
    if (isLatestOnlyReport(reportType)) {
      setAvailableDates([]);
      return;
    }
    
    try {
      const response = await portfolioAPI.getAvailableDatesByType(reportType, selectedClient);
      if (response.status === 'success' && response.data) {
        setAvailableDates(response.data.available_dates);
      }
    } catch (error) {
      console.error('Failed to load available dates:', error);
    }
  };

  const loadClients = async () => {
    try {
      const response = await portfolioAPI.getClients();
      if (response.status === 'success' && response.data) {
        setClients(response.data);
      }
    } catch (error) {
      console.error('Failed to load clients:', error);
    }
  };

  const loadGeneratedReports = async () => {
    try {
      const response = await portfolioAPI.getGeneratedReportsByType(
        reportType,
        isLatestOnlyReport(reportType) ? undefined : selectedClientForBrowse
      );
      if (response.status === 'success' && response.data) {
        setExistingReports(response.data.reports);
      }
    } catch (error) {
      console.error('Failed to load generated reports:', error);
    }
  };

  const toggleGenerate = () => {
    setGenerateExpanded(!generateExpanded);
    if (!generateExpanded) {
      setLoadingData(true);
      Promise.all([
        loadAvailableDates(),
        loadClients()
      ]).finally(() => {
        setLoadingData(false);
      });
    }
  };

  const toggleOpen = () => {
    setOpenExpanded(!openExpanded);
    if (!openExpanded) {
      // For latest-only reports, clear selection to ensure unfiltered fetch and full client list
      if (isLatestOnlyReport(reportType)) {
        setSelectedClientForBrowse('');
      }
      setLoadingData(true);
      Promise.all([
        loadClients(),
        loadGeneratedReports()
      ]).finally(() => {
        setLoadingData(false);
      });
    }
  };

  const handleGenerateReport = async () => {
    // Check if report type is implemented
    const implementedTypes = ['weekly_investment', 'bond_issuer_weight', 'bond_maturity', 'cash_position', 'monthly_returns_custody', 'total_positions', 'equity_breakdown'];
    if (!implementedTypes.includes(reportType)) {
      toast.error(`${title} generation is not yet implemented. Coming soon!`);
      return;
    }
    
    // Validation based on report type
    if (reportType === 'monthly_returns_custody') {
      if (!selectedClient || !selectedYear || !selectedMonth) {
        toast.error('Please select client, year, and month');
        return;
      }
    } else if (isLatestOnlyReport(reportType)) {
      if (!selectedClient) {
        toast.error('Please select a client');
        return;
      }
    } else {
      // Other reports need both date and client
      if (!selectedDate || !selectedClient) {
        toast.error('Please select both date and client');
        return;
      }
    }

    setIsGenerating(true);

    try {
      // Show loading toast for bulk generation
      if (selectedClient === 'ALL') {
        toast.loading('Preparing bulk generation...', { id: 'bulk-generation' });
      }
      
      const requestData: any = {
        report_type: reportType,
        client_code: selectedClient
      };
      
      // Add date parameters based on report type
      if (reportType === 'monthly_returns_custody') {
        requestData.year = selectedYear;
        requestData.month = selectedMonth;
      } else if (reportType !== 'bond_issuer_weight' && reportType !== 'cash_position') {
        requestData.current_date = selectedDate;
      }
      
      const response = await portfolioAPI.generateReportNoOpen(requestData);

      if (response.status === 'success') {
        // Handle bulk generation results
        if (response.data?.results && Array.isArray(response.data.results)) {
          setBulkProgress({
            total: response.data.results.length,
            completed: response.data.results.length,
            current: '',
            results: response.data.results
          });
          
          // Use enhanced summary if available
          if (response.data.summary) {
            const { success, already_exists, errors, total, generation_time } = response.data.summary;
            toast.success(`Bulk generation completed! ${total} clients processed in ${generation_time.toFixed(1)}s`, 
              { id: 'bulk-generation' });
            
            setTimeout(() => {
              toast.success(`✅ ${success} generated, ⚠️ ${already_exists} already existed, ❌ ${errors} errors`);
            }, 1000);
          } else {
            // Fallback to manual counting
            toast.success(`Bulk generation completed! ${response.data.results.length} reports processed`, 
              { id: 'bulk-generation' });
            
            const successCount = response.data.results.filter(r => r.status === 'success').length;
            const errorCount = response.data.results.filter(r => r.status === 'error').length;
            const existingCount = response.data.results.filter(r => r.status.includes('already_exists')).length;
            
            setTimeout(() => {
              toast.success(`✅ ${successCount} generated, ⚠️ ${existingCount} already existed, ❌ ${errorCount} errors`);
            }, 1000);
          }
          
        } else {
          // Single report generation
          toast.success(`Report generated successfully! ${response.data?.message || ''}`);
        }
        
        // Reset form
        setSelectedDate('');
        setSelectedClient('');
        if (reportType === 'monthly_returns_custody') {
          setSelectedYear('2025');
          setSelectedMonth('08');
        }
        setGenerateExpanded(false);
        
        // Clear progress after delay
        setTimeout(() => {
          setBulkProgress({ total: 0, completed: 0, current: '', results: [] });
        }, 5000);
        
        // Refresh available dates
        await loadAvailableDates();
      } else {
        toast.error(`Generation failed: ${response.error}`);
      }
    } catch (error) {
      console.error('Report generation error:', error);
      toast.error('Failed to generate report. Please try again.');
    } finally {
      setIsGenerating(false);
    }
  };

  const handleOpenReport = async () => {
    if (!selectedReportId) {
      toast.error('Please select a report');
      return;
    }

    try {
      // Universal pre-open window approach for all devices
      const newWindow = window.open('', '_blank');
      const token = localStorage.getItem('access_token');
      const reportUrl = `${process.env.NEXT_PUBLIC_API_URL}/api/portfolio/reports/${selectedReportId}/html/`;
      
      try {
        const response = await fetch(reportUrl, {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        });
        
        if (response.ok) {
          const htmlContent = await response.text();
          newWindow.document.write(htmlContent);
          newWindow.document.close();
        } else {
          newWindow.close();
          throw new Error('Failed to load report');
        }
      } catch (error) {
        newWindow.close();
        throw error;
      }
      
      toast.success('Report opened successfully!');
    } catch (error) {
      console.error('Report opening error:', error);
      toast.error('Failed to open report. Please try again.');
    }
  };

  const handleOpenLatestReport = async () => {
    if (!selectedClientForBrowse) {
      toast.error('Please select a client');
      return;
    }

    try {
      // For latest-only reports, use the specific API method
      if (reportType === 'bond_issuer_weight') {
        // Universal pre-open window approach - open BEFORE any API calls
        const newWindow = window.open('', '_blank');
        
        try {
          const response = await portfolioAPI.getBondIssuerWeightReport(selectedClientForBrowse);
          if (response.ok) {
            const jsonData = await response.json();
            if (jsonData.success && jsonData.report_id) {
              // Fetch report content from the HTML endpoint
              const token = localStorage.getItem('access_token');
              const reportUrl = `${process.env.NEXT_PUBLIC_API_URL}/api/portfolio/reports/${jsonData.report_id}/html/`;
              const htmlResponse = await fetch(reportUrl, {
                headers: { 'Authorization': `Bearer ${token}` }
              });
              const htmlContent = await htmlResponse.text();
              
              if (newWindow) {
                newWindow.document.write(htmlContent);
                newWindow.document.close();
              }
              toast.success('Bond Issuer Weight report opened successfully!');
            } else {
              if (newWindow) newWindow.close();
              throw new Error('Invalid report data received');
            }
          } else {
            if (newWindow) newWindow.close();
            throw new Error('Failed to open report');
          }
        } catch (error) {
          if (newWindow) newWindow.close();
          throw error;
        }
      } else {
        // For other latest-only reports, find the latest report and open it
        const latestReport = filteredReports.find(r => r.client_code === selectedClientForBrowse);
        if (latestReport) {
          // Universal pre-open window approach for other latest-only reports
          const newWindow = window.open('', '_blank');
          try {
            const token = localStorage.getItem('access_token');
            const reportUrl = `${process.env.NEXT_PUBLIC_API_URL}/api/portfolio/reports/${latestReport.id}/html/`;
            const response = await fetch(reportUrl, {
              headers: { 'Authorization': `Bearer ${token}` }
            });
            const htmlContent = await response.text();
            newWindow.document.write(htmlContent);
            newWindow.document.close();
          } catch (error) {
            newWindow.close();
            throw error;
          }
          toast.success('Report opened successfully!');
        } else {
          toast.error('No report found for this client');
        }
      }
    } catch (error) {
      console.error('Latest report opening error:', error);
      toast.error('Failed to open report. Please try again.');
    }
  };

  return (
    <Card className="bg-white shadow-sm border rounded-lg">
      <CardHeader className="px-6 py-4">
        <CardTitle className="flex items-center gap-2 text-lg font-semibold aurum-text-dark">
          {icon}
          {title}
        </CardTitle>
      </CardHeader>
      
      <CardContent className="px-6 pb-6 space-y-4">
        {/* Generate Section */}
        <Button
          onClick={toggleGenerate}
          className={`w-full flex items-center justify-start h-12 px-4 border border-gray-300 rounded-md hover:bg-gray-50 transition-colors ${
            generateExpanded ? 'bg-blue-50 border-blue-300' : ''
          }`}
          variant="outline"
        >
          <FileText className="mr-2 h-4 w-4" />
          {generateLabel}
        </Button>

        {/* Expandable Generate Form */}
        {generateExpanded && (
          <div className="bg-gray-50 border border-gray-200 rounded-md p-4 space-y-3">
            {/* Progress tracking UI */}
            {bulkProgress.total > 0 && (
              <div className="bg-blue-50 border border-blue-200 rounded-md p-3 mb-3">
                <div className="flex items-center mb-2">
                  <div className="loading-spinner mr-2"></div>
                  <span className="text-sm text-blue-700">
                    Bulk generation completed ({bulkProgress.completed}/{bulkProgress.total})
                  </span>
                </div>
                <div className="w-full bg-blue-200 rounded-full h-2 mt-2">
                  <div 
                    className="bg-blue-600 h-2 rounded-full transition-all duration-300" 
                    style={{width: `${(bulkProgress.completed / bulkProgress.total) * 100}%`}}
                  ></div>
                </div>
                {bulkProgress.results.length > 0 && (
                  <div className="mt-2 text-xs text-blue-600">
                    <div>✅ Generated: {bulkProgress.results.filter(r => r.status === 'success').length}</div>
                    <div>⚠️ Already existed: {bulkProgress.results.filter(r => r.status.includes('already_exists')).length}</div>
                    <div>❌ Errors: {bulkProgress.results.filter(r => r.status === 'error').length}</div>
                  </div>
                )}
              </div>
            )}
            {/* Monthly Returns Custody - Year and Month selection */}
            {reportType === 'monthly_returns_custody' && (
              <>
                <div>
                  <Label className="block text-xs font-medium text-gray-700 mb-1">Select Year:</Label>
                  <Select value={selectedYear} onValueChange={setSelectedYear}>
                    <SelectTrigger>
                      <SelectValue placeholder="Choose year" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="2025">2025</SelectItem>
                      <SelectItem value="2024">2024</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                
                <div>
                  <Label className="block text-xs font-medium text-gray-700 mb-1">Select Month:</Label>
                  <Select value={selectedMonth} onValueChange={setSelectedMonth}>
                    <SelectTrigger>
                      <SelectValue placeholder="Choose month" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="01">January</SelectItem>
                      <SelectItem value="02">February</SelectItem>
                      <SelectItem value="03">March</SelectItem>
                      <SelectItem value="04">April</SelectItem>
                      <SelectItem value="05">May</SelectItem>
                      <SelectItem value="06">June</SelectItem>
                      <SelectItem value="07">July</SelectItem>
                      <SelectItem value="08">August</SelectItem>
                      <SelectItem value="09">September</SelectItem>
                      <SelectItem value="10">October</SelectItem>
                      <SelectItem value="11">November</SelectItem>
                      <SelectItem value="12">December</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </>
            )}
            
            {/* Date selection - only show for weekly reports */}
            {reportType === 'weekly_investment' && (
              <div>
                <Label className="block text-xs font-medium text-gray-700 mb-1">Select Date:</Label>
                <Select value={selectedDate} onValueChange={setSelectedDate}>
                  <SelectTrigger>
                    <SelectValue placeholder="Choose date" />
                  </SelectTrigger>
                  <SelectContent>
                    {loadingData ? (
                      <SelectItem value="loading" disabled>Loading dates...</SelectItem>
                    ) : availableDates.length > 0 ? (
                      availableDates.map(date => (
                        <SelectItem key={date} value={date}>{date}</SelectItem>
                      ))
                    ) : (
                      <SelectItem value="no-dates" disabled>No dates available</SelectItem>
                    )}
                  </SelectContent>
                </Select>
              </div>
            )}
            
            {/* Info message for bond issuer weight reports */}
            {reportType === 'bond_issuer_weight' && (
              <div className="bg-blue-50 border border-blue-200 rounded-md p-3">
                <p className="text-sm text-blue-700">
                  ℹ️ Bond Issuer Weight reports use the latest available portfolio data automatically.
                </p>
              </div>
            )}
            
            {reportType === 'cash_position' && (
              <div className="bg-blue-50 border border-blue-200 rounded-md p-3">
                <p className="text-sm text-blue-700">
                  ℹ️ Cash Position reports use the latest available portfolio data automatically.
                </p>
              </div>
            )}

            {reportType === 'equity_breakdown' && (
              <div className="bg-blue-50 border border-blue-200 rounded-md p-3">
                <p className="text-sm text-blue-700">
                  ℹ️ Equity Breakdown reports use the latest available portfolio data automatically. Includes sector analysis and SPY benchmark comparison.
                </p>
              </div>
            )}

            <div>
              <Label className="block text-xs font-medium text-gray-700 mb-1">
                {isLatestOnlyReport(reportType) ? 'Client:' : 'Client:'}
              </Label>
              <Select value={selectedClient} onValueChange={setSelectedClient}>
                <SelectTrigger>
                  <SelectValue placeholder="Choose client" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ALL">
                    {isLatestOnlyReport(reportType) ? 'All Clients (Bulk Generation)' : 'All Clients (Individual Reports)'}
                  </SelectItem>
                  {loadingData ? (
                    <SelectItem value="loading" disabled>Loading clients...</SelectItem>
                  ) : clients.length > 0 ? (
                    clients
                      .filter(client => ['cash_position', 'monthly_returns_custody'].includes(reportType) || client.client_code !== 'ALL')
                      .map(client => (
                        <SelectItem key={client.client_code} value={client.client_code}>
                          {client.client_code}
                        </SelectItem>
                      ))
                  ) : (
                    <SelectItem value="no-clients" disabled>No clients available</SelectItem>
                  )}
                </SelectContent>
              </Select>
            </div>
            
            <Button 
              onClick={handleGenerateReport}
              disabled={
                isGenerating || 
                !selectedClient || 
                (reportType === 'monthly_returns_custody' && (!selectedYear || !selectedMonth)) ||
                (reportType === 'weekly_investment' && !selectedDate)
              }
              className="w-full px-3 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 text-sm"
            >
              {isGenerating ? 'Generating...' : 'Generate Report'}
            </Button>
          </div>
        )}

        {/* Open Section */}
        <Button
          onClick={toggleOpen}
          className={`w-full flex items-center justify-start h-12 px-4 border border-gray-300 rounded-md hover:bg-gray-50 transition-colors ${
            openExpanded ? 'bg-blue-50 border-blue-300' : ''
          }`}
          variant="outline"
        >
          <FolderOpen className="mr-2 h-4 w-4" />
          {openLabel}
        </Button>

        {/* Expandable Open Form */}
        {openExpanded && (
          <div className="bg-gray-50 border border-gray-200 rounded-md p-4 space-y-3">
            {isLatestOnlyReport(reportType) ? (
              // Simplified UI for latest-only reports (bond issuer weight, etc.)
              <>
                <div>
                  <Label className="block text-xs font-medium text-gray-700 mb-1">Select Client:</Label>
                  <Select value={selectedClientForBrowse} onValueChange={setSelectedClientForBrowse}>
                    <SelectTrigger>
                      <SelectValue placeholder="Choose client" />
                    </SelectTrigger>
                    <SelectContent>
                      {loadingData ? (
                        <SelectItem value="loading" disabled>Loading clients...</SelectItem>
                      ) : uniqueClients.length > 0 ? (
                        uniqueClients.map((c) => (
                          <SelectItem key={c.code} value={c.code}>
                            {c.code === 'ALL' ? 'All Clients' : c.code}
                          </SelectItem>
                        ))
                      ) : (
                        <SelectItem value="no-reports" disabled>No reports available</SelectItem>
                      )}
                    </SelectContent>
                  </Select>
                </div>
                
                <Button 
                  onClick={handleOpenLatestReport}
                  disabled={!selectedClientForBrowse || loadingData}
                  className="w-full px-3 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 text-sm"
                >
                  Open Report
                </Button>
              </>
            ) : (
              // Original UI for date-based reports (weekly investment, etc.)
              <>
                <div>
                  <Label className="block text-xs font-medium text-gray-700 mb-1">Select Client:</Label>
                  <Select value={selectedClientForBrowse} onValueChange={setSelectedClientForBrowse}>
                    <SelectTrigger>
                      <SelectValue placeholder="Choose client" />
                    </SelectTrigger>
                    <SelectContent>
                      {loadingData ? (
                        <SelectItem value="loading" disabled>Loading clients...</SelectItem>
                      ) : clients.length > 0 ? (
                        clients
                          .filter(client => ['cash_position', 'monthly_returns_custody'].includes(reportType) || client.client_code !== 'ALL')
                          .sort((a, b) => {
                            if (a.client_code === 'ALL') return -1;
                            if (b.client_code === 'ALL') return 1;
                            return a.client_code.localeCompare(b.client_code);
                          })
                          .map(client => (
                            <SelectItem key={client.client_code} value={client.client_code}>
                              {client.client_code === 'ALL' ? 'All Clients' : client.client_code}
                            </SelectItem>
                          ))
                      ) : (
                        <SelectItem value="no-clients" disabled>No clients available</SelectItem>
                      )}
                    </SelectContent>
                  </Select>
                </div>
                
                <div>
                  <Label className="block text-xs font-medium text-gray-700 mb-1">Select Report:</Label>
                  <Select value={selectedReportId} onValueChange={setSelectedReportId}>
                    <SelectTrigger>
                      <SelectValue placeholder="Choose report" />
                    </SelectTrigger>
                    <SelectContent>
                      {loadingData ? (
                        <SelectItem value="loading" disabled>Loading reports...</SelectItem>
                      ) : filteredReports.length > 0 ? (
                        filteredReports.map(report => (
                          <SelectItem key={report.id} value={report.id.toString()}>
                            {selectedClientForBrowse && selectedClientForBrowse !== '' ? report.report_date : `${report.report_date} - ${report.client_name}`}
                          </SelectItem>
                        ))
                      ) : (
                        <SelectItem value="no-reports" disabled>
                          {!selectedClientForBrowse ? 'Select a client first' : 
                           filteredReports.length === 0 ? 'No reports for this client' : 'No reports generated'}
                        </SelectItem>
                      )}
                    </SelectContent>
                  </Select>
                </div>
                
                <Button 
                  onClick={handleOpenReport}
                  disabled={!selectedReportId}
                  className="w-full px-3 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 text-sm"
                >
                  Open Report
                </Button>
              </>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}