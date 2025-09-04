'use client';

import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Label } from '@/components/ui/label';
import { FileText, BarChart3, Calendar, Building, TrendingUp, DollarSign } from 'lucide-react';
import { portfolioAPI } from '@/lib/api/portfolio';
import { Client } from '@/types/portfolio';
import { ReportCard } from './ReportCard';

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

export function ProjectAurumReportGeneration() {
  // State management
  const [loadingStates, setLoadingStates] = useState({
    weekly: false,
    bondMaturity: false,
    bondIssuer: false,
    equity: false,
    loadingData: true
  });

  // Weekly report generation state
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [selectedDate, setSelectedDate] = useState<string>('');
  const [selectedClient, setSelectedClient] = useState<string>('');
  const [clients, setClients] = useState<Client[]>([]);

  // Weekly report browsing state
  const [existingReports, setExistingReports] = useState<WeeklyReport[]>([]);
  const [selectedClientForBrowse, setSelectedClientForBrowse] = useState<string>('');
  const [selectedReportId, setSelectedReportId] = useState<string>('');
  const [filteredReports, setFilteredReports] = useState<WeeklyReport[]>([]);

  // Load initial data - RE-ENABLED
  useEffect(() => {
    loadInitialData();
  }, []);

  // Filter reports by client selection
  useEffect(() => {
    const filtered = (selectedClientForBrowse && selectedClientForBrowse !== 'all-clients')
      ? existingReports.filter(r => r.client_code === selectedClientForBrowse)
      : existingReports;
    setFilteredReports(filtered);
    setSelectedReportId(''); // Reset selection when filtering
  }, [selectedClientForBrowse, existingReports]);

  const loadInitialData = async () => {
    try {
      // Load clients
      const clientsResponse = await portfolioAPI.getClients();
      if (clientsResponse.status === 'success' && clientsResponse.data) {
        setClients(clientsResponse.data);
      }

      // Load available dates and existing reports
      await loadWeeklyReportData();
      
    } catch (error) {
      console.error('Failed to load initial data:', error);
    } finally {
      setLoadingStates(prev => ({ ...prev, loadingData: false }));
    }
  };

  const loadWeeklyReportData = async () => {
    try {
      // Load available dates for generation
      const datesResponse = await portfolioAPI.getAvailableWeeklyReportDates();
      if (datesResponse.status === 'success' && datesResponse.data) {
        setAvailableDates(datesResponse.data.available_dates);
      }

      // Load existing reports for browsing
      const reportsResponse = await portfolioAPI.getGeneratedWeeklyReports();
      if (reportsResponse.status === 'success' && reportsResponse.data) {
        setExistingReports(reportsResponse.data.reports);
      }
    } catch (error) {
      console.error('Failed to load weekly report data:', error);
    }
  };

  const handleGenerateWeeklyReport = async () => {
    if (!selectedDate) {
      alert('Please select a date');
      return;
    }

    if (!selectedClient) {
      alert('Please select a client');
      return;
    }

    setLoadingStates(prev => ({ ...prev, weekly: true }));
    
    try {
      const response = await portfolioAPI.generateReport({
        report_type: 'weekly_investment',
        client_code: selectedClient,
        current_date: selectedDate
      });
      
      if (response.status === 'success' && response.data?.html_content) {
        // Open report in new tab
        if (/iPhone|iPad|iPod|Android/i.test(navigator.userAgent)) {
          // Mobile: Use data URL in same tab
          const dataURL = 'data:text/html;charset=utf-8,' + encodeURIComponent(response.data.html_content);
          window.location.href = dataURL;
        } else {
          // Desktop: Keep exact same blob approach (unchanged)
          const blob = new Blob([response.data.html_content], { type: 'text/html' });
          const url = URL.createObjectURL(blob);
          window.open(url, '_blank');
          URL.revokeObjectURL(url);
        }
        
        // Refresh data to update available dates and existing reports
        await loadWeeklyReportData();
        
        // Reset selections
        setSelectedDate('');
        setSelectedClient('');
        
        alert(`Report generated successfully! Saved to: ${response.data.file_path}`);
      } else {
        alert('Failed to generate report: ' + (response.error || 'Unknown error'));
      }
    } catch (error) {
      console.error('Report generation error:', error);
      alert('Failed to generate report. Please try again.');
    } finally {
      setLoadingStates(prev => ({ ...prev, weekly: false }));
    }
  };

  const handleOpenReport = async () => {
    if (!selectedReportId) {
      alert('Please select a report');
      return;
    }

    try {
      const response = await portfolioAPI.getReportFile(parseInt(selectedReportId));
      
      if (response.status === 'success' && response.data?.html_content) {
        if (/iPhone|iPad|iPod|Android/i.test(navigator.userAgent)) {
          // Mobile: Use data URL in same tab
          const dataURL = 'data:text/html;charset=utf-8,' + encodeURIComponent(response.data.html_content);
          window.location.href = dataURL;
        } else {
          // Desktop: Keep exact same blob approach (unchanged)
          const blob = new Blob([response.data.html_content], { type: 'text/html' });
          const url = URL.createObjectURL(blob);
          window.open(url, '_blank');
          URL.revokeObjectURL(url);
        }
      } else {
        alert('Failed to open report: ' + (response.error || 'Unknown error'));
      }
    } catch (error) {
      console.error('Report opening error:', error);
      alert('Failed to open report. Please try again.');
    }
  };

  const handlePlaceholderReport = (reportType: string) => {
    alert(`${reportType} generation coming soon!`);
  };

  if (loadingStates.loadingData) {
    return (
      <div className="space-y-6">
        <Card>
          <CardContent className="flex items-center justify-center py-8">
            <div className="text-center">
              <div className="loading-spinner mb-2"></div>
              <p>Loading report data...</p>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* 2x3 Grid - Added Cash Report as 5th card */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {/* Card 1: Investment Reports */}
        <ReportCard
          title="Investment Reports"
          icon={<BarChart3 />}
          reportType="weekly_investment"
          generateLabel="Generate Weekly Investment Report"
          openLabel="Open Weekly Investment Reports"
        />
        
        {/* Card 2: Bond Issuer Weight Reports */}
        <ReportCard
          title="Bond Issuer Weight Reports"
          icon={<Building />}
          reportType="bond_issuer_weight"
          generateLabel="Generate Bond Issuer Weight Report"
          openLabel="Open Bond Issuer Weight Reports"
        />
        
        {/* Card 3: Bond Maturity Reports */}
        <ReportCard
          title="Bond Maturity Reports"
          icon={<Calendar />}
          reportType="bond_maturity"
          generateLabel="Generate Bond Maturity Report"
          openLabel="Open Bond Maturity Reports"
        />
        
        {/* Card 4: Equity Breakdown Reports */}
        <ReportCard
          title="Equity Breakdown Reports"
          icon={<TrendingUp />}
          reportType="equity_breakdown"
          generateLabel="Generate Equity Breakdown Report"
          openLabel="Open Equity Breakdown Reports"
        />
        
        {/* Card 5: Cash Position Reports */}
        <ReportCard
          title="Cash Position Reports"
          icon={<DollarSign />}
          reportType="cash_position"
          generateLabel="Generate Cash Position Report"
          openLabel="Open Cash Position Reports"
        />
        
        {/* Card 6: Monthly Returns by Custody */}
        <ReportCard
          title="Monthly Returns by Custody"
          icon={<TrendingUp />}
          reportType="monthly_returns_custody"
          generateLabel="Generate Monthly Returns by Custody"
          openLabel="Open Monthly Returns Reports"
        />
      </div>
    </div>
  );
}