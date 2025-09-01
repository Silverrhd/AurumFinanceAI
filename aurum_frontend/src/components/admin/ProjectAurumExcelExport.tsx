'use client';

import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Label } from '@/components/ui/label';
import { portfolioAPI } from '@/lib/api/portfolio';
import { Client } from '@/types/portfolio';
import { 
  FileDown, 
  ChevronDown,
  Download,
  Calendar,
  Users
} from 'lucide-react';
import toast from 'react-hot-toast';

interface ExportCardProps {
  title: string;
  type: 'positions' | 'transactions';
  description: string;
}

function ExportCard({ title, type, description }: ExportCardProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [isExporting, setIsExporting] = useState(false);
  
  // Form states
  const [snapshotDate, setSnapshotDate] = useState('');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [clientCode, setClientCode] = useState('ALL');
  
  // Data states
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [clients, setClients] = useState<Client[]>([]);
  const [loading, setLoading] = useState(false);

  const loadData = async () => {
    setLoading(true);
    try {
      const [datesResponse, clientsResponse] = await Promise.all([
        portfolioAPI.getExportAvailableDates(),
        portfolioAPI.getClients()
      ]);

      if (datesResponse.status === 'success' && datesResponse.data) {
        console.log('Export dates API response:', datesResponse.data);
        setAvailableDates(datesResponse.data.snapshot_dates || []);
      } else {
        console.error('Export dates API failed:', datesResponse);
      }

      if (clientsResponse.status === 'success' && clientsResponse.data) {
        setClients(clientsResponse.data);
      }
    } catch (error) {
      console.error('Failed to load export data:', error);
      toast.error('Failed to load export data');
    } finally {
      setLoading(false);
    }
  };

  const handleToggleExpand = () => {
    setIsExpanded(!isExpanded);
    if (!isExpanded && availableDates.length === 0) {
      loadData();
    }
  };

  const handleExport = async () => {
    setIsExporting(true);
    
    try {
      let response: Response;
      let filename: string;
      
      if (type === 'positions') {
        if (!snapshotDate) {
          toast.error('Please select a snapshot date');
          return;
        }
        
        response = await portfolioAPI.exportPositionsExcel({
          client_code: clientCode,
          snapshot_date: snapshotDate
        });
        
        filename = `positions_${snapshotDate}_${clientCode}.xlsx`;
      } else {
        if (!startDate || !endDate) {
          toast.error('Please select both start and end dates');
          return;
        }
        
        response = await portfolioAPI.exportTransactionsExcel({
          client_code: clientCode,
          start_date: startDate,
          end_date: endDate
        });
        
        filename = `transactions_${startDate}-${endDate}_${clientCode}.xlsx`;
      }
      
      if (response.ok) {
        const blob = await response.blob();
        downloadFile(blob, filename);
        toast.success('Excel file downloaded successfully!');
        
        // Reset form
        if (type === 'positions') {
          setSnapshotDate('');
        } else {
          setStartDate('');
          setEndDate('');
        }
        setClientCode('ALL');
        setIsExpanded(false);
      } else {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || 'Export failed');
      }
    } catch (error) {
      console.error('Export error:', error);
      toast.error(error instanceof Error ? error.message : 'Failed to export data');
    } finally {
      setIsExporting(false);
    }
  };

  const downloadFile = (blob: Blob, filename: string) => {
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
  };

  const formatDateDisplay = (date: string) => {
    if (!date || typeof date !== 'string') return date;
    const [day, month, year] = date.split('_');
    if (!day || !month || !year) return date;
    return new Date(parseInt(year), parseInt(month) - 1, parseInt(day)).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });
  };

  const isFormValid = () => {
    if (type === 'positions') {
      return !!snapshotDate;
    } else {
      return !!startDate && !!endDate;
    }
  };

  return (
    <Card className="bg-white shadow-sm border rounded-lg">
      <CardContent className="p-4">
        {/* Clickable Header */}
        <Button
          onClick={handleToggleExpand}
          className="w-full flex items-center justify-between h-auto p-4 border border-gray-300 rounded-md hover:bg-gray-50 transition-colors"
          variant="outline"
        >
          <div className="flex items-center gap-3">
            <FileDown className="h-4 w-4 text-gray-600" />
            <div className="text-left">
              <div className="font-medium text-sm">{title}</div>
              <div className="text-xs text-gray-500">{description}</div>
            </div>
          </div>
          <ChevronDown className={`h-4 w-4 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
        </Button>

        {/* Expandable Form */}
        {isExpanded && (
          <div className="mt-4 p-4 bg-gray-50 border border-gray-200 rounded-md space-y-3">
            {loading ? (
              <div className="text-center py-4">
                <div className="loading-spinner mx-auto mb-2"></div>
                <p className="text-sm text-gray-500">Loading export data...</p>
              </div>
            ) : (
              <>
                {type === 'positions' ? (
                  // Positions Export Form
                  <>
                    <div>
                      <Label className="block text-xs font-medium text-gray-700 mb-1">
                        <Calendar className="inline h-3 w-3 mr-1" />
                        Snapshot Date:
                      </Label>
                      <Select value={snapshotDate} onValueChange={setSnapshotDate}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select snapshot date" />
                        </SelectTrigger>
                        <SelectContent>
                          {availableDates.length > 0 ? (
                            availableDates.map(date => (
                              <SelectItem key={date} value={date}>
                                {formatDateDisplay(date)}
                              </SelectItem>
                            ))
                          ) : (
                            <SelectItem value="no-dates" disabled>
                              No dates available
                            </SelectItem>
                          )}
                        </SelectContent>
                      </Select>
                    </div>
                  </>
                ) : (
                  // Transactions Export Form
                  <>
                    <div className="grid grid-cols-2 gap-2">
                      <div>
                        <Label className="block text-xs font-medium text-gray-700 mb-1">
                          From Date:
                        </Label>
                        <input
                          type="date"
                          value={startDate}
                          onChange={(e) => setStartDate(e.target.value)}
                          className="w-full p-2 border border-gray-300 rounded text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                        />
                      </div>
                      <div>
                        <Label className="block text-xs font-medium text-gray-700 mb-1">
                          To Date:
                        </Label>
                        <input
                          type="date"
                          value={endDate}
                          onChange={(e) => setEndDate(e.target.value)}
                          className="w-full p-2 border border-gray-300 rounded text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                        />
                      </div>
                    </div>
                  </>
                )}
                
                {/* Client Selection */}
                <div>
                  <Label className="block text-xs font-medium text-gray-700 mb-1">
                    <Users className="inline h-3 w-3 mr-1" />
                    Client:
                  </Label>
                  <Select value={clientCode} onValueChange={setClientCode}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="ALL">All Clients</SelectItem>
                      {clients.filter(client => client.client_code !== 'ALL').map(client => (
                        <SelectItem key={client.client_code} value={client.client_code}>
                          {client.client_code}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                
                {/* Export Button */}
                <Button 
                  onClick={handleExport}
                  disabled={!isFormValid() || isExporting}
                  className={`w-full text-white hover:opacity-90 ${
                    type === 'positions' 
                      ? 'bg-blue-600 hover:bg-blue-700' 
                      : 'bg-green-600 hover:bg-green-700'
                  }`}
                >
                  {isExporting ? (
                    <>
                      <div className="loading-spinner mr-2"></div>
                      Exporting...
                    </>
                  ) : (
                    <>
                      <Download className="h-4 w-4 mr-2" />
                      Download Excel ⬇️
                    </>
                  )}
                </Button>
                
                {/* Info Text */}
                <p className="text-xs text-gray-500 text-center">
                  {type === 'positions' 
                    ? 'Export includes unrealized gains calculations'
                    : 'Export includes all transactions in the selected date range'
                  }
                </p>
              </>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

export function ProjectAurumExcelExport() {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-lg font-semibold aurum-text-dark">
          <FileDown className="h-5 w-5" />
          Excel Data Export
        </CardTitle>
        <p className="text-sm text-gray-600">
          Export positions and transactions data to Excel files
        </p>
      </CardHeader>
      <CardContent>
        {/* 2-card grid matching ProcessingControls pattern */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <ExportCard
            title="📊 Positions Export"
            type="positions"
            description="Export portfolio positions with unrealized gains"
          />
          <ExportCard
            title="💼 Transactions Export"
            type="transactions"
            description="Export transaction history for date ranges"
          />
        </div>
      </CardContent>
    </Card>
  );
}