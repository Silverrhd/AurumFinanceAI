'use client';

import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { portfolioAPI } from '@/lib/api/portfolio';
import { BankStatus } from '@/types/portfolio';
import { 
  Building, 
  RefreshCw, 
  CheckCircle, 
  AlertTriangle, 
  Clock,
  FileText,
  Settings
} from 'lucide-react';

// The 13 ProjectAurum Banks with their processing types
const PROJECTAURUM_BANKS = [
  { code: 'JPM', name: 'JPMorgan', type: 'simple', color: 'bg-blue-500' },
  { code: 'MS', name: 'Morgan Stanley', type: 'simple', color: 'bg-green-500' },
  { code: 'IDB', name: 'Inter-American Development Bank', type: 'simple', color: 'bg-emerald-500' },
  { code: 'Safra', name: 'Safra', type: 'simple', color: 'bg-violet-500' },
  { code: 'HSBC', name: 'HSBC', type: 'enrichment', color: 'bg-red-500' },
  { code: 'CS', name: 'Credit Suisse', type: 'combination', color: 'bg-purple-500' },
  { code: 'Valley', name: 'Valley Bank', type: 'combination', color: 'bg-yellow-500' },
  { code: 'JB', name: 'JB Private Bank', type: 'combination', color: 'bg-teal-500' },
  { code: 'CSC', name: 'Charles Schwab', type: 'combination', color: 'bg-lime-500' },
  { code: 'Banchile', name: 'Banchile', type: 'combination', color: 'bg-cyan-500' },
  { code: 'Gonet', name: 'Gonet Bank', type: 'combination', color: 'bg-slate-600' },
  { code: 'Pershing', name: 'Pershing', type: 'enrichment_combination', color: 'bg-indigo-500' },
  { code: 'LO', name: 'Lombard', type: 'enrichment_combination', color: 'bg-pink-500' }
];

interface ProjectAurumBankStatusProps {
  onProcessBank?: (bankCode: string) => void;
}

export function ProjectAurumBankStatus({ onProcessBank }: ProjectAurumBankStatusProps) {
  const [bankStatuses, setBankStatuses] = useState<BankStatus[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date());
  const [currentDate, setCurrentDate] = useState<string | null>(null);

  useEffect(() => {
    loadBankStatus();
  }, []);

  const loadBankStatus = async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    else setLoading(true);

    try {
      console.log('Loading bank status...');
      const response = await portfolioAPI.getBankStatus();
      console.log('Bank status response:', response);
      
      if (response.status === 'success' && response.data) {
        console.log('Setting bank statuses:', response.data);
        // Handle enhanced API response format
        if (response.data.banks) {
          setBankStatuses(response.data.banks);
          setCurrentDate(response.data.current_date || null);
        } else if (Array.isArray(response.data)) {
          // Fallback for old format
          setBankStatuses(response.data);
        } else {
          setBankStatuses([]);
        }
      } else {
        console.error('Failed to load bank status from API:', response.error);
        // Show empty state instead of mock data
        setBankStatuses([]);
      }
      
      setLastRefresh(new Date());
    } catch (error) {
      console.error('Failed to load bank status:', error);
      // Show empty state instead of mock data
      setBankStatuses([]);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const getNextSteps = (processingType: string): string => {
    switch (processingType) {
      case 'simple': return 'Ready for processing';
      case 'enrichment': return 'Requires enrichment step';
      case 'combination': return 'Requires combination step';
      case 'enrichment_combination': return 'Requires enrichment + combination';
      default: return 'Unknown processing type';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'COMPLETE': return <CheckCircle className="h-4 w-4 text-green-600" />;
      case 'PARTIAL': return <AlertTriangle className="h-4 w-4 text-yellow-600" />;
      case 'EMPTY': return <Clock className="h-4 w-4 text-blue-600" />;
      default: return <AlertTriangle className="h-4 w-4 text-gray-600" />;
    }
  };

  const getStatusBadgeClass = (status: string) => {
    switch (status) {
      case 'COMPLETE': return 'bg-green-100 text-green-800';
      case 'PARTIAL': return 'bg-yellow-100 text-yellow-800';
      case 'EMPTY': return 'bg-blue-100 text-blue-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getProcessingTypeBadge = (type: string) => {
    const badges = {
      'simple': 'bg-gray-100 text-gray-700',
      'enrichment': 'bg-orange-100 text-orange-700',
      'combination': 'bg-purple-100 text-purple-700',
      'enrichment_combination': 'bg-red-100 text-red-700'
    };
    return badges[type as keyof typeof badges] || badges.simple;
  };

  const handleProcessBank = (bankCode: string) => {
    if (onProcessBank) {
      onProcessBank(bankCode);
    } else {
      console.log('Processing bank:', bankCode);
      // Default implementation - could show a toast or modal
    }
  };

  const getBankColor = (bankCode: string) => {
    const bank = PROJECTAURUM_BANKS.find(b => b.code === bankCode);
    return bank?.color || 'bg-gray-500';
  };

  const formatDateDisplay = (date: string) => {
    if (!date || typeof date !== 'string') {
      return 'Latest Date';
    }
    const [day, month, year] = date.split('_');
    if (!day || !month || !year) {
      return date; // Return original if format is unexpected
    }
    return new Date(parseInt(year), parseInt(month) - 1, parseInt(day)).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });
  };

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2 text-lg font-semibold aurum-text-dark">
            <Building className="h-5 w-5" />
            Bank Status Monitor
          </CardTitle>
          <Button 
            variant="outline" 
            size="sm"
            onClick={() => loadBankStatus(true)}
            disabled={refreshing}
            className="aurum-text-primary hover:aurum-text-primary"
          >
            <RefreshCw className={`h-4 w-4 mr-1 ${refreshing ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
        </div>
        <p className="text-sm text-gray-600">
          Monitor file status and processing requirements for all 13 ProjectAurum banks
          {currentDate && (
            <span className="font-medium text-blue-600"> - Status for {formatDateDisplay(currentDate)}</span>
          )}
        </p>
        <div className="text-xs text-gray-500">
          Last updated: {lastRefresh.toLocaleTimeString()}
        </div>
      </CardHeader>
      <CardContent>
        {loading ? (
          <div className="text-center py-8">
            <div className="loading-spinner mx-auto mb-2"></div>
            <p className="text-sm text-gray-500">Loading bank status...</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {bankStatuses.map((bank) => (
              <Card key={bank.bank_code} className="relative overflow-hidden">
                {/* Bank Color Strip */}
                <div className={`absolute top-0 left-0 w-1 h-full ${getBankColor(bank.bank_code)}`}></div>
                
                <CardContent className="p-4 pl-6">
                  <div className="space-y-3">
                    {/* Bank Header */}
                    <div className="flex items-start justify-between">
                      <div>
                        <h4 className="font-medium text-sm">{bank.bank_name}</h4>
                        <p className="text-xs text-gray-500">({bank.bank_code})</p>
                      </div>
                      {getStatusIcon(bank.status)}
                    </div>

                    {/* Status and Percentage */}
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <Badge className={getStatusBadgeClass(bank.status)}>
                          {bank.status} ({bank.percentage}%)
                        </Badge>
                        <span className="text-xs text-gray-500">
                          {bank.file_count} files
                        </span>
                      </div>
                      
                      {/* Progress Bar */}
                      <div className="w-full bg-gray-200 rounded-full h-2">
                        <div 
                          className={`h-2 rounded-full transition-all duration-300 ${
                            bank.status === 'COMPLETE' ? 'bg-green-500' :
                            bank.status === 'PARTIAL' ? 'bg-yellow-500' : 'bg-blue-500'
                          }`}
                          style={{ width: `${bank.percentage}%` }}
                        ></div>
                      </div>
                    </div>

                    {/* Processing Type */}
                    <Badge className={getProcessingTypeBadge(bank.processing_type)}>
                      {bank.processing_type.replace('_', ' + ')}
                    </Badge>

                    {/* Next Steps */}
                    <p className="text-xs text-gray-600">{bank.next_steps}</p>

                    {/* Last Upload */}
                    {bank.last_upload && (
                      <p className="text-xs text-gray-500">
                        Last upload: {new Date(bank.last_upload).toLocaleDateString()}
                      </p>
                    )}

                    {/* Process Button */}
                    <Button 
                      size="sm" 
                      variant="outline"
                      onClick={() => handleProcessBank(bank.bank_code)}
                      className="w-full text-xs"
                      disabled={bank.status === 'EMPTY'}
                    >
                      <Settings className="h-3 w-3 mr-1" />
                      {bank.status === 'EMPTY' ? 'No Files' : 'Process'}
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        )}

        {/* Bank Processing Legend */}
        <div className="mt-6 p-4 bg-gray-50 rounded-lg">
          <h5 className="text-sm font-medium text-gray-700 mb-3">Processing Types</h5>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-xs">
            <div className="flex items-center gap-2">
              <Badge className="bg-gray-100 text-gray-700">Simple</Badge>
              <span className="text-gray-600">Direct processing (JPM, MS, IDB, Safra)</span>
            </div>
            <div className="flex items-center gap-2">
              <Badge className="bg-orange-100 text-orange-700">Enrichment</Badge>
              <span className="text-gray-600">Requires enrichment (HSBC)</span>
            </div>
            <div className="flex items-center gap-2">
              <Badge className="bg-purple-100 text-purple-700">Combination</Badge>
              <span className="text-gray-600">Requires combining (CS, Valley, JB, CSC, Banchile, Gonet)</span>
            </div>
            <div className="flex items-center gap-2">
              <Badge className="bg-red-100 text-red-700">Enrichment + Combination</Badge>
              <span className="text-gray-600">Both steps (Pershing, Lombard)</span>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}