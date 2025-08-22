'use client';

import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Progress } from '@/components/ui/progress';
import { portfolioAPI } from '@/lib/api/portfolio';
import { ProcessingStatus } from '@/types/portfolio';
import { DatabaseService } from '@/lib/services/dashboard';
import { 
  Settings, 
  Play, 
  Database, 
  RefreshCw,
  CheckCircle,
  AlertTriangle,
  Clock,
  Save,
  RotateCcw
} from 'lucide-react';

export function ProjectAurumProcessingControls() {
  const [selectedDate, setSelectedDate] = useState<string | undefined>(undefined);
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [populationDate, setPopulationDate] = useState<string | undefined>(undefined);
  const [populationReadyDates, setPopulationReadyDates] = useState<string[]>([]);
  const [processingStatus, setProcessingStatus] = useState<ProcessingStatus>({ status: 'idle' });
  const [loading, setLoading] = useState(false);
  
  // Backup related state
  const [isCreatingBackup, setIsCreatingBackup] = useState(false);
  const [availableBackups, setAvailableBackups] = useState<any[]>([]);
  const [selectedBackup, setSelectedBackup] = useState('');
  const [isRestoringBackup, setIsRestoringBackup] = useState(false);
  const [showRestoreConfirmation, setShowRestoreConfirmation] = useState(false);

  useEffect(() => {
    loadAvailableDates();
    loadPopulationReadyDates();
    loadAvailableBackups();
    // Poll processing status every 5 seconds when processing
    const interval = setInterval(() => {
      if (processingStatus.status === 'preprocessing' || processingStatus.status === 'populating') {
        loadProcessingStatus();
      }
    }, 5000);

    return () => clearInterval(interval);
  }, [processingStatus.status]);

  const loadAvailableDates = async () => {
    try {
      const response = await portfolioAPI.getAvailableSnapshots();
      
      if (response.status === 'success' && response.data) {
        // Extract date strings from the response objects
        const dateStrings = response.data.map((item: any) => item.date || item);
        setAvailableDates(dateStrings);
        // Set latest date as default
        if (dateStrings.length > 0) {
          setSelectedDate(dateStrings[0]);
        } else {
          // No dates available - clear selection
          setSelectedDate(undefined);
        }
      } else {
        // No fallback - if API fails, show no dates
        setAvailableDates([]);
        setSelectedDate(undefined);
      }
    } catch (error) {
      console.error('Failed to load available dates:', error);
      // No fallback - show empty state
      setAvailableDates([]);
      setSelectedDate(undefined);
    }
  };

  const loadPopulationReadyDates = async () => {
    try {
      const response = await portfolioAPI.getPopulationReadyDates();
      
      if (response.status === 'success' && response.data) {
        setPopulationReadyDates(response.data.ready_dates);
        // Auto-select first available date
        if (response.data.ready_dates.length > 0) {
          setPopulationDate(response.data.ready_dates[0]);
        }
      } else {
        setPopulationReadyDates([]);
        setPopulationDate(undefined);
      }
    } catch (error) {
      console.error('Failed to load population ready dates:', error);
      setPopulationReadyDates([]);
      setPopulationDate(undefined);
    }
  };

  const loadProcessingStatus = async () => {
    try {
      const response = await portfolioAPI.getProcessingStatus();
      
      if (response.status === 'success' && response.data) {
        setProcessingStatus(response.data);
      }
    } catch (error) {
      console.error('Failed to load processing status:', error);
    }
  };

  const handleRunPreprocessing = async () => {
    if (!selectedDate) return;
    
    setLoading(true);
    setProcessingStatus({ status: 'preprocessing', progress: 0, message: 'Starting preprocessing pipeline...' });
    
    try {
      const response = await portfolioAPI.startPreprocessing(selectedDate);
      
      if (response.status === 'success') {
        // Start polling for status updates
        setProcessingStatus({ 
          status: 'preprocessing', 
          progress: 10, 
          message: 'Preprocessing pipeline started successfully' 
        });
      } else {
        setProcessingStatus({ 
          status: 'error', 
          error: response.error || 'Failed to start preprocessing' 
        });
      }
    } catch (error) {
      setProcessingStatus({ 
        status: 'error', 
        error: error instanceof Error ? error.message : 'Preprocessing failed' 
      });
    } finally {
      setLoading(false);
    }
  };

  const handlePopulateDatabase = async () => {
    if (!populationDate) return;
    
    setLoading(true);
    setProcessingStatus({ status: 'populating', progress: 0, message: 'Starting database population...' });
    
    try {
      const response = await portfolioAPI.updateDatabase(populationDate);
      
      if (response.status === 'success') {
        setProcessingStatus({ 
          status: 'complete', 
          progress: 100, 
          message: `Database populated successfully for ${formatDateDisplay(populationDate)}` 
        });
        // Refresh the ready dates list
        loadPopulationReadyDates();
      } else {
        setProcessingStatus({ 
          status: 'error', 
          error: response.error || 'Failed to populate database' 
        });
      }
    } catch (error) {
      setProcessingStatus({ 
        status: 'error', 
        error: error instanceof Error ? error.message : 'Database population failed' 
      });
    } finally {
      setLoading(false);
    }
  };

  const formatDateDisplay = (date: string) => {
    if (!date || typeof date !== 'string') {
      return 'Invalid Date';
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

  // Backup related functions
  const loadAvailableBackups = async () => {
    try {
      const response = await DatabaseService.listBackups();
      if (response.status === 'success') {
        setAvailableBackups(response.backups);
      }
    } catch (error) {
      console.error('Failed to load backups:', error);
    }
  };

  const handleCreateBackup = async () => {
    try {
      setIsCreatingBackup(true);
      const response = await DatabaseService.createBackup();
      
      if (response.status === 'success') {
        alert(`Backup created successfully: ${response.backup_info?.display_name}`);
        await loadAvailableBackups();
      } else {
        alert(`Backup failed: ${response.message}`);
      }
    } catch (error: any) {
      alert(`Backup error: ${error.message}`);
    } finally {
      setIsCreatingBackup(false);
    }
  };

  const handleRestoreBackup = async () => {
    if (!selectedBackup) {
      alert('Please select a backup to restore');
      return;
    }

    try {
      setIsRestoringBackup(true);
      const response = await DatabaseService.restoreBackup(selectedBackup, true);
      
      if (response.status === 'success') {
        alert(`Database restored successfully from: ${response.restore_info?.restored_from}`);
        setShowRestoreConfirmation(false);
        setSelectedBackup('');
        // Optionally refresh the page to reflect restored data
        window.location.reload();
      } else {
        alert(`Restore failed: ${response.message}`);
      }
    } catch (error: any) {
      alert(`Restore error: ${error.message}`);
    } finally {
      setIsRestoringBackup(false);
    }
  };

  const getStatusIcon = () => {
    switch (processingStatus.status) {
      case 'preprocessing':
      case 'populating':
        return <RefreshCw className="h-5 w-5 animate-spin text-blue-600" />;
      case 'complete':
        return <CheckCircle className="h-5 w-5 text-green-600" />;
      case 'error':
        return <AlertTriangle className="h-5 w-5 text-red-600" />;
      default:
        return <Clock className="h-5 w-5 text-gray-600" />;
    }
  };

  const getStatusColor = () => {
    switch (processingStatus.status) {
      case 'preprocessing':
      case 'populating':
        return 'text-blue-700 bg-blue-50 border-blue-200';
      case 'complete':
        return 'text-green-700 bg-green-50 border-green-200';
      case 'error':
        return 'text-red-700 bg-red-50 border-red-200';
      default:
        return 'text-gray-700 bg-gray-50 border-gray-200';
    }
  };

  const isProcessing = processingStatus.status === 'preprocessing' || processingStatus.status === 'populating';

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
      {/* Preprocessing Controls */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-lg font-semibold aurum-text-dark">
            <Settings className="h-5 w-5" />
            Data Processing Pipeline
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Date Selection */}
          <div className="space-y-2">
            <label htmlFor="dateSelector" className="block text-sm font-medium text-gray-700">
              Select Processing Date
            </label>
            <Select value={selectedDate} onValueChange={setSelectedDate}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder={availableDates.length === 0 ? "No dates available" : "Select a date"} />
              </SelectTrigger>
              <SelectContent>
                {availableDates.length === 0 ? (
                  <div className="p-2 text-sm text-gray-500 text-center">
                    No processing dates available
                  </div>
                ) : (
                  availableDates.map((date) => (
                    <SelectItem key={date} value={date}>
                      {formatDateDisplay(date)}
                    </SelectItem>
                  ))
                )}
              </SelectContent>
            </Select>
            <p className="text-xs text-gray-500">
              {availableDates.length === 0 
                ? "Upload files first to make dates available for processing" 
                : "Choose from available dates or use latest"
              }
            </p>
          </div>
          
          {/* Processing Button */}
          <Button 
            onClick={handleRunPreprocessing}
            disabled={!selectedDate || isProcessing || loading}
            className="w-full aurum-primary text-white"
          >
            <Play className="h-4 w-4 mr-2" />
            Run Complete Preprocessing Pipeline
          </Button>
          
          <p className="text-sm text-gray-500">
            Processes uploaded bank files through enrichment, combining, and main preprocessing to generate unified securities and transactions files.
          </p>
        </CardContent>
      </Card>

      {/* Database Management */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-lg font-semibold aurum-text-dark">
            <Database className="h-5 w-5" />
            Database Management
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Population Date Selection */}
          <div className="space-y-2">
            <label className="block text-sm font-medium text-gray-700">
              Select Date for Population
            </label>
            <Select value={populationDate} onValueChange={setPopulationDate}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder={
                  populationReadyDates.length === 0 
                    ? "No dates ready for population" 
                    : "Select a date to populate"
                } />
              </SelectTrigger>
              <SelectContent>
                {populationReadyDates.length === 0 ? (
                  <div className="p-2 text-sm text-gray-500 text-center">
                    No dates ready for population
                  </div>
                ) : (
                  populationReadyDates.map((date) => (
                    <SelectItem key={date} value={date}>
                      {formatDateDisplay(date)} - Ready for Population
                    </SelectItem>
                  ))
                )}
              </SelectContent>
            </Select>
            <p className="text-xs text-gray-500">
              {populationReadyDates.length === 0 
                ? "Process files first to make dates available for population" 
                : "Dates with processed files but no database snapshots"
              }
            </p>
          </div>

          {/* Populate Database Button */}
          <Button 
            onClick={handlePopulateDatabase}
            disabled={!populationDate || isProcessing || loading}
            className="w-full aurum-primary text-white"
          >
            <Database className="h-4 w-4 mr-2" />
            Populate Database
          </Button>
          
          <Button 
            variant="outline"
            disabled={isProcessing || loading}
            className="w-full"
          >
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh Database
          </Button>
          
          <Button 
            variant="outline"
            disabled={isProcessing || loading}
            className="w-full"
          >
            <Settings className="h-4 w-4 mr-2" />
            System Settings
          </Button>
          
          {/* Database Backup & Restore Section */}
          <div className="pt-4 border-t border-gray-200 space-y-4">
            <h4 className="text-sm font-medium text-gray-700">Database Backup & Restore</h4>
            
            {/* Create Backup Button */}
            <Button
              onClick={handleCreateBackup}
              disabled={isCreatingBackup || isProcessing}
              className="w-full bg-blue-600 hover:bg-blue-700 text-white"
            >
              <Save className="h-4 w-4 mr-2" />
              {isCreatingBackup ? 'Creating Backup...' : 'Create Database Backup'}
            </Button>
            
            {/* Restore Section */}
            <div className="space-y-2">
              <label className="block text-sm font-medium text-gray-700">
                Restore from Backup
              </label>
              
              {/* Backup Selection Dropdown */}
              <Select value={selectedBackup} onValueChange={setSelectedBackup}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder={
                    availableBackups.length === 0 
                      ? "No backups available" 
                      : "Select backup to restore"
                  } />
                </SelectTrigger>
                <SelectContent>
                  {availableBackups.length === 0 ? (
                    <div className="p-2 text-sm text-gray-500 text-center">
                      No backups found
                    </div>
                  ) : (
                    availableBackups.map((backup) => (
                      <SelectItem key={backup.filename} value={backup.filename}>
                        {backup.display_name} ({backup.size_mb} MB)
                      </SelectItem>
                    ))
                  )}
                </SelectContent>
              </Select>
              
              {/* Restore Button */}
              <Button
                onClick={() => setShowRestoreConfirmation(true)}
                disabled={!selectedBackup || isRestoringBackup || isProcessing}
                className="w-full bg-orange-600 hover:bg-orange-700 text-white"
                variant="destructive"
              >
                <RotateCcw className="h-4 w-4 mr-2" />
                {isRestoringBackup ? 'Restoring...' : 'Restore Database'}
              </Button>
            </div>
            
            <p className="text-xs text-gray-500">
              ⚠️ Backup creates instant snapshots. Restore will backup current DB before replacing.
            </p>
          </div>
          
          <p className="text-sm text-gray-500">
            Manage system-wide configurations and database operations.
          </p>
        </CardContent>
      </Card>

      {/* Processing Status - Full Width */}
      {(processingStatus.status !== 'idle' || processingStatus.message) && (
        <Card className="md:col-span-2">
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-lg font-semibold aurum-text-dark">
              {getStatusIcon()}
              Processing Status
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className={`p-4 rounded-lg border ${getStatusColor()}`}>
              <div className="flex items-center justify-between mb-2">
                <span className="font-medium">
                  {processingStatus.status === 'preprocessing' && 'Running Preprocessing Pipeline'}
                  {processingStatus.status === 'populating' && 'Populating Database'}
                  {processingStatus.status === 'complete' && 'Processing Complete'}
                  {processingStatus.status === 'error' && 'Processing Error'}
                  {processingStatus.status === 'idle' && 'Ready'}
                </span>
                {processingStatus.progress !== undefined && (
                  <span className="text-sm">
                    {processingStatus.progress}%
                  </span>
                )}
              </div>
              
              {processingStatus.progress !== undefined && (
                <Progress value={processingStatus.progress} className="mb-2" />
              )}
              
              {processingStatus.message && (
                <p className="text-sm">{processingStatus.message}</p>
              )}
              
              {processingStatus.error && (
                <p className="text-sm text-red-600">{processingStatus.error}</p>
              )}
              
              {processingStatus.stage && (
                <p className="text-xs mt-1 opacity-75">
                  Current stage: {processingStatus.stage}
                </p>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Restore Confirmation Dialog */}
      {showRestoreConfirmation && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white p-6 rounded-lg max-w-md mx-4">
            <h3 className="text-lg font-semibold mb-4">Confirm Database Restore</h3>
            <p className="text-sm text-gray-600 mb-4">
              Are you sure you want to restore the database from:
              <br />
              <strong>{availableBackups.find(b => b.filename === selectedBackup)?.display_name}</strong>
            </p>
            <p className="text-xs text-orange-600 mb-4">
              This will create a backup of the current database before restoring.
            </p>
            <div className="flex space-x-3">
              <Button
                onClick={handleRestoreBackup}
                disabled={isRestoringBackup}
                className="flex-1 bg-orange-600 hover:bg-orange-700 text-white"
              >
                {isRestoringBackup ? 'Restoring...' : 'Yes, Restore'}
              </Button>
              <Button
                onClick={() => setShowRestoreConfirmation(false)}
                disabled={isRestoringBackup}
                variant="outline"
                className="flex-1"
              >
                Cancel
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}