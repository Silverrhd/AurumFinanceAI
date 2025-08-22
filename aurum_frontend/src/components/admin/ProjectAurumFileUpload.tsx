'use client';

import React, { useState, useCallback } from 'react';
import { useDropzone } from 'react-dropzone';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { portfolioAPI } from '@/lib/api/portfolio';
import { UploadResult } from '@/types/portfolio';
import { 
  Upload, 
  CloudUpload, 
  Download, 
  CheckCircle, 
  AlertTriangle, 
  FileSpreadsheet,
  X
} from 'lucide-react';

interface FileUploadState {
  status: 'idle' | 'uploading' | 'success' | 'error';
  progress: number;
  message: string;
  results: UploadResult[];
}

export function ProjectAurumFileUpload() {
  const [uploadState, setUploadState] = useState<FileUploadState>({
    status: 'idle',
    progress: 0,
    message: '',
    results: []
  });

  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    if (acceptedFiles.length === 0) return;

    setUploadState({
      status: 'uploading',
      progress: 0,
      message: 'Preparing files for upload...',
      results: []
    });

    try {
      // Simulate progress updates
      const progressInterval = setInterval(() => {
        setUploadState(prev => ({
          ...prev,
          progress: Math.min(prev.progress + 10, 90),
          message: `Uploading ${acceptedFiles.length} files...`
        }));
      }, 200);

      const response = await portfolioAPI.uploadFiles(acceptedFiles);
      
      clearInterval(progressInterval);

      if (response.status === 'success' && response.data) {
        setUploadState({
          status: 'success',
          progress: 100,
          message: `Successfully uploaded ${acceptedFiles.length} files`,
          results: response.data
        });
      } else {
        setUploadState({
          status: 'error',
          progress: 0,
          message: response.error || 'Upload failed',
          results: []
        });
      }
    } catch (error) {
      setUploadState({
        status: 'error',
        progress: 0,
        message: error instanceof Error ? error.message : 'Upload failed',
        results: []
      });
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive, isDragReject } = useDropzone({
    onDrop,
    accept: {
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
      'application/vnd.ms-excel': ['.xls']
    },
    maxFiles: 50,
    disabled: uploadState.status === 'uploading'
  });

  const resetUpload = () => {
    setUploadState({
      status: 'idle',
      progress: 0,
      message: '',
      results: []
    });
  };

  const getBankBadgeColor = (bankCode: string) => {
    const colors: Record<string, string> = {
      'JPM': 'bg-blue-100 text-blue-800',
      'MS': 'bg-green-100 text-green-800',
      'CS': 'bg-purple-100 text-purple-800',
      'UBS': 'bg-orange-100 text-orange-800',
      'HSBC': 'bg-red-100 text-red-800',
      'Pershing': 'bg-indigo-100 text-indigo-800',
      'Lombard': 'bg-pink-100 text-pink-800',
      'Valley': 'bg-yellow-100 text-yellow-800',
      'Banchile': 'bg-cyan-100 text-cyan-800',
      'JB': 'bg-teal-100 text-teal-800',
      'CSC': 'bg-lime-100 text-lime-800',
      'IDB': 'bg-emerald-100 text-emerald-800',
      'Safra': 'bg-violet-100 text-violet-800'
    };
    return colors[bankCode] || 'bg-gray-100 text-gray-800';
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-lg font-semibold aurum-text-dark">
          <Upload className="h-5 w-5" />
          Bank File Upload System
        </CardTitle>
        <p className="text-sm text-gray-600">
          Upload Excel files from banks - automatic detection and routing
        </p>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Upload Zone */}
        <div
          {...getRootProps()}
          className={`
            border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-colors
            ${isDragActive && !isDragReject ? 'border-blue-400 bg-blue-50' : ''}
            ${isDragReject ? 'border-red-400 bg-red-50' : ''}
            ${uploadState.status === 'uploading' ? 'pointer-events-none opacity-50' : 'hover:border-blue-400'}
            ${uploadState.status === 'idle' ? 'border-gray-300' : ''}
          `}
        >
          <input {...getInputProps()} />
          
          {uploadState.status === 'idle' && (
            <div className="space-y-4">
              {isDragActive ? (
                <>
                  <Download className="h-12 w-12 text-blue-500 mx-auto" />
                  <h4 className="text-lg font-medium text-blue-700">Drop files here to upload</h4>
                </>
              ) : (
                <>
                  <CloudUpload className="h-12 w-12 text-gray-400 mx-auto" />
                  <h4 className="text-lg font-medium text-gray-700">Drop Excel files here or click to browse</h4>
                  <p className="text-sm text-gray-500">Supports .xlsx and .xls files from all supported banks</p>
                  <Button className="aurum-primary text-white">
                    <Upload className="h-4 w-4 mr-2" />
                    Browse Files
                  </Button>
                </>
              )}
            </div>
          )}

          {uploadState.status === 'uploading' && (
            <div className="space-y-4">
              <div className="loading-spinner mx-auto"></div>
              <h4 className="text-lg font-medium text-gray-700">Processing uploads...</h4>
              <div className="max-w-md mx-auto">
                <Progress value={uploadState.progress} className="w-full" />
              </div>
              <p className="text-sm text-gray-600">{uploadState.message}</p>
            </div>
          )}

          {uploadState.status === 'success' && (
            <div className="space-y-4">
              <CheckCircle className="h-12 w-12 text-green-500 mx-auto" />
              <h4 className="text-lg font-medium text-green-700">Upload Complete!</h4>
              <p className="text-sm text-gray-600">{uploadState.message}</p>
              <Button onClick={resetUpload} variant="outline">
                Upload More Files
              </Button>
            </div>
          )}

          {uploadState.status === 'error' && (
            <div className="space-y-4">
              <AlertTriangle className="h-12 w-12 text-red-500 mx-auto" />
              <h4 className="text-lg font-medium text-red-700">Upload Failed</h4>
              <p className="text-sm text-red-600">{uploadState.message}</p>
              <Button onClick={resetUpload} variant="outline">
                Try Again
              </Button>
            </div>
          )}
        </div>

        {/* Upload Results */}
        {uploadState.results.length > 0 && (
          <div className="space-y-4">
            <h4 className="font-medium text-gray-700">Upload Results</h4>
            <div className="space-y-2 max-h-64 overflow-y-auto">
              {uploadState.results.map((result, index) => (
                <div
                  key={index}
                  className={`
                    flex items-center justify-between p-3 rounded-lg border
                    ${result.status === 'success' ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-200'}
                  `}
                >
                  <div className="flex items-center gap-3">
                    <FileSpreadsheet className="h-5 w-5 text-gray-600" />
                    <div>
                      <div className="font-medium text-sm">{result.filename}</div>
                      <div className="text-xs text-gray-500">
                        {(result.file_size / 1024).toFixed(1)} KB
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    {result.bank_detected && (
                      <Badge className={getBankBadgeColor(result.bank_detected)}>
                        {result.bank_detected}
                      </Badge>
                    )}
                    {result.status === 'success' ? (
                      <CheckCircle className="h-5 w-5 text-green-500" />
                    ) : (
                      <X className="h-5 w-5 text-red-500" />
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Supported Banks Info */}
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
          <h5 className="font-medium text-sm text-blue-900 mb-2">Supported Banks</h5>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2 text-xs">
            {[
              'JPMorgan (JPM)', 'Morgan Stanley (MS)', 'Credit Suisse (CS)', 'HSBC',
              'Pershing', 'Lombard (LO)', 'Valley Bank', 'Banchile',
              'JB Private Bank', 'Charles Schwab (CSC)', 'IDB', 'Safra'
            ].map((bank, index) => (
              <div key={index} className="text-blue-700">• {bank}</div>
            ))}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}