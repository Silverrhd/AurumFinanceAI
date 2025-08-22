"use client"

import { useCallback, useState } from 'react'
import { useDropzone } from 'react-dropzone'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'
import { 
  Upload, 
  CheckCircle, 
  AlertCircle, 
  Clock, 
  FileText,
  X
} from 'lucide-react'

interface UploadResult {
  filename: string
  bank: string
  status: 'success' | 'error'
  message: string
}

interface FileUploadZoneProps {
  onUploadComplete?: (results: UploadResult[]) => void
  onUploadProgress?: (progress: number) => void
}

interface UploadFile extends File {
  id: string
  progress: number
  status: 'pending' | 'uploading' | 'success' | 'error'
  bank?: string
  message?: string
}

export function FileUploadZone({ onUploadComplete, onUploadProgress }: FileUploadZoneProps) {
  const [uploadState, setUploadState] = useState<'idle' | 'uploading' | 'success' | 'error'>('idle')
  const [uploadFiles, setUploadFiles] = useState<UploadFile[]>([])
  const [overallProgress, setOverallProgress] = useState(0)
  const [detectedBanks, setDetectedBanks] = useState<string[]>([])

  const removeFile = (fileId: string) => {
    setUploadFiles(prev => prev.filter(file => file.id !== fileId))
  }

  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    // Convert files to UploadFile format
    const filesToUpload: UploadFile[] = acceptedFiles.map(file => ({
      ...file,
      id: `${file.name}-${Date.now()}-${Math.random()}`,
      progress: 0,
      status: 'pending'
    }))

    setUploadFiles(filesToUpload)
    setUploadState('uploading')
    setOverallProgress(0)

    try {
      // Prepare FormData
      const formData = new FormData()
      filesToUpload.forEach(file => {
        formData.append('files', file)
      })

      // Track upload progress for each file
      const uploadPromises = filesToUpload.map(async (file, index) => {
        // Simulate individual file progress
        setUploadFiles(prev => prev.map(f => 
          f.id === file.id ? { ...f, status: 'uploading' } : f
        ))

        // Update progress in steps
        for (let progress = 0; progress <= 100; progress += 20) {
          await new Promise(resolve => setTimeout(resolve, 100))
          setUploadFiles(prev => prev.map(f => 
            f.id === file.id ? { ...f, progress } : f
          ))
        }
      })

      // Start all file uploads
      await Promise.all(uploadPromises)

      // Make actual API call to Django backend
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/api/portfolio/files/upload/`, {
        method: 'POST',
        body: formData,
        headers: {
          // Don't set Content-Type, let browser set it for FormData
          'Authorization': `Bearer ${localStorage.getItem('auth_token') || ''}`
        }
      })

      if (!response.ok) {
        throw new Error(`Upload failed: ${response.status}`)
      }

      const result = await response.json()
      
      // Process results and update file statuses
      const banks = new Set<string>()
      
      result.results?.forEach((uploadResult: UploadResult, index: number) => {
        if (uploadResult.bank) {
          banks.add(uploadResult.bank)
        }
        
        setUploadFiles(prev => prev.map((file, fileIndex) => 
          fileIndex === index 
            ? { 
                ...file, 
                status: uploadResult.status as 'success' | 'error',
                bank: uploadResult.bank,
                message: uploadResult.message,
                progress: 100
              } 
            : file
        ))
      })

      setDetectedBanks(Array.from(banks))
      setUploadState('success')
      setOverallProgress(100)
      
      // Notify parent component
      if (onUploadComplete) {
        onUploadComplete(result.results || [])
      }

    } catch (error) {
      console.error('Upload error:', error)
      setUploadState('error')
      
      // Mark all files as failed
      setUploadFiles(prev => prev.map(file => ({
        ...file,
        status: 'error',
        message: error instanceof Error ? error.message : 'Upload failed'
      })))
    }
  }, [onUploadComplete])

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
      'application/vnd.ms-excel': ['.xls']
    },
    multiple: true,
    disabled: uploadState === 'uploading'
  })

  const resetUpload = () => {
    setUploadState('idle')
    setUploadFiles([])
    setOverallProgress(0)
    setDetectedBanks([])
  }

  return (
    <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-lg">
      <CardHeader>
        <CardTitle className="flex items-center space-x-2">
          <Upload className="w-5 h-5" />
          <span>Bank File Upload System</span>
        </CardTitle>
        <p className="text-sm text-gray-600">Upload Excel files from banks - automatic detection and routing</p>
      </CardHeader>
      <CardContent>
        {/* Main Upload Zone */}
        <div 
          {...getRootProps()} 
          className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors cursor-pointer ${
            isDragActive 
              ? 'border-blue-400 bg-blue-50' 
              : uploadState === 'uploading'
              ? 'border-gray-300 bg-gray-50 cursor-not-allowed'
              : 'border-gray-300 hover:border-blue-400'
          }`}
        >
          <input {...getInputProps()} />
          
          {uploadState === 'idle' && (
            <>
              <Upload className="w-12 h-12 text-gray-400 mx-auto mb-4" />
              <h4 className="text-lg font-medium text-gray-700 mb-2">
                {isDragActive ? 'Drop files here' : 'Drop Excel files here or click to browse'}
              </h4>
              <p className="text-sm text-gray-500 mb-4">
                Supports .xlsx and .xls files from all supported banks
              </p>
              <Button className="bg-gradient-to-r from-blue-600 to-indigo-600">
                <FileText className="w-4 h-4 mr-2" />
                Browse Files
              </Button>
            </>
          )}
          
          {uploadState === 'uploading' && (
            <>
              <Clock className="w-12 h-12 text-blue-500 mx-auto mb-4 animate-spin" />
              <h4 className="text-lg font-medium text-gray-700 mb-2">Processing uploads...</h4>
              <Progress value={overallProgress} className="w-full mb-4" />
              <p className="text-sm text-gray-500">{overallProgress}% complete</p>
            </>
          )}
          
          {uploadState === 'success' && (
            <>
              <CheckCircle className="w-12 h-12 text-green-500 mx-auto mb-4" />
              <h4 className="text-lg font-medium text-green-700 mb-2">Upload successful!</h4>
              {detectedBanks.length > 0 && (
                <div className="mb-4">
                  <p className="text-sm text-gray-600 mb-2">Detected banks:</p>
                  <div className="flex flex-wrap gap-2 justify-center">
                    {detectedBanks.map(bank => (
                      <Badge key={bank} variant="secondary" className="bg-blue-100 text-blue-800">
                        {bank}
                      </Badge>
                    ))}
                  </div>
                </div>
              )}
              <Button onClick={resetUpload} variant="outline">
                <Upload className="w-4 h-4 mr-2" />
                Upload More Files
              </Button>
            </>
          )}
          
          {uploadState === 'error' && (
            <>
              <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
              <h4 className="text-lg font-medium text-red-700 mb-2">Upload failed</h4>
              <p className="text-sm text-red-600 mb-4">Please check your files and try again</p>
              <Button onClick={resetUpload} variant="outline">
                <Upload className="w-4 h-4 mr-2" />
                Try Again
              </Button>
            </>
          )}
        </div>

        {/* File List */}
        {uploadFiles.length > 0 && (
          <div className="mt-6">
            <h5 className="text-sm font-medium text-gray-700 mb-3">Upload Progress</h5>
            <div className="space-y-2">
              {uploadFiles.map((file) => (
                <div key={file.id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center space-x-3">
                    <FileText className="w-4 h-4 text-gray-500" />
                    <div className="flex-1">
                      <p className="text-sm font-medium text-gray-900">{file.name}</p>
                      {file.bank && (
                        <p className="text-xs text-gray-500">Bank: {file.bank}</p>
                      )}
                      {file.message && (
                        <p className={`text-xs ${file.status === 'error' ? 'text-red-600' : 'text-green-600'}`}>
                          {file.message}
                        </p>
                      )}
                    </div>
                  </div>
                  
                  <div className="flex items-center space-x-3">
                    {file.status === 'uploading' && (
                      <div className="flex items-center space-x-2">
                        <Progress value={file.progress} className="w-16" />
                        <span className="text-xs text-gray-500">{file.progress}%</span>
                      </div>
                    )}
                    
                    {file.status === 'success' && (
                      <CheckCircle className="w-4 h-4 text-green-500" />
                    )}
                    
                    {file.status === 'error' && (
                      <AlertCircle className="w-4 h-4 text-red-500" />
                    )}
                    
                    {file.status === 'pending' && uploadState !== 'uploading' && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => removeFile(file.id)}
                      >
                        <X className="w-4 h-4" />
                      </Button>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}