'use client'

import React, { useState, useEffect } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Progress } from '@/components/ui/progress'
import { cn } from '@/lib/utils'
import { 
  Building2, 
  CheckCircle, 
  AlertCircle, 
  Clock,
  FileText,
  RefreshCw,
  XCircle,
  TrendingUp
} from 'lucide-react'

// Bank configuration matching ProjectAurum's supported banks
const SUPPORTED_BANKS = [
  { code: 'JPM', name: 'J.P. Morgan', color: 'bg-blue-500' },
  { code: 'MS', name: 'Morgan Stanley', color: 'bg-green-500' },
  { code: 'CS', name: 'Credit Suisse', color: 'bg-purple-500' },
  { code: 'UBS', name: 'UBS', color: 'bg-red-500' },
  { code: 'GS', name: 'Goldman Sachs', color: 'bg-yellow-500' },
  { code: 'BAML', name: 'Bank of America ML', color: 'bg-blue-600' },
  { code: 'Citi', name: 'Citigroup', color: 'bg-orange-500' },
  { code: 'Pershing', name: 'Pershing LLC', color: 'bg-indigo-500' },
  { code: 'SSGA', name: 'State Street', color: 'bg-teal-500' },
  { code: 'Fidelity', name: 'Fidelity', color: 'bg-pink-500' },
  { code: 'Vanguard', name: 'Vanguard', color: 'bg-cyan-500' },
  { code: 'Schwab', name: 'Charles Schwab', color: 'bg-emerald-500' },
  { code: 'ALT', name: 'Alternative Assets', color: 'bg-amber-500' }
]

export interface BankStatus {
  bank_code: string
  bank_name: string
  file_count: number
  last_upload: string | null
  processing_status: 'pending' | 'processing' | 'completed' | 'error'
  last_processed: string | null
  error_message?: string
  total_positions: number
  total_value: number
  next_action: string
}

interface BankStatusGridProps {
  onRefresh?: () => void
  onProcessBank?: (bankCode: string) => void
  className?: string
}

export function BankStatusGrid({ 
  onRefresh, 
  onProcessBank, 
  className 
}: BankStatusGridProps) {
  const [bankStatuses, setBankStatuses] = useState<BankStatus[]>([])
  const [loading, setLoading] = useState(true)
  const [lastUpdated, setLastUpdated] = useState<Date>(new Date())

  // Fetch bank status data from Django API
  const fetchBankStatuses = async () => {
    try {
      setLoading(true)
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/api/portfolio/bank-status/`,
        {
          headers: {
            'Authorization': `Bearer ${localStorage.getItem('accessToken')}`,
            'Content-Type': 'application/json',
          },
        }
      )

      if (!response.ok) {
        throw new Error(`API error: ${response.status}`)
      }

      const data = await response.json()
      setBankStatuses(data)
      setLastUpdated(new Date())
    } catch (error) {
      console.error('Failed to fetch bank statuses:', error)
      // Initialize with default empty statuses for all supported banks
      const defaultStatuses = SUPPORTED_BANKS.map(bank => ({
        bank_code: bank.code,
        bank_name: bank.name,
        file_count: 0,
        last_upload: null,
        processing_status: 'pending' as const,
        last_processed: null,
        total_positions: 0,
        total_value: 0,
        next_action: 'Upload files to begin processing'
      }))
      setBankStatuses(defaultStatuses)
    } finally {
      setLoading(false)
    }
  }

  // Refresh data every 30 seconds
  useEffect(() => {
    fetchBankStatuses()
    const interval = setInterval(fetchBankStatuses, 30000)
    return () => clearInterval(interval)
  }, [])

  const handleRefresh = () => {
    fetchBankStatuses()
    onRefresh?.()
  }

  const handleProcessBank = (bankCode: string) => {
    onProcessBank?.(bankCode)
    // Refresh status after processing
    setTimeout(fetchBankStatuses, 1000)
  }

  const getStatusIcon = (status: BankStatus['processing_status']) => {
    switch (status) {
      case 'completed':
        return <CheckCircle className="h-5 w-5 text-green-500" />
      case 'processing':
        return <RefreshCw className="h-5 w-5 text-blue-500 animate-spin" />
      case 'error':
        return <XCircle className="h-5 w-5 text-red-500" />
      default:
        return <Clock className="h-5 w-5 text-gray-400" />
    }
  }

  const getStatusBadge = (status: BankStatus['processing_status']) => {
    const variants = {
      completed: 'bg-green-100 text-green-800 hover:bg-green-200',
      processing: 'bg-blue-100 text-blue-800 hover:bg-blue-200',
      error: 'bg-red-100 text-red-800 hover:bg-red-200',
      pending: 'bg-gray-100 text-gray-600 hover:bg-gray-200'
    }

    return (
      <Badge variant="secondary" className={cn(variants[status])}>
        {status.charAt(0).toUpperCase() + status.slice(1)}
      </Badge>
    )
  }

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value)
  }

  const formatDate = (dateString: string | null) => {
    if (!dateString) return 'Never'
    return new Date(dateString).toLocaleString()
  }

  const getTotalStats = () => {
    return bankStatuses.reduce(
      (acc, bank) => ({
        totalFiles: acc.totalFiles + bank.file_count,
        totalPositions: acc.totalPositions + bank.total_positions,
        totalValue: acc.totalValue + bank.total_value,
        completedBanks: acc.completedBanks + (bank.processing_status === 'completed' ? 1 : 0),
        errorBanks: acc.errorBanks + (bank.processing_status === 'error' ? 1 : 0)
      }),
      { totalFiles: 0, totalPositions: 0, totalValue: 0, completedBanks: 0, errorBanks: 0 }
    )
  }

  const stats = getTotalStats()
  const completionPercentage = (stats.completedBanks / SUPPORTED_BANKS.length) * 100

  if (loading) {
    return (
      <div className={cn("space-y-6", className)}>
        <div className="flex items-center justify-between">
          <h2 className="text-2xl font-bold text-aurum-primary">Bank Processing Status</h2>
          <div className="animate-pulse bg-gray-200 h-10 w-24 rounded"></div>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          {Array.from({ length: 12 }).map((_, i) => (
            <Card key={i} className="animate-pulse">
              <CardHeader>
                <div className="h-4 bg-gray-200 rounded w-3/4"></div>
                <div className="h-3 bg-gray-200 rounded w-1/2"></div>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  <div className="h-3 bg-gray-200 rounded"></div>
                  <div className="h-3 bg-gray-200 rounded w-2/3"></div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className={cn("space-y-6", className)}>
      {/* Header with Summary Stats */}
      <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-2xl font-bold text-aurum-primary">Bank Processing Status</h2>
          <p className="text-sm text-gray-600">
            Last updated: {lastUpdated.toLocaleTimeString()}
          </p>
        </div>
        <Button 
          onClick={handleRefresh}
          variant="outline"
          className="flex items-center gap-2"
        >
          <RefreshCw className="h-4 w-4" />
          Refresh
        </Button>
      </div>

      {/* Overall Progress Summary */}
      <Card className="bg-gradient-to-r from-aurum-primary/5 to-aurum-dark-blue/5">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5 text-aurum-primary" />
            Overall Processing Status
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-4">
            <div className="text-center">
              <div className="text-2xl font-bold text-aurum-primary">{stats.completedBanks}</div>
              <div className="text-sm text-gray-600">Completed</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold">{stats.totalFiles}</div>
              <div className="text-sm text-gray-600">Total Files</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold">{stats.totalPositions.toLocaleString()}</div>
              <div className="text-sm text-gray-600">Positions</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold">{formatCurrency(stats.totalValue)}</div>
              <div className="text-sm text-gray-600">Total Value</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-red-500">{stats.errorBanks}</div>
              <div className="text-sm text-gray-600">Errors</div>
            </div>
          </div>
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span>Processing Completion</span>
              <span>{Math.round(completionPercentage)}%</span>
            </div>
            <Progress value={completionPercentage} className="h-2" />
          </div>
        </CardContent>
      </Card>

      {/* Bank Status Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
        {bankStatuses.map((bank) => {
          const bankConfig = SUPPORTED_BANKS.find(b => b.code === bank.bank_code)
          
          return (
            <Card 
              key={bank.bank_code} 
              className={cn(
                "transition-all duration-200 hover:shadow-lg",
                bank.processing_status === 'error' && "border-red-200 bg-red-50/50",
                bank.processing_status === 'completed' && "border-green-200 bg-green-50/50"
              )}
            >
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div 
                      className={cn(
                        "w-3 h-3 rounded-full",
                        bankConfig?.color || "bg-gray-400"
                      )}
                    />
                    <CardTitle className="text-sm font-medium truncate">
                      {bank.bank_name}
                    </CardTitle>
                  </div>
                  {getStatusIcon(bank.processing_status)}
                </div>
                <div className="flex justify-between items-center">
                  <CardDescription className="text-xs">
                    {bank.bank_code}
                  </CardDescription>
                  {getStatusBadge(bank.processing_status)}
                </div>
              </CardHeader>
              
              <CardContent className="space-y-3">
                {/* File and Processing Info */}
                <div className="flex items-center justify-between text-sm">
                  <span className="flex items-center gap-1">
                    <FileText className="h-3 w-3" />
                    Files:
                  </span>
                  <span className="font-medium">{bank.file_count}</span>
                </div>

                {bank.total_positions > 0 && (
                  <>
                    <div className="flex items-center justify-between text-sm">
                      <span>Positions:</span>
                      <span className="font-medium">{bank.total_positions.toLocaleString()}</span>
                    </div>
                    <div className="flex items-center justify-between text-sm">
                      <span>Value:</span>
                      <span className="font-medium">{formatCurrency(bank.total_value)}</span>
                    </div>
                  </>
                )}

                {/* Last Upload/Processed */}
                <div className="text-xs text-gray-500 space-y-1">
                  <div>Last upload: {formatDate(bank.last_upload)}</div>
                  <div>Last processed: {formatDate(bank.last_processed)}</div>
                </div>

                {/* Error Message */}
                {bank.processing_status === 'error' && bank.error_message && (
                  <div className="text-xs text-red-600 bg-red-50 p-2 rounded border border-red-200">
                    {bank.error_message}
                  </div>
                )}

                {/* Next Action */}
                <div className="text-xs text-blue-600 bg-blue-50 p-2 rounded border border-blue-200">
                  <strong>Next:</strong> {bank.next_action}
                </div>

                {/* Process Button */}
                {bank.file_count > 0 && bank.processing_status !== 'processing' && (
                  <Button
                    size="sm"
                    onClick={() => handleProcessBank(bank.bank_code)}
                    className="w-full text-xs"
                    variant={bank.processing_status === 'error' ? 'destructive' : 'default'}
                  >
                    {bank.processing_status === 'error' ? 'Retry Processing' : 'Process Files'}
                  </Button>
                )}
              </CardContent>
            </Card>
          )
        })}
      </div>
    </div>
  )
}