'use client'

import React, { useEffect, useState } from 'react'
import dynamic from 'next/dynamic'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { TrendingUp, TrendingDown, Minus } from 'lucide-react'

// Dynamically import ApexCharts to avoid SSR issues
const Chart = dynamic(() => import('react-apexcharts'), { ssr: false })

export interface PortfolioValueDataPoint {
  date: string
  value: number
  change: number
  change_percent: number
}

interface PortfolioValueChartProps {
  className?: string
  height?: number
  portfolioId?: string
  dateRange?: string
  onDateRangeChange?: (range: string) => void
}

const DATE_RANGES = [
  { label: '7D', value: '7D' },
  { label: '1M', value: '1M' },
  { label: '3M', value: '3M' },
  { label: '6M', value: '6M' },
  { label: '1Y', value: '1Y' },
  { label: 'ALL', value: 'ALL' }
]

export function PortfolioValueChart({ 
  className, 
  height = 400, 
  portfolioId,
  dateRange = '1M',
  onDateRangeChange
}: PortfolioValueChartProps) {
  const [data, setData] = useState<PortfolioValueDataPoint[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedRange, setSelectedRange] = useState(dateRange)

  const fetchPortfolioValue = async () => {
    try {
      setLoading(true)
      setError(null)
      
      const params = new URLSearchParams()
      if (portfolioId) params.append('portfolio_id', portfolioId)
      if (selectedRange) params.append('date_range', selectedRange)
      
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/api/portfolio/analytics/value-history/?${params}`,
        {
          headers: {
            'Authorization': `Bearer ${localStorage.getItem('accessToken')}`,
            'Content-Type': 'application/json',
          },
        }
      )

      if (!response.ok) {
        throw new Error(`Failed to fetch portfolio value: ${response.status}`)
      }

      const result = await response.json()
      setData(result)
    } catch (error) {
      console.error('Failed to fetch portfolio value:', error)
      setError(error instanceof Error ? error.message : 'Failed to load data')
      
      // Mock data for development
      const mockData: PortfolioValueDataPoint[] = []
      const startDate = new Date('2025-01-01')
      const endDate = new Date()
      const daysDiff = Math.floor((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24))
      
      let baseValue = 4000000
      for (let i = 0; i <= daysDiff; i++) {
        const date = new Date(startDate)
        date.setDate(date.getDate() + i)
        
        // Simulate market volatility
        const dailyChange = (Math.random() - 0.5) * 0.04 // ±2% daily volatility
        const trend = 0.0003 // Slight upward trend
        baseValue = baseValue * (1 + dailyChange + trend)
        
        const prevValue = i > 0 ? mockData[i-1].value : baseValue
        const change = baseValue - prevValue
        const changePercent = (change / prevValue) * 100
        
        mockData.push({
          date: date.toISOString().split('T')[0],
          value: Math.round(baseValue),
          change: Math.round(change),
          change_percent: Number(changePercent.toFixed(2))
        })
      }
      
      setData(mockData)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchPortfolioValue()
  }, [portfolioId, selectedRange])

  const handleDateRangeChange = (range: string) => {
    setSelectedRange(range)
    onDateRangeChange?.(range)
  }

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value)
  }

  const formatCompactCurrency = (value: number) => {
    if (value >= 1000000) {
      return `$${(value / 1000000).toFixed(1)}M`
    } else if (value >= 1000) {
      return `$${(value / 1000).toFixed(0)}K`
    }
    return formatCurrency(value)
  }

  // Calculate summary statistics
  const currentValue = data.length > 0 ? data[data.length - 1].value : 0
  const firstValue = data.length > 0 ? data[0].value : 0
  const totalChange = currentValue - firstValue
  const totalChangePercent = firstValue > 0 ? (totalChange / firstValue) * 100 : 0
  const isPositive = totalChange >= 0

  const chartOptions = {
    chart: {
      type: 'area' as const,
      fontFamily: 'inherit',
      toolbar: {
        show: false
      },
      zoom: {
        enabled: false
      },
      animations: {
        enabled: true,
        easing: 'easeinout',
        speed: 800
      }
    },
    grid: {
      borderColor: '#f1f5f9',
      strokeDashArray: 3,
      xaxis: {
        lines: {
          show: false
        }
      },
      yaxis: {
        lines: {
          show: true
        }
      }
    },
    dataLabels: {
      enabled: false
    },
    stroke: {
      curve: 'smooth' as const,
      width: 3,
      colors: [isPositive ? '#10b981' : '#ef4444']
    },
    fill: {
      type: 'gradient',
      gradient: {
        shadeIntensity: 1,
        opacityFrom: 0.3,
        opacityTo: 0.0,
        stops: [0, 100],
        colorStops: [{
          offset: 0,
          color: isPositive ? '#10b981' : '#ef4444',
          opacity: 0.3
        }, {
          offset: 100,
          color: isPositive ? '#10b981' : '#ef4444',
          opacity: 0.0
        }]
      }
    },
    xaxis: {
      type: 'datetime',
      categories: data.map(item => item.date),
      axisBorder: {
        show: false
      },
      axisTicks: {
        show: false
      },
      labels: {
        style: {
          colors: '#64748b',
          fontSize: '12px'
        },
        datetimeFormatter: {
          year: 'yyyy',
          month: 'MMM',
          day: 'dd MMM',
          hour: 'HH:mm'
        }
      }
    },
    yaxis: {
      labels: {
        style: {
          colors: '#64748b',
          fontSize: '12px'
        },
        formatter: (val: number) => formatCompactCurrency(val)
      }
    },
    tooltip: {
      enabled: true,
      theme: 'light',
      custom: ({ series, seriesIndex, dataPointIndex, w }: any) => {
        const item = data[dataPointIndex]
        const date = new Date(item.date).toLocaleDateString('en-US', {
          year: 'numeric',
          month: 'short',
          day: 'numeric'
        })
        
        return `
          <div class="px-4 py-3 bg-white shadow-lg rounded-lg border">
            <div class="font-semibold text-gray-900 mb-2">${date}</div>
            <div class="space-y-1 text-sm">
              <div>Value: ${formatCurrency(item.value)}</div>
              <div class="${item.change >= 0 ? 'text-green-600' : 'text-red-600'}">
                Change: ${item.change >= 0 ? '+' : ''}${formatCurrency(item.change)} (${item.change_percent >= 0 ? '+' : ''}${item.change_percent}%)
              </div>
            </div>
          </div>
        `
      }
    },
    markers: {
      size: 0,
      hover: {
        size: 8,
        sizeOffset: 3
      }
    },
    responsive: [{
      breakpoint: 480,
      options: {
        chart: {
          height: 300
        }
      }
    }]
  }

  const series = [{
    name: 'Portfolio Value',
    data: data.map(item => item.value)
  }]

  if (loading) {
    return (
      <Card className={cn("", className)}>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <TrendingUp className="h-5 w-5 text-aurum-primary" />
                Portfolio Value
              </CardTitle>
              <CardDescription>Historical portfolio value over time</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="flex justify-center">
            <Skeleton className="w-full h-80 rounded-lg" />
          </div>
        </CardContent>
      </Card>
    )
  }

  if (error) {
    return (
      <Card className={cn("", className)}>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-red-600">
            <TrendingUp className="h-5 w-5" />
            Portfolio Value  
          </CardTitle>
          <CardDescription>Historical portfolio value over time</CardDescription>  
        </CardHeader>
        <CardContent>
          <div className="flex flex-col items-center justify-center h-80 text-gray-500">
            <div className="text-lg font-medium">Failed to load data</div>
            <div className="text-sm">{error}</div>
          </div>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card className={cn("", className)}>
      <CardHeader>
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <div>
            <CardTitle className="flex items-center gap-2">
              {isPositive ? (
                <TrendingUp className="h-5 w-5 text-green-500" />
              ) : totalChange === 0 ? (
                <Minus className="h-5 w-5 text-gray-500" />
              ) : (
                <TrendingDown className="h-5 w-5 text-red-500" />
              )}
              Portfolio Value
            </CardTitle>
            <CardDescription>Historical portfolio value over time</CardDescription>
          </div>
          
          {/* Date Range Selector */}
          <div className="flex gap-1 bg-gray-100 rounded-lg p-1">
            {DATE_RANGES.map((range) => (
              <Button
                key={range.value}
                size="sm"
                variant={selectedRange === range.value ? "default" : "ghost"}
                onClick={() => handleDateRangeChange(range.value)}
                className="text-xs px-3 py-1 h-8"
              >
                {range.label}
              </Button>
            ))}
          </div>
        </div>
        
        {/* Summary Stats */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
          <div>
            <div className="text-2xl font-bold text-gray-900">
              {formatCurrency(currentValue)}
            </div>
            <div className="text-sm text-gray-500">Current Value</div>
          </div>
          <div>
            <div className={cn(
              "text-2xl font-bold",
              isPositive ? "text-green-600" : totalChange === 0 ? "text-gray-600" : "text-red-600"
            )}>
              {totalChange >= 0 ? '+' : ''}{formatCurrency(totalChange)}
            </div>
            <div className="text-sm text-gray-500">Total Change</div>
          </div>
          <div>
            <div className={cn(
              "text-2xl font-bold",
              isPositive ? "text-green-600" : totalChange === 0 ? "text-gray-600" : "text-red-600"
            )}>
              {totalChangePercent >= 0 ? '+' : ''}{totalChangePercent.toFixed(2)}%
            </div>
            <div className="text-sm text-gray-500">Percentage Change</div>
          </div>
          <div>
            <Badge 
              variant="secondary" 
              className={cn(
                "text-sm",
                isPositive ? "bg-green-100 text-green-800" : 
                totalChange === 0 ? "bg-gray-100 text-gray-600" : 
                "bg-red-100 text-red-800"
              )}
            >
              {selectedRange} Range
            </Badge>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <Chart
          options={chartOptions}
          series={series}
          type="area"
          height={height}
        />
      </CardContent>
    </Card>
  )
}