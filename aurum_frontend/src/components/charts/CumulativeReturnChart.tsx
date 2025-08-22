'use client'

import React, { useEffect, useState } from 'react'
import dynamic from 'next/dynamic'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { LineChart, TrendingUp, TrendingDown, BarChart } from 'lucide-react'

// Dynamically import ApexCharts to avoid SSR issues
const Chart = dynamic(() => import('react-apexcharts'), { ssr: false })

export interface CumulativeReturnDataPoint {
  date: string
  cumulative_return: number
  period_return: number
  benchmark_cumulative_return?: number
  benchmark_period_return?: number
}

interface CumulativeReturnChartProps {
  className?: string
  height?: number
  portfolioId?: string
  dateRange?: string
  showBenchmark?: boolean 
  comparisonMode?: 'absolute' | 'relative'
}

const DATE_RANGES = [
  { label: '1M', value: '1M' },
  { label: '3M', value: '3M' },
  { label: '6M', value: '6M' },
  { label: '1Y', value: '1Y' },
  { label: '3Y', value: '3Y' },
  { label: 'ALL', value: 'ALL' }
]

export function CumulativeReturnChart({ 
  className, 
  height = 400, 
  portfolioId,
  dateRange = '1Y',
  showBenchmark = true,
  comparisonMode = 'absolute'
}: CumulativeReturnChartProps) {
  const [data, setData] = useState<CumulativeReturnDataPoint[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedRange, setSelectedRange] = useState(dateRange)
  const [selectedMode, setSelectedMode] = useState(comparisonMode)

  const fetchCumulativeReturns = async () => {
    try {
      setLoading(true)
      setError(null)
      
      const params = new URLSearchParams()
      if (portfolioId) params.append('portfolio_id', portfolioId)
      if (selectedRange) params.append('date_range', selectedRange)
      if (showBenchmark) params.append('include_benchmark', 'true')
      
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/api/portfolio/analytics/cumulative-returns/?${params}`,
        {
          headers: {
            'Authorization': `Bearer ${localStorage.getItem('accessToken')}`,
            'Content-Type': 'application/json',
          },
        }
      )

      if (!response.ok) {
        throw new Error(`Failed to fetch cumulative returns: ${response.status}`)
      }

      const result = await response.json()
      setData(result)
    } catch (error) {
      console.error('Failed to fetch cumulative returns:', error)
      setError(error instanceof Error ? error.message : 'Failed to load data')
      
      // Mock data for development
      const mockData: CumulativeReturnDataPoint[] = []
      const startDate = new Date('2024-01-01')
      const endDate = new Date()
      const daysDiff = Math.floor((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24))
      
      let portfolioCumulative = 0
      let benchmarkCumulative = 0
      
      for (let i = 0; i <= daysDiff; i += 7) { // Weekly data points
        const date = new Date(startDate)
        date.setDate(date.getDate() + i)
        
        // Simulate portfolio returns with higher volatility
        const portfolioPeriodReturn = (Math.random() - 0.5) * 0.06 + 0.001 // ±3% weekly, slight upward bias
        portfolioCumulative = (1 + portfolioCumulative) * (1 + portfolioPeriodReturn) - 1
        
        // Simulate benchmark returns with lower volatility
        const benchmarkPeriodReturn = (Math.random() - 0.5) * 0.04 + 0.0008 // ±2% weekly, slight upward bias
        benchmarkCumulative = (1 + benchmarkCumulative) * (1 + benchmarkPeriodReturn) - 1
        
        mockData.push({
          date: date.toISOString().split('T')[0],
          cumulative_return: portfolioCumulative,
          period_return: portfolioPeriodReturn,
          benchmark_cumulative_return: benchmarkCumulative,
          benchmark_period_return: benchmarkPeriodReturn
        })
      }
      
      setData(mockData)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchCumulativeReturns()
  }, [portfolioId, selectedRange, showBenchmark])

  const handleDateRangeChange = (range: string) => {
    setSelectedRange(range)
  }

  const formatPercentage = (value: number, decimals = 2) => {
    return `${(value * 100).toFixed(decimals)}%`
  }

  // Calculate relative performance vs benchmark
  const getRelativeReturns = () => {
    return data.map(item => ({
      date: item.date,
      relative_return: item.cumulative_return - (item.benchmark_cumulative_return || 0)
    }))
  }

  const relativeData = getRelativeReturns()

  // Calculate summary statistics
  const latestData = data.length > 0 ? data[data.length - 1] : null
  const totalReturn = latestData?.cumulative_return || 0
  const benchmarkReturn = latestData?.benchmark_cumulative_return || 0
  const outperformance = totalReturn - benchmarkReturn
  const isOutperforming = outperformance > 0

  // Determine chart series based on mode
  const getChartSeries = () => {
    if (selectedMode === 'relative') {
      return [{
        name: 'Relative Performance',
        data: relativeData.map(item => item.relative_return * 100),
        color: isOutperforming ? '#10b981' : '#ef4444'
      }]
    }

    const series = [{
      name: 'Portfolio Return',
      data: data.map(item => item.cumulative_return * 100),
      color: '#1f77b4'
    }]

    if (showBenchmark && data[0]?.benchmark_cumulative_return !== undefined) {
      series.push({
        name: 'Benchmark Return',
        data: data.map(item => (item.benchmark_cumulative_return || 0) * 100),
        color: '#6b7280'
      })
    }

    return series
  }

  const series = getChartSeries()

  const chartOptions = {
    chart: {
      type: 'line' as const,
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
      width: 3
    },
    colors: series.map(s => s.color),
    fill: selectedMode === 'relative' ? {
      type: 'gradient',
      gradient: {
        shadeIntensity: 1,
        opacityFrom: 0.2,
        opacityTo: 0.0,
        stops: [0, 100],
        colorStops: [{
          offset: 0,
          color: series[0].color,
          opacity: 0.2
        }, {
          offset: 100,
          color: series[0].color,
          opacity: 0.0
        }]
      }
    } : undefined,
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
        formatter: (val: number) => `${val.toFixed(1)}%`
      },
      ...(selectedMode === 'relative' && {
        min: Math.min(...relativeData.map(item => item.relative_return * 100)) - 1,
        max: Math.max(...relativeData.map(item => item.relative_return * 100)) + 1
      })
    },
    tooltip: {
      enabled: true,
      theme: 'light',
      shared: true,
      intersect: false,
      custom: ({ series, seriesIndex, dataPointIndex, w }: any) => {
        const item = data[dataPointIndex]
        const date = new Date(item.date).toLocaleDateString('en-US', {
          year: 'numeric',
          month: 'short',
          day: 'numeric'
        })
        
        let content = `<div class="px-4 py-3 bg-white shadow-lg rounded-lg border">
          <div class="font-semibold text-gray-900 mb-2">${date}</div>
          <div class="space-y-1 text-sm">`
        
        if (selectedMode === 'relative') {
          const relativeReturn = item.cumulative_return - (item.benchmark_cumulative_return || 0)
          content += `
            <div>Relative Performance: <span class="font-medium ${relativeReturn >= 0 ? 'text-green-600' : 'text-red-600'}">${formatPercentage(relativeReturn)}</span></div>
            <div>Portfolio Return: <span class="font-medium text-blue-600">${formatPercentage(item.cumulative_return)}</span></div>
            <div>Benchmark Return: <span class="font-medium text-gray-600">${formatPercentage(item.benchmark_cumulative_return || 0)}</span></div>
          `
        } else {
          content += `
            <div>Portfolio Return: <span class="font-medium text-blue-600">${formatPercentage(item.cumulative_return)}</span></div>
            ${item.benchmark_cumulative_return !== undefined ? 
              `<div>Benchmark Return: <span class="font-medium text-gray-600">${formatPercentage(item.benchmark_cumulative_return)}</span></div>` : ''
            }
            <div>Period Return: <span class="font-medium">${formatPercentage(item.period_return)}</span></div>
          `
        }
        
        content += `</div></div>`
        return content
      }
    },
    legend: {
      position: 'top',
      horizontalAlign: 'right',
      fontSize: '12px',
      markers: {
        width: 8,
        height: 8,
        radius: 4
      }
    },
    markers: {
      size: 0,
      hover: {
        size: 6,
        sizeOffset: 3
      }
    },
    annotations: selectedMode === 'relative' ? {
      yaxis: [{
        y: 0,
        borderColor: '#6b7280',
        borderWidth: 1,
        strokeDashArray: 5,
        label: {
          text: 'Benchmark',
          style: {
            color: '#6b7280',
            background: '#f9fafb'
          }
        }
      }]
    } : undefined,
    responsive: [{
      breakpoint: 480,
      options: {
        chart: {
          height: 300
        },
        legend: {
          position: 'bottom'
        }
      }
    }]
  }

  if (loading) {
    return (
      <Card className={cn("", className)}>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <LineChart className="h-5 w-5 text-aurum-primary" />
                Cumulative Returns
              </CardTitle>
              <CardDescription>Portfolio performance over time</CardDescription>
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
            <LineChart className="h-5 w-5" />
            Cumulative Returns
          </CardTitle>
          <CardDescription>Portfolio performance over time</CardDescription>
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
              {selectedMode === 'relative' ? (
                isOutperforming ? (
                  <TrendingUp className="h-5 w-5 text-green-500" />
                ) : (
                  <TrendingDown className="h-5 w-5 text-red-500" />
                )
              ) : (
                <LineChart className="h-5 w-5 text-aurum-primary" />
              )}
              {selectedMode === 'relative' ? 'Relative Performance' : 'Cumulative Returns'}
            </CardTitle>
            <CardDescription>
              {selectedMode === 'relative' ? 
                'Portfolio performance vs benchmark' : 
                'Portfolio performance over time'
              }
            </CardDescription>
          </div>
          
          {/* Controls */}
          <div className="flex gap-2">
            {/* View Mode Toggle */}
            <div className="flex gap-1 bg-gray-100 rounded-lg p-1">
              <Button
                size="sm"
                variant={selectedMode === 'absolute' ? "default" : "ghost"}
                onClick={() => setSelectedMode('absolute')}
                className="text-xs px-3 py-1 h-8"
              >
                <BarChart className="h-3 w-3 mr-1" />
                Absolute
              </Button>
              <Button
                size="sm"
                variant={selectedMode === 'relative' ? "default" : "ghost"}
                onClick={() => setSelectedMode('relative')}
                className="text-xs px-3 py-1 h-8"
              >
                <TrendingUp className="h-3 w-3 mr-1" />
                Relative
              </Button>
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
        </div>
        
        {/* Summary Stats */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
          <div>
            <div className="text-2xl font-bold text-blue-600">
              {formatPercentage(totalReturn)}
            </div>
            <div className="text-sm text-gray-500">Portfolio Return</div>
          </div>
          {showBenchmark && (
            <div>
              <div className="text-2xl font-bold text-gray-600">
                {formatPercentage(benchmarkReturn)}
              </div>
              <div className="text-sm text-gray-500">Benchmark Return</div>
            </div>
          )}
          <div>
            <div className={cn(
              "text-2xl font-bold",
              isOutperforming ? "text-green-600" : "text-red-600"
            )}>
              {outperformance >= 0 ? '+' : ''}{formatPercentage(outperformance)}
            </div>
            <div className="text-sm text-gray-500">Outperformance</div>
          </div>
          <div>
            <Badge 
              variant="secondary" 
              className={cn(
                "text-sm",
                isOutperforming ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"
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
          type={selectedMode === 'relative' ? 'area' : 'line'}
          height={height}
        />
      </CardContent>
    </Card>
  )
}