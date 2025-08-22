'use client'

import React, { useEffect, useState } from 'react'
import dynamic from 'next/dynamic'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { BarChart3, TrendingUp, Target, Shield } from 'lucide-react'

// Dynamically import ApexCharts to avoid SSR issues
const Chart = dynamic(() => import('react-apexcharts'), { ssr: false })

export interface PortfolioMetrics {
  period: string
  modified_dietz_return: number
  total_return: number
  volatility: number
  sharpe_ratio: number
  max_drawdown: number
  alpha: number
  beta: number
  benchmark_return?: number
}

interface PortfolioMetricsChartProps {
  className?: string
  height?: number
  portfolioId?: string
  dateRange?: string
  chartType?: 'returns' | 'risk_metrics' | 'performance_attribution'
}

const METRIC_TYPES = [
  { label: 'Returns', value: 'returns' },
  { label: 'Risk Metrics', value: 'risk_metrics' },
  { label: 'Attribution', value: 'performance_attribution' }
]

export function PortfolioMetricsChart({ 
  className, 
  height = 400, 
  portfolioId,
  dateRange = '1Y',
  chartType = 'returns'
}: PortfolioMetricsChartProps) {
  const [data, setData] = useState<PortfolioMetrics[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedMetric, setSelectedMetric] = useState(chartType)

  const fetchPortfolioMetrics = async () => {
    try {
      setLoading(true)
      setError(null)
      
      const params = new URLSearchParams()
      if (portfolioId) params.append('portfolio_id', portfolioId)
      if (dateRange) params.append('date_range', dateRange)
      if (selectedMetric) params.append('metric_type', selectedMetric)
      
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/api/portfolio/analytics/metrics/?${params}`,
        {
          headers: {
            'Authorization': `Bearer ${localStorage.getItem('accessToken')}`,
            'Content-Type': 'application/json',
          },
        }
      )

      if (!response.ok) {
        throw new Error(`Failed to fetch portfolio metrics: ${response.status}`)
      }

      const result = await response.json()
      setData(result)
    } catch (error) {
      console.error('Failed to fetch portfolio metrics:', error)
      setError(error instanceof Error ? error.message : 'Failed to load data')
      
      // Mock data for development
      const periods = ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', '2025-07']
      const mockData: PortfolioMetrics[] = periods.map(period => ({
        period,
        modified_dietz_return: (Math.random() - 0.5) * 0.15 + 0.02, // ±7.5% around 2%
        total_return: (Math.random() - 0.5) * 0.12 + 0.015, // ±6% around 1.5%
        volatility: Math.random() * 0.05 + 0.08, // 8-13%
        sharpe_ratio: Math.random() * 1.2 + 0.8, // 0.8-2.0
        max_drawdown: -(Math.random() * 0.08 + 0.02), // -2% to -10%
        alpha: (Math.random() - 0.5) * 0.04, // ±2%
        beta: Math.random() * 0.6 + 0.7, // 0.7-1.3
        benchmark_return: (Math.random() - 0.5) * 0.10 + 0.01 // ±5% around 1%
      }))
      
      setData(mockData)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchPortfolioMetrics()
  }, [portfolioId, dateRange, selectedMetric])

  const formatPercentage = (value: number, decimals = 2) => {
    return `${(value * 100).toFixed(decimals)}%`
  }

  const formatRatio = (value: number, decimals = 2) => {
    return value.toFixed(decimals)
  }

  const getMetricIcon = (type: string) => {
    switch (type) {
      case 'returns':
        return <TrendingUp className="h-5 w-5 text-green-500" />
      case 'risk_metrics':
        return <Shield className="h-5 w-5 text-orange-500" />
      case 'performance_attribution':
        return <Target className="h-5 w-5 text-blue-500" />
      default:
        return <BarChart3 className="h-5 w-5 text-aurum-primary" />
    }
  }

  const getChartConfig = () => {
    switch (selectedMetric) {
      case 'returns':
        return {
          title: 'Return Analysis',
          series: [
            {
              name: 'Modified Dietz Return',
              data: data.map(item => item.modified_dietz_return * 100),
              color: '#10b981'
            },
            {
              name: 'Total Return',
              data: data.map(item => item.total_return * 100),
              color: '#3b82f6'
            },
            ...(data[0]?.benchmark_return !== undefined ? [{
              name: 'Benchmark Return',
              data: data.map(item => (item.benchmark_return || 0) * 100),
              color: '#6b7280'
            }] : [])
          ],
          yAxisFormatter: (val: number) => `${val.toFixed(1)}%`
        }
      
      case 'risk_metrics':
        return {
          title: 'Risk Analysis',
          series: [
            {
              name: 'Volatility',
              data: data.map(item => item.volatility * 100),
              color: '#f59e0b'
            },
            {
              name: 'Max Drawdown',
              data: data.map(item => Math.abs(item.max_drawdown) * 100),
              color: '#ef4444'
            }
          ],
          yAxisFormatter: (val: number) => `${val.toFixed(1)}%`
        }
      
      case 'performance_attribution':
        return {
          title: 'Performance Attribution',
          series: [
            {
              name: 'Alpha',
              data: data.map(item => item.alpha * 100),
              color: '#8b5cf6'
            },
            {
              name: 'Beta',
              data: data.map(item => item.beta),
              color: '#06b6d4'
            },
            {
              name: 'Sharpe Ratio',
              data: data.map(item => item.sharpe_ratio),
              color: '#10b981'
            }
          ],
          yAxisFormatter: (val: number) => val.toFixed(2)
        }
      
      default:
        return {
          title: 'Portfolio Metrics',
          series: [],
          yAxisFormatter: (val: number) => val.toFixed(2)
        }
    }
  }

  const chartConfig = getChartConfig()

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
    colors: chartConfig.series.map(s => s.color),
    xaxis: {
      categories: data.map(item => {
        const date = new Date(item.period + '-01')
        return date.toLocaleDateString('en-US', { month: 'short', year: 'numeric' })
      }),
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
        }
      }
    },
    yaxis: {
      labels: {
        style: {
          colors: '#64748b',
          fontSize: '12px'
        },
        formatter: chartConfig.yAxisFormatter
      }
    },
    tooltip: {
      enabled: true,
      theme: 'light',
      shared: true,
      intersect: false,
      custom: ({ series, seriesIndex, dataPointIndex, w }: any) => {
        const item = data[dataPointIndex]
        const date = new Date(item.period + '-01').toLocaleDateString('en-US', {
          month: 'long',
          year: 'numeric'
        })
        
        let content = `<div class="px-4 py-3 bg-white shadow-lg rounded-lg border">
          <div class="font-semibold text-gray-900 mb-2">${date}</div>
          <div class="space-y-1 text-sm">`
        
        if (selectedMetric === 'returns') {
          content += `
            <div>Modified Dietz Return: <span class="font-medium text-green-600">${formatPercentage(item.modified_dietz_return)}</span></div>
            <div>Total Return: <span class="font-medium text-blue-600">${formatPercentage(item.total_return)}</span></div>
            ${item.benchmark_return ? `<div>Benchmark Return: <span class="font-medium text-gray-600">${formatPercentage(item.benchmark_return)}</span></div>` : ''}
          `
        } else if (selectedMetric === 'risk_metrics') {
          content += `
            <div>Volatility: <span class="font-medium text-yellow-600">${formatPercentage(item.volatility)}</span></div>
            <div>Max Drawdown: <span class="font-medium text-red-600">${formatPercentage(item.max_drawdown)}</span></div>
          `
        } else if (selectedMetric === 'performance_attribution') {
          content += `
            <div>Alpha: <span class="font-medium text-purple-600">${formatPercentage(item.alpha)}</span></div>
            <div>Beta: <span class="font-medium text-cyan-600">${formatRatio(item.beta)}</span></div>
            <div>Sharpe Ratio: <span class="font-medium text-green-600">${formatRatio(item.sharpe_ratio)}</span></div>
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

  // Calculate summary statistics
  const latestData = data.length > 0 ? data[data.length - 1] : null
  const avgModifiedDietz = data.length > 0 ? 
    data.reduce((sum, item) => sum + item.modified_dietz_return, 0) / data.length : 0
  const avgVolatility = data.length > 0 ?
    data.reduce((sum, item) => sum + item.volatility, 0) / data.length : 0

  if (loading) {
    return (
      <Card className={cn("", className)}>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <BarChart3 className="h-5 w-5 text-aurum-primary" />
                Portfolio Metrics
              </CardTitle>
              <CardDescription>Key performance and risk metrics</CardDescription>
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
            <BarChart3 className="h-5 w-5" />
            Portfolio Metrics
          </CardTitle>
          <CardDescription>Key performance and risk metrics</CardDescription>
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
              {getMetricIcon(selectedMetric)}
              {chartConfig.title}
            </CardTitle>
            <CardDescription>Key performance and risk metrics analysis</CardDescription>
          </div>
          
          {/* Metric Type Selector */}
          <div className="flex gap-1 bg-gray-100 rounded-lg p-1">
            {METRIC_TYPES.map((type) => (
              <Button
                key={type.value}
                size="sm"
                variant={selectedMetric === type.value ? "default" : "ghost"}
                onClick={() => setSelectedMetric(type.value)}
                className="text-xs px-3 py-1 h-8"
              >
                {type.label}
              </Button>
            ))}
          </div>
        </div>
        
        {/* Summary Stats */}
        {latestData && (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
            <div>
              <div className="text-xl font-bold text-green-600">
                {formatPercentage(latestData.modified_dietz_return)}
              </div>
              <div className="text-sm text-gray-500">Latest Modified Dietz</div>
            </div>
            <div>
              <div className="text-xl font-bold text-blue-600">
                {formatPercentage(avgModifiedDietz)}
              </div>
              <div className="text-sm text-gray-500">Avg. Return</div>
            </div>
            <div>
              <div className="text-xl font-bold text-orange-600">
                {formatPercentage(avgVolatility)}
              </div>
              <div className="text-sm text-gray-500">Avg. Volatility</div>
            </div>
            <div>
              <div className="text-xl font-bold text-purple-600">
                {formatRatio(latestData.sharpe_ratio)}
              </div>
              <div className="text-sm text-gray-500">Latest Sharpe Ratio</div>
            </div>
          </div>
        )}
      </CardHeader>
      <CardContent>
        <Chart
          options={chartOptions}
          series={chartConfig.series}
          type="line"
          height={height}
        />
      </CardContent>
    </Card>
  )
}