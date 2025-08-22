'use client'

import React, { useEffect, useState } from 'react'
import dynamic from 'next/dynamic'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'
import { PieChart } from 'lucide-react'

// Dynamically import ApexCharts to avoid SSR issues
const Chart = dynamic(() => import('react-apexcharts'), { ssr: false })

export interface AssetAllocation {
  asset_class: string
  value: number
  percentage: number
  count: number
  color?: string
}

interface AssetAllocationChartProps {
  className?: string
  height?: number
  portfolioId?: string
  dateRange?: string
}

export function AssetAllocationChart({ 
  className, 
  height = 400, 
  portfolioId,
  dateRange = '1M'
}: AssetAllocationChartProps) {
  const [data, setData] = useState<AssetAllocation[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchAssetAllocation = async () => {
    try {
      setLoading(true)
      setError(null)
      
      const params = new URLSearchParams()
      if (portfolioId) params.append('portfolio_id', portfolioId)
      if (dateRange) params.append('date_range', dateRange)
      
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/api/portfolio/analytics/asset-allocation/?${params}`,
        {
          headers: {
            'Authorization': `Bearer ${localStorage.getItem('accessToken')}`,
            'Content-Type': 'application/json',
          },
        }
      )

      if (!response.ok) {
        throw new Error(`Failed to fetch asset allocation: ${response.status}`)
      }

      const result = await response.json()
      
      // Assign colors to asset classes
      const colors = [
        '#1f77b4', // Blue
        '#ff7f0e', // Orange  
        '#2ca02c', // Green
        '#d62728', // Red
        '#9467bd', // Purple
        '#8c564b', // Brown
        '#e377c2', // Pink
        '#7f7f7f', // Gray
        '#bcbd22', // Olive
        '#17becf'  // Cyan
      ]
      
      const dataWithColors = result.map((item: AssetAllocation, index: number) => ({
        ...item,
        color: colors[index % colors.length]
      }))
      
      setData(dataWithColors)
    } catch (error) {
      console.error('Failed to fetch asset allocation:', error)
      setError(error instanceof Error ? error.message : 'Failed to load data')
      
      // Mock data for development
      setData([
        { asset_class: 'Equities', value: 2500000, percentage: 62.5, count: 145, color: '#1f77b4' },
        { asset_class: 'Fixed Income', value: 800000, percentage: 20.0, count: 89, color: '#ff7f0e' },
        { asset_class: 'Alternatives', value: 400000, percentage: 10.0, count: 34, color: '#2ca02c' },
        { asset_class: 'Cash & Equivalents', value: 200000, percentage: 5.0, count: 12, color: '#d62728' },
        { asset_class: 'Real Estate', value: 100000, percentage: 2.5, count: 8, color: '#9467bd' }
      ])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchAssetAllocation()
  }, [portfolioId, dateRange])

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value)
  }

  const chartOptions = {
    chart: {
      type: 'donut' as const,
      fontFamily: 'inherit',
      toolbar: {
        show: false
      },
      animations: {
        enabled: true,
        easing: 'easeinout',
        speed: 800,
        animateGradually: {
          enabled: true,
          delay: 150
        },
        dynamicAnimation: {
          enabled: true,
          speed: 350
        }
      }
    },
    labels: data.map(item => item.asset_class),
    colors: data.map(item => item.color || '#1f77b4'),
    plotOptions: {
      pie: {
        donut: {
          size: '65%',
          labels: {
            show: true,
            name: {
              show: true,
              fontSize: '16px',
              fontWeight: 600,
              color: '#374151',
              offsetY: -10
            },
            value: {
              show: true,
              fontSize: '24px',
              fontWeight: 700,
              color: '#1f2937',
              offsetY: 10,
              formatter: (val: string) => `${parseFloat(val).toFixed(1)}%`
            },
            total: {
              show: true,
              showAlways: true,
              label: 'Total Value',
              fontSize: '14px',
              fontWeight: 400,
              color: '#6b7280',
              formatter: () => {
                const total = data.reduce((sum, item) => sum + item.value, 0)
                return formatCurrency(total)
              }
            }
          }
        }
      }
    },
    dataLabels: {
      enabled: true,
      formatter: (val: number) => `${val.toFixed(1)}%`,
      style: {
        fontSize: '12px',
        fontWeight: 'bold'
      },
      dropShadow: {
        enabled: false
      }
    },
    legend: {
      show: false // We'll use custom legend
    },
    tooltip: {
      enabled: true,
      theme: 'light',
      custom: ({ series, seriesIndex, dataPointIndex, w }: any) => {
        const item = data[seriesIndex]
        return `
          <div class="px-3 py-2 bg-white shadow-lg rounded-lg border">
            <div class="font-semibold text-gray-900 mb-1">${item.asset_class}</div>
            <div class="text-sm text-gray-600">
              <div>Value: ${formatCurrency(item.value)}</div>
              <div>Percentage: ${item.percentage.toFixed(1)}%</div>
              <div>Positions: ${item.count}</div>
            </div>
          </div>
        `
      }
    },
    responsive: [{
      breakpoint: 480,
      options: {
        chart: {
          height: 300
        },
        plotOptions: {
          pie: {
            donut: {
              size: '60%',
              labels: {
                name: {
                  fontSize: '14px'
                },
                value: {
                  fontSize: '20px'
                },
                total: {
                  fontSize: '12px'
                }
              }
            }
          }
        }
      }
    }]
  }

  const series = data.map(item => item.percentage)

  if (loading) {
    return (
      <Card className={cn("", className)}>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <PieChart className="h-5 w-5 text-aurum-primary" />
                Asset Allocation
              </CardTitle>
              <CardDescription>Portfolio breakdown by asset class</CardDescription>
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
            <PieChart className="h-5 w-5" />
            Asset Allocation
          </CardTitle>
          <CardDescription>Portfolio breakdown by asset class</CardDescription>
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
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="flex items-center gap-2">
              <PieChart className="h-5 w-5 text-aurum-primary" />
              Asset Allocation
            </CardTitle>
            <CardDescription>Portfolio breakdown by asset class</CardDescription>
          </div>
          <Badge variant="secondary" className="text-xs">
            {dateRange} Range
          </Badge>
        </div>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Chart */}
          <div className="lg:col-span-2">
            <Chart
              options={chartOptions}
              series={series}
              type="donut"
              height={height}
            />
          </div>
          
          {/* Custom Legend */}
          <div className="space-y-3">
            <h4 className="font-semibold text-sm text-gray-700 mb-3">Asset Classes</h4>
            {data.map((item, index) => (
              <div key={index} className="flex items-center justify-between p-2 rounded-lg hover:bg-gray-50 transition-colors">
                <div className="flex items-center gap-3">
                  <div 
                    className="w-3 h-3 rounded-full"
                    style={{ backgroundColor: item.color }}
                  />
                  <div>
                    <div className="font-medium text-sm text-gray-900">{item.asset_class}</div>
                    <div className="text-xs text-gray-500">{item.count} positions</div>
                  </div>
                </div>
                <div className="text-right">
                  <div className="font-semibold text-sm">{item.percentage.toFixed(1)}%</div>
                  <div className="text-xs text-gray-500">{formatCurrency(item.value)}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}