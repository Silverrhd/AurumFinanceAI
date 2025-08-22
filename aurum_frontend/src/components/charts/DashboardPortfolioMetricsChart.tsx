'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { ApexOptions } from 'apexcharts';

const Chart = dynamic(() => import('react-apexcharts'), { ssr: false });

interface PortfolioMetricsData {
  hasData: boolean;
  message: string;
  series: Array<{
    name: string;
    data: number[];
  }>;
  categories: string[];
  colors: string[];
  yAxisMin: number;
  yAxisMax: number;
}

interface DashboardPortfolioMetricsChartProps {
  data: PortfolioMetricsData;
}

export const DashboardPortfolioMetricsChart: React.FC<DashboardPortfolioMetricsChartProps> = ({ data }) => {
  if (!data.hasData || data.series.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-gray-500">
        {data.message || 'No portfolio metrics data available'}
      </div>
    );
  }

  const options: ApexOptions = {
    chart: {
      type: 'bar',
      height: 400,
      toolbar: { show: false },
      animations: { 
        enabled: true,
        easing: 'easeinout',
        speed: 800
      },
      background: 'transparent',
      fontFamily: 'Arial, sans-serif'
    },
    colors: data.colors,
    plotOptions: {
      bar: {
        horizontal: false,
        columnWidth: '70%',
        endingShape: 'rounded'
      }
    },
    dataLabels: { enabled: false },
    stroke: {
      show: true,
      width: 2,
      colors: ['transparent']
    },
    xaxis: {
      categories: data.categories,
      labels: {
        style: {
          colors: '#666',
          fontSize: '12px'
        }
      },
      axisBorder: { show: false },
      axisTicks: { show: false }
    },
    yaxis: {
      title: {
        text: 'Value ($)',
        style: {
          color: '#666',
          fontSize: '14px',
          fontWeight: 500
        }
      },
      labels: {
        formatter: function(val: number) {
          return '$' + val.toLocaleString();
        },
        style: {
          colors: '#666',
          fontSize: '12px'
        }
      },
      min: data.yAxisMin,
      max: data.yAxisMax
    },
    grid: {
      borderColor: '#e0e0e0',
      strokeDashArray: 3,
      xaxis: { lines: { show: false } },
      yaxis: { lines: { show: true } },
      padding: { left: 20, right: 20 }
    },
    tooltip: {
      theme: 'light',
      y: {
        formatter: function(val: number) {
          return '$' + val.toLocaleString();
        }
      },
      style: { fontSize: '12px' }
    },
    legend: {
      position: 'top',
      horizontalAlign: 'left',
      offsetX: 40
    },
    responsive: [{
      breakpoint: 768,
      options: {
        chart: { height: 300 },
        xaxis: {
          labels: { rotate: -90 }
        }
      }
    }]
  };

  return (
    <div className="w-full h-64">
      <Chart
        options={options}
        series={data.series}
        type="bar"
        height="100%"
      />
    </div>
  );
};