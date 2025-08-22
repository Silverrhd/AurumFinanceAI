'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { ApexOptions } from 'apexcharts';

const Chart = dynamic(() => import('react-apexcharts'), { ssr: false });

interface PortfolioValueData {
  hasData: boolean;
  message: string;
  series: Array<{
    name: string;
    data: Array<{ x: number; y: number }>;
  }>;
  yAxisMin: number;
  yAxisMax: number;
  colors: string[];
  gradient: { to: string };
}

interface DashboardPortfolioValueChartProps {
  data: PortfolioValueData;
}

export const DashboardPortfolioValueChart: React.FC<DashboardPortfolioValueChartProps> = ({ data }) => {
  if (!data.hasData || data.series.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-gray-500">
        {data.message || 'No portfolio evolution data available'}
      </div>
    );
  }

  const options: ApexOptions = {
    chart: {
      height: 400,
      type: 'area',
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
    fill: {
      type: 'gradient',
      gradient: {
        shade: 'light',
        type: 'vertical',
        shadeIntensity: 0.5,
        gradientToColors: [data.gradient.to],
        opacityFrom: 0.7,
        opacityTo: 0.1,
        stops: [0, 100]
      }
    },
    stroke: {
      curve: 'smooth',
      width: 3
    },
    dataLabels: { enabled: false },
    grid: {
      borderColor: '#e0e0e0',
      strokeDashArray: 3,
      xaxis: { lines: { show: false } },
      yaxis: { lines: { show: true } },
      padding: { left: 20, right: 20 }
    },
    xaxis: {
      type: 'datetime',
      labels: { 
        format: 'MMM dd',
        style: { 
          colors: '#666',
          fontSize: '12px'
        },
        rotate: -45
      },
      axisBorder: { show: false },
      axisTicks: { show: false }
    },
    yaxis: {
      title: { 
        text: 'Portfolio Value ($)',
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
    tooltip: {
      theme: 'light',
      x: { format: 'MMM dd, yyyy' },
      y: { 
        formatter: function(val: number) { 
          return '$' + val.toLocaleString(); 
        } 
      },
      marker: { show: true },
      style: { fontSize: '12px' }
    },
    markers: {
      size: 0,
      hover: { 
        size: 6,
        sizeOffset: 3 
      }
    },
    responsive: [{
      breakpoint: 768,
      options: {
        chart: { height: 300 },
        xaxis: {
          labels: { rotate: 0 }
        }
      }
    }]
  };

  return (
    <div className="w-full h-64">
      <Chart
        options={options}
        series={data.series}
        type="area"
        height="100%"
      />
    </div>
  );
};