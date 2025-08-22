'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { ApexOptions } from 'apexcharts';

// Dynamic import to avoid SSR issues
const Chart = dynamic(() => import('react-apexcharts'), { ssr: false });

interface AssetAllocationData {
  hasData: boolean;
  series: number[];
  labels: string[];
  monetaryValues: number[];
  colors: string[];
}

interface DashboardAssetAllocationChartProps {
  data: AssetAllocationData;
}

export const DashboardAssetAllocationChart: React.FC<DashboardAssetAllocationChartProps> = ({ data }) => {
  if (!data.hasData || data.series.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-gray-500">
        No asset allocation data available
      </div>
    );
  }

  const options: ApexOptions = {
    chart: {
      type: 'polarArea',
      toolbar: { show: false },
      animations: { 
        enabled: true,
        easing: 'easeinout',
        speed: 800
      },
      background: 'transparent',
      fontFamily: 'Arial, sans-serif'
    },
    labels: data.labels,
    colors: data.colors,
    stroke: {
      colors: ['#fff']
    },
    fill: {
      opacity: 0.8
    },
    legend: {
      position: 'bottom',
      fontSize: '13px',
      fontWeight: '500',
      labels: {
        colors: '#333'
      }
    },
    tooltip: {
      y: {
        formatter: function(value: number, opts: any) {
          const monetaryValue = data.monetaryValues[opts.seriesIndex];
          return `${value.toFixed(1)}% ($${monetaryValue.toLocaleString()})`;
        }
      }
    },
    responsive: [{
      breakpoint: 480,
      options: {
        chart: {
          width: 200
        },
        legend: {
          position: 'bottom'
        }
      }
    }]
  };

  return (
    <div className="w-full h-64">
      <Chart
        options={options}
        series={data.series}
        type="polarArea"
        height="100%"
      />
    </div>
  );
};