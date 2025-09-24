'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { ApexOptions } from 'apexcharts';

// Dynamic import to avoid SSR issues
const Chart = dynamic(() => import('react-apexcharts'), { ssr: false });

interface BondMaturityData {
  hasData: boolean;
  data: number[];
  categories: string[];
}

interface DashboardBondMaturityChartProps {
  data: BondMaturityData;
}

export const DashboardBondMaturityChart: React.FC<DashboardBondMaturityChartProps> = ({ data }) => {
  if (!data.hasData || data.data.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-gray-500">
        No bond maturity data available
      </div>
    );
  }

  const options: ApexOptions = {
    series: [{
      name: 'Face Value',
      data: data.data
    }],
    chart: {
      type: 'area',
      height: 350,
      fontFamily: 'Arial, sans-serif',
      toolbar: { show: false },
      animations: { enabled: true, easing: 'easeinout', speed: 800 },
      background: 'transparent'
    },
    colors: ['#5f76a1'],
    fill: {
      type: 'gradient',
      gradient: {
        shade: 'light',
        type: 'vertical',
        shadeIntensity: 0.5,
        gradientToColors: ['#a1b8d4'],
        opacityFrom: 0.7,
        opacityTo: 0.1,
        stops: [0, 100]
      }
    },
    stroke: { curve: 'smooth', width: 3 },
    dataLabels: { enabled: false },
    grid: {
      borderColor: '#e0e0e0',
      strokeDashArray: 3,
      xaxis: { lines: { show: false } },
      yaxis: { lines: { show: true } },
      padding: { left: 20, right: 20 }
    },
    xaxis: {
      categories: data.categories,
      labels: { style: { colors: '#666', fontSize: '12px' } }
    },
    yaxis: {
      title: { text: 'Face Value ($)', style: { color: '#666', fontSize: '12px' } },
      labels: {
        style: { colors: '#666', fontSize: '12px' },
        formatter: function(value: number) {
          if (value >= 1000000) {
            return '$' + (value / 1000000).toFixed(1) + 'M';
          } else if (value >= 1000) {
            return '$' + (value / 1000).toFixed(0) + 'K';
          }
          return '$' + value.toFixed(0);
        }
      }
    },
    tooltip: {
      y: {
        formatter: function(value: number) {
          return '$' + value.toLocaleString();
        }
      }
    },
    responsive: [{
      breakpoint: 768,
      options: {
        chart: { height: 300 },
        yaxis: { title: { text: 'Face Value' } }
      }
    }]
  };

  return (
    <div className="w-full h-64">
      <Chart
        options={options}
        series={options.series}
        type="area"
        height="100%"
      />
    </div>
  );
};