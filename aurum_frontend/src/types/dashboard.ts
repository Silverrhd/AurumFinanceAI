export interface AdminMetrics {
  total_clients: number;
  total_assets: number;
  total_positions: number;
  total_market_value: number;
  monthly_change: number;
  ytd_return: number;
  last_updated: string;
  active_clients: number;
}

export interface ClientMetrics {
  client_code: string;
  total_assets: number;
  total_market_value: number;
  monthly_change: number;
  ytd_return: number;
  last_updated: string;
  portfolio_allocation: Record<string, number>;
}

export interface AssetAllocationData {
  chart_type: string;
  series: number[];
  labels: string[];
  colors: string[];
  total_value: number;
}

export interface PortfolioHistoryData {
  chart_type: string;
  series: Array<{
    name: string;
    data: number[];
  }>;
  categories: string[];
  colors: string[];
}

export interface AllChartsResponse {
  asset_allocation: AssetAllocationData;
  portfolio_history: PortfolioHistoryData;
  portfolio_comparison: {
    series: Array<{
      name: string;
      data: number[];
    }>;
    categories: string[];
    colors: string[];
  };
  cumulative_return: {
    series: Array<{
      name: string;
      data: number[];
    }>;
    categories: string[];
    colors: string[];
  };
  last_updated: string;
}

export interface ReportListItem {
  filename: string;
  report_date: string;
  formatted_date: string;
  file_size: number;
  created_at: string;
  download_url: string;
}

export interface ClientReportsResponse {
  reports: ReportListItem[];
  client_code: string;
  total_reports: number;
}