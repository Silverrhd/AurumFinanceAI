// ProjectAurum Portfolio Types

export interface Client {
  id: string;
  name: string;
  client_code: string;
  email?: string;
}

export interface PortfolioSummary {
  total_aum: number;
  inception_dollar_performance: number;
  inception_return_pct: number;
  estimated_annual_income: number;
  client_count?: number;
  filter_applied?: string;
}

export interface AssetAllocation {
  asset_class: string;
  value: number;
  percentage: number;
  count: number;
}

export interface PortfolioMetric {
  client_code?: string;
  metric_name: string;
  current_value: number;
  previous_value: number;
  change_pct: number;
  period: string;
}

export interface PortfolioValue {
  date: string;
  value: number;
  client_code?: string;
}

export interface CumulativeReturn {
  date: string;
  return_pct: number;
  return_dollar: number;
  client_code?: string;
}

export interface BankStatus {
  bank_code: string;
  bank_name: string;
  status: 'COMPLETE' | 'PARTIAL' | 'EMPTY';
  percentage: number;
  file_count: number;
  processing_type: 'simple' | 'enrichment' | 'combination' | 'enrichment_combination';
  last_upload?: string;
  next_steps?: string;
}

export interface UploadResult {
  filename: string;
  bank_detected: string;
  status: 'success' | 'error';
  message: string;
  file_size: number;
}

export interface ReportGenerationProgress {
  stage: string;
  progress: number;
  current_client?: string;
  total_clients?: number;
  message: string;
}

export interface AvailableReport {
  report_type: 'investment' | 'bond_issuer' | 'bond_maturity' | 'equity_breakdown';
  client_code?: string;
  date: string;
  url: string;
  generated_at: string;
}

export interface ProcessingStatus {
  status: 'idle' | 'preprocessing' | 'populating' | 'generating' | 'complete' | 'error';
  stage?: string;
  progress?: number;
  message?: string;
  error?: string;
}

// Dashboard Chart Data
export interface DashboardChartData {
  asset_allocation: AssetAllocation[];
  portfolio_metrics: PortfolioMetric[];
  portfolio_values: PortfolioValue[];
  cumulative_returns: CumulativeReturn[];
}

// API Response Types
export interface ApiResponse<T> {
  status: 'success' | 'error';
  data?: T;
  message?: string;
  error?: string;
}

export interface ClientDashboardData {
  summary: PortfolioSummary;
  charts: DashboardChartData;
  available_reports: AvailableReport[];
}

export interface AdminDashboardData {
  summary: PortfolioSummary;
  charts: DashboardChartData;
  clients_data?: any[];
  last_updated: string;
}