// ProjectAurum API Integration Layer

import { 
  Client,
  PortfolioSummary,
  DashboardChartData,
  BankStatus,
  UploadResult,
  AvailableReport,
  ProcessingStatus,
  AdminDashboardData,
  ClientDashboardData,
  ApiResponse
} from '@/types/portfolio';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

class PortfolioAPI {
  private async request<T>(
    endpoint: string, 
    options: RequestInit = {}
  ): Promise<ApiResponse<T>> {
    // Check if we're in browser environment
    const token = typeof window !== 'undefined' ? localStorage.getItem('access_token') : null;
    
    const config: RequestInit = {
      headers: {
        'Content-Type': 'application/json',
        ...(token && { 'Authorization': `Bearer ${token}` }),
        ...options.headers,
      },
      ...options,
    };

    try {
      console.log(`API Request: ${endpoint}`, { hasToken: !!token, headers: config.headers });
      const response = await fetch(`${API_BASE_URL}${endpoint}`, config);
      const data = await response.json();

      console.log(`API Response: ${endpoint}`, { status: response.status, data });

      if (!response.ok) {
        return {
          status: 'error',
          error: data.message || data.error || `HTTP ${response.status}: ${response.statusText}`,
        };
      }

      return {
        status: 'success',
        data,
      };
    } catch (error) {
      console.error(`API Error: ${endpoint}`, error);
      return {
        status: 'error',
        error: error instanceof Error ? error.message : 'Network error',
      };
    }
  }

  // Dashboard Data
  async getAdminDashboardData(clientCode?: string): Promise<ApiResponse<AdminDashboardData>> {
    const endpoint = clientCode 
      ? `/api/portfolio/admin/dashboard/?client_code=${clientCode}&t=${Date.now()}`
      : `/api/portfolio/admin/dashboard/?t=${Date.now()}`;
    return this.request<AdminDashboardData>(endpoint);
  }

  // Bond Issuer Weight Report Methods
  async generateBondIssuerWeightReports(): Promise<ApiResponse<any>> {
    return this.request('/api/portfolio/generate-report-no-open/', {
      method: 'POST',
      body: JSON.stringify({
        report_type: 'bond_issuer_weight',
        client_code: 'ALL'
      })
    });
  }

  async listBondIssuerWeightReports(): Promise<ApiResponse<any>> {
    return this.request('/api/portfolio/reports/bond_issuer_weight/generated/');
  }

  async getBondIssuerWeightReport(clientCode: string): Promise<Response> {
    // Get the latest report for this client
    const reportsResponse = await this.listBondIssuerWeightReports();
    if (reportsResponse.status === 'success' && reportsResponse.data.reports) {
      const clientReport = reportsResponse.data.reports.find(
        (report: any) => report.client_code === clientCode
      );
      
      if (clientReport) {
        const token = typeof window !== 'undefined' ? localStorage.getItem('access_token') : null;
        const response = await fetch(
          `${API_BASE_URL}/api/portfolio/reports/${clientReport.id}/view/`,
          {
            headers: {
              'Content-Type': 'application/json',
              ...(token && { 'Authorization': `Bearer ${token}` }),
            }
          }
        );
        return response;
      }
    }
    
    throw new Error(`No bond issuer weight report found for client ${clientCode}`);
  }

  async getClientDashboardData(clientCode?: string): Promise<ApiResponse<ClientDashboardData>> {
    const endpoint = clientCode 
      ? `/api/portfolio/client/dashboard/${clientCode}/`
      : '/api/portfolio/client/dashboard/';
    return this.request<ClientDashboardData>(endpoint);
  }

  async getClientDashboardWithCharts(): Promise<ApiResponse<AdminDashboardData>> {
    return this.request<AdminDashboardData>('/api/portfolio/client/dashboard-with-charts/');
  }

  // Portfolio Data
  async getPortfolioSummary(clientCode?: string): Promise<ApiResponse<PortfolioSummary>> {
    const endpoint = clientCode 
      ? `/api/portfolio/summary/${clientCode}/`
      : '/api/portfolio/summary/';
    return this.request<PortfolioSummary>(endpoint);
  }

  async getChartData(clientCode?: string): Promise<ApiResponse<DashboardChartData>> {
    const endpoint = clientCode
      ? `/api/portfolio/charts/${clientCode}/`
      : '/api/portfolio/charts/';
    return this.request<DashboardChartData>(endpoint);
  }

  // Client Management
  async getClients(): Promise<ApiResponse<Client[]>> {
    return this.request<Client[]>('/api/portfolio/clients/');
  }

  async getAvailableSnapshots(clientCode?: string): Promise<ApiResponse<any[]>> {
    const response = await this.request<{
      success: boolean, 
      dates: any[],
      summary: any
    }>('/api/portfolio/available-dates/');
    
    if (response.status === 'success' && response.data?.success) {
      return {
        status: 'success',
        data: response.data.dates
      };
    }
    
    return {
      status: 'error',
      error: 'Failed to load available dates'
    };
  }

  // File Upload
  async uploadFiles(files: File[]): Promise<ApiResponse<UploadResult[]>> {
    const formData = new FormData();
    files.forEach((file, index) => {
      formData.append('files', file);
    });

    const token = localStorage.getItem('access_token');
    
    try {
      const response = await fetch(`${API_BASE_URL}/api/portfolio/files/upload/`, {
        method: 'POST',
        headers: {
          ...(token && { 'Authorization': `Bearer ${token}` }),
        },
        body: formData,
      });

      const data = await response.json();

      if (!response.ok) {
        return {
          status: 'error',
          error: data.message || 'Upload failed',
        };
      }

      return {
        status: 'success',
        data: data.results,
      };
    } catch (error) {
      return {
        status: 'error',
        error: error instanceof Error ? error.message : 'Upload error',
      };
    }
  }

  // Enhanced Bank Status
  async getBankStatus(): Promise<ApiResponse<any>> {
    const response = await this.request<any>('/api/portfolio/bank-status/');
    
    // Case 1: wrapper returns nested { data: { success, banks, summary, current_date } }
    if (response.status === 'success' && response.data && typeof response.data === 'object') {
      const d: any = response.data;
      if (d.success === true && Array.isArray(d.banks)) {
        return {
          status: 'success',
          data: {
            banks: d.banks,
            summary: d.summary,
            current_date: d.current_date,
            status_date: d?.summary?.status_date || d.current_date
          }
        };
      }
      // Case 2: backend returned the final object directly (no wrapper)
      if (Array.isArray(d.banks)) {
        return {
          status: 'success',
          data: {
            banks: d.banks,
            summary: d.summary,
            current_date: d.current_date,
            status_date: d?.summary?.status_date || d.current_date
          }
        };
      }
    }
    
    // Case 3: legacy array
    if (response.status === 'success' && Array.isArray(response.data)) {
      return {
        status: 'success',
        data: response.data
      };
    }
    
    return {
      status: 'error',
      error: 'Unexpected response format from bank status API'
    };
  }



  // Processing
  async startPreprocessing(date: string): Promise<ApiResponse<{ message: string }>> {
    return this.request('/api/portfolio/preprocess/start/', {
      method: 'POST',
      body: JSON.stringify({ date }),
    });
  }

  async getProcessingStatus(): Promise<ApiResponse<ProcessingStatus>> {
    return this.request<ProcessingStatus>('/api/portfolio/preprocess/status/');
  }

  // Population Ready Dates
  async getPopulationReadyDates(): Promise<ApiResponse<{ready_dates: string[], message: string}>> {
    return this.request<{ready_dates: string[], message: string}>('/api/portfolio/population-ready-dates/');
  }

  async updateDatabase(fileDate: string): Promise<ApiResponse<{ message: string }>> {
    // Convert DD_MM_YYYY to YYYY-MM-DD for backend
    const [day, month, year] = fileDate.split('_');
    const snapshotDate = `${year}-${month}-${day}`;
    
    return this.request('/api/portfolio/update-database/', {
      method: 'POST',
      body: JSON.stringify({ 
        snapshot_date: snapshotDate,
        securities_file: `data/excel/securities_${fileDate}.xlsx`,
        transactions_file: `data/excel/transactions_${fileDate}.xlsx`
      }),
    });
  }

  // Report Generation
  async generateReport(params: {
    report_type: string;
    client_code?: string;
    current_date?: string;
    comparison_date?: string;
  }): Promise<ApiResponse<{ 
    html_content: string;
    report_id: number;
    file_path: string;
    message: string;
  }>> {
    return this.request('/api/portfolio/generate-report/', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  }

  // New API method for generating without auto-opening
  async generateReportNoOpen(params: {
    report_type: string;
    client_code?: string;
    current_date?: string;
    comparison_date?: string;
  }): Promise<ApiResponse<{
    report_id: number;
    file_path: string;
    message: string;
    results?: Array<{client: string, status: string}>; // For bulk generation
  }>> {
    return this.request('/api/portfolio/generate-report-no-open/', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  }

  // Updated method for getting dates per report type
  async getAvailableDatesByType(
    reportType: string,
    clientCode?: string
  ): Promise<ApiResponse<{
    available_dates: string[];
    client_code?: string;
    report_type: string;
  }>> {
    const queryParams = clientCode ? `?client_code=${clientCode}` : '';
    return this.request(`/api/portfolio/available-dates/${reportType}/${queryParams}`);
  }

  // Updated method for getting reports per report type
  async getGeneratedReportsByType(
    reportType: string,
    clientCode?: string
  ): Promise<ApiResponse<{
    reports: Array<{
      id: number;
      client_code: string;
      client_name: string;
      report_date: string;
      file_path: string;
      file_size: number;
      generation_time: number;
      created_at: string;
    }>;
    count: number;
    report_type: string;
    client_code?: string;
  }>> {
    const queryParams = clientCode ? `?client_code=${clientCode}` : '';
    return this.request(`/api/portfolio/reports/${reportType}/generated/${queryParams}`);
  }

  async getAvailableReports(
    clientCode?: string
  ): Promise<ApiResponse<AvailableReport[]>> {
    const endpoint = clientCode
      ? `/api/portfolio/available-reports/?client=${clientCode}`
      : '/api/portfolio/available-reports/';
    return this.request<AvailableReport[]>(endpoint);
  }

  // Weekly Report Specific APIs
  async getAvailableWeeklyReportDates(
    clientCode?: string
  ): Promise<ApiResponse<{ 
    available_dates: string[];
    total_snapshots: number;
    total_available: number;
  }>> {
    const queryParams = clientCode ? `?client_code=${clientCode}` : '';
    return this.request(`/api/portfolio/weekly-reports/available-dates/${queryParams}`);
  }

  async getGeneratedWeeklyReports(
    clientCode?: string
  ): Promise<ApiResponse<{
    reports: Array<{
      id: number;
      client_code: string;
      client_name: string;
      report_date: string;
      file_path: string;
      file_size: number;
      generation_time: number;
      created_at: string;
    }>;
    total_count: number;
  }>> {
    const queryParams = clientCode ? `?client_code=${clientCode}` : '';
    return this.request(`/api/portfolio/weekly-reports/generated/${queryParams}`);
  }

  async getReportFile(reportId: number): Promise<ApiResponse<{
    html_content: string;
    report_id: number;
    client_code: string;
    client_name: string;
    report_date: string;
    report_type: string;
    file_path: string;
  }>> {
    return this.request(`/api/portfolio/reports/${reportId}/view/`);
  }

  async getReportDates(
    reportType: string,
    clientCode?: string
  ): Promise<ApiResponse<string[]>> {
    const endpoint = `/api/portfolio/report-dates/${reportType}/` + 
      (clientCode ? `?client=${clientCode}` : '');
    return this.request<string[]>(endpoint);
  }

  async getReportGenerationProgress(): Promise<ApiResponse<any>> {
    return this.request('/api/portfolio/report-progress/');
  }

  // File promotion for processing
  async promoteFilesForProcessing(date: string): Promise<ApiResponse<any>> {
    const response = await this.request<any>('/api/portfolio/promote-files/', {
      method: 'POST',
      body: JSON.stringify({ date })
    });
    
    return response;
  }

  // Report URLs
  getReportUrl(reportType: string, clientCode: string, date?: string): string {
    const params = new URLSearchParams({
      type: reportType,
      client: clientCode,
      ...(date && { date }),
    });
    return `${API_BASE_URL}/api/portfolio/report/?${params}`;
  }

  // Excel Export Methods
  async exportPositionsExcel(params: {
    client_code: string;
    snapshot_date: string;
  }): Promise<Response> {
    const token = typeof window !== 'undefined' ? localStorage.getItem('access_token') : null;
    
    return fetch(`${API_BASE_URL}/api/portfolio/export/positions/`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token && { 'Authorization': `Bearer ${token}` }),
      },
      body: JSON.stringify(params),
    });
  }

  async exportTransactionsExcel(params: {
    client_code: string;
    start_date: string;
    end_date: string;
  }): Promise<Response> {
    const token = typeof window !== 'undefined' ? localStorage.getItem('access_token') : null;
    
    return fetch(`${API_BASE_URL}/api/portfolio/export/transactions/`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token && { 'Authorization': `Bearer ${token}` }),
      },
      body: JSON.stringify(params),
    });
  }

  async getExportAvailableDates(): Promise<ApiResponse<{
    snapshot_dates: string[];
    clients: Client[];
  }>> {
    return this.request('/api/portfolio/export/available-dates/');
  }
}

export const portfolioAPI = new PortfolioAPI();