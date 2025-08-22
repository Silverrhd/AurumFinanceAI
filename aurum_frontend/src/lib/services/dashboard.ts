import { apiClient } from '../api';
import { 
  AdminMetrics, 
  ClientMetrics, 
  AssetAllocationData, 
  PortfolioHistoryData, 
  AllChartsResponse,
  ClientReportsResponse 
} from '@/types/dashboard';
import {
  FileUploadResponse,
  BankStatus,
  PreprocessingResponse,
  ReportGenerationProgress,
  ReportAnalysis,
  DatabaseUpdateResponse
} from '@/types/api';

export class DashboardService {
  // Admin dashboard endpoints
  static async getAdminMetrics(): Promise<AdminMetrics> {
    return await apiClient.get<AdminMetrics>('/admin/dashboard/metrics/');
  }

  static async getAdminAssetAllocation(): Promise<AssetAllocationData> {
    return await apiClient.get<AssetAllocationData>('/admin/dashboard/asset-allocation/');
  }

  static async getAdminPortfolioHistory(): Promise<PortfolioHistoryData> {
    return await apiClient.get<PortfolioHistoryData>('/admin/dashboard/portfolio-history/');
  }

  static async getAdminAllCharts(): Promise<AllChartsResponse> {
    return await apiClient.get<AllChartsResponse>('/admin/dashboard/all-charts/');
  }

  // Client dashboard endpoints  
  static async getClientMetrics(): Promise<ClientMetrics> {
    return await apiClient.get<ClientMetrics>('/client/dashboard/metrics/');
  }

  static async getClientAssetAllocation(): Promise<AssetAllocationData> {
    return await apiClient.get<AssetAllocationData>('/client/dashboard/asset-allocation/');
  }

  static async getClientPortfolioHistory(): Promise<PortfolioHistoryData> {
    return await apiClient.get<PortfolioHistoryData>('/client/dashboard/portfolio-history/');
  }

  static async getClientCumulativeReturn(): Promise<{ series: unknown[], categories: string[], colors: string[] }> {
    return await apiClient.get('/client/dashboard/cumulative-return/');
  }

  static async getClientAllCharts(): Promise<AllChartsResponse> {
    return await apiClient.get<AllChartsResponse>('/client/dashboard/all-charts/');
  }

  // Client reports endpoints
  static async getClientReports(): Promise<ClientReportsResponse> {
    return await apiClient.get<ClientReportsResponse>('/client/reports/');
  }

  static async downloadClientReport(date: string): Promise<Blob> {
    return await apiClient.downloadFile(`/client/reports/download/${date}/`);
  }

  static async getClientReportMetadata(date: string): Promise<{
    filename: string;
    report_date: string;
    client_code: string;
    file_size: number;
    created_at: string;
    download_url: string;
  }> {
    return await apiClient.get(`/client/reports/metadata/${date}/`);
  }
}

export class FileService {
  static async uploadFiles(files: File[], onProgress?: (progress: number) => void): Promise<FileUploadResponse> {
    const formData = new FormData();
    files.forEach(file => formData.append('files', file));

    return await apiClient.post('/admin/upload-files/', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: onProgress ? (progressEvent) => {
        const progress = progressEvent.total 
          ? Math.round((progressEvent.loaded * 100) / progressEvent.total)
          : 0;
        onProgress(progress);
      } : undefined,
    });
  }

  static async getBankStatus(): Promise<unknown> {
    return await apiClient.get('/admin/bank-status/');
  }

  static async runPreprocessing(date: string, forceReprocess = false): Promise<unknown> {
    return await apiClient.post('/admin/run-preprocessing/', { 
      date, 
      force_reprocess: forceReprocess 
    });
  }
}

export class ReportService {
  static async generateReports(currentDate: string, comparisonDate: string): Promise<unknown> {
    return await apiClient.post('/admin/generate-reports/', {
      current_date: currentDate,
      comparison_date: comparisonDate
    });
  }

  static async generateMissingReports(date: string): Promise<unknown> {
    return await apiClient.post('/admin/generate-missing-reports/', { date });
  }

  static async getGenerationProgress(): Promise<unknown> {
    return await apiClient.get('/admin/generation-progress/');
  }

  static async getBondIssuerProgress(): Promise<unknown> {
    return await apiClient.get('/admin/bond-issuer-progress/');
  }

  static async getBondMaturityProgress(): Promise<unknown> {
    return await apiClient.get('/admin/bond-maturity-progress/');
  }

  static async getEquityBreakdownProgress(): Promise<unknown> {
    return await apiClient.get('/admin/equity-breakdown-progress/');
  }

  static async getReportAnalysis(date: string): Promise<unknown> {
    return await apiClient.get(`/admin/report-analysis/${date}/`);
  }
}

export class DatabaseService {
  static async updateDatabase(securitiesFile: string, transactionsFile: string, date: string, clientCode?: string): Promise<unknown> {
    return await apiClient.post('/admin/update-database/', {
      securities_file: securitiesFile,
      transactions_file: transactionsFile,
      date,
      client_code: clientCode
    });
  }

  // Database Backup and Restore Methods
  static async createBackup(): Promise<{
    status: string;
    message: string;
    backup_info?: {
      filename: string;
      size_mb: number;
      created_at: string;
      display_name: string;
    };
  }> {
    return await apiClient.post('/portfolio/admin/create-database-backup/');
  }

  static async listBackups(): Promise<{
    status: string;
    backups: Array<{
      filename: string;
      display_name: string;
      size_mb: number;
      created_at: string;
      is_valid: boolean;
    }>;
    count: number;
  }> {
    return await apiClient.get('/portfolio/admin/list-database-backups/');
  }

  static async restoreBackup(backupFilename: string, createPreRestoreBackup: boolean = true): Promise<{
    status: string;
    message: string;
    restore_info?: {
      restored_from: string;
      pre_restore_backup?: string;
    };
  }> {
    return await apiClient.post('/portfolio/admin/restore-database-backup/', {
      backup_filename: backupFilename,
      create_pre_restore_backup: createPreRestoreBackup
    });
  }

  static async deleteBackup(backupFilename: string): Promise<{
    status: string;
    message: string;
    deletion_info?: {
      deleted_file: string;
      freed_space_mb: number;
    };
  }> {
    return await apiClient.delete(`/portfolio/admin/delete-database-backup/${backupFilename}/`);
  }
}