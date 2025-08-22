// Generic API response types
export interface FileUploadResponse {
  success: boolean;
  files_processed: number;
  results: Array<{
    filename: string;
    bank_detected?: string;
    file_size: number;
    status: 'success' | 'error' | 'warning';
    message: string;
    file_path?: string;
  }>;
  total_size: number;
  processing_time: number;
  errors?: string[];
}

export interface BankStatus {
  bank_code: string;
  bank_name: string;
  files_count: number;
  last_processed?: string;
  status: 'ready' | 'processing' | 'completed' | 'error' | 'no_files';
  next_action?: string;
  file_types: string[];
}

export interface PreprocessingResponse {
  success: boolean;
  message: string;
  processing_time: number;
  files_generated: string[];
  banks_processed: string[];
  errors?: string[];
}

export interface ReportGenerationProgress {
  status: 'idle' | 'started' | 'processing' | 'completed' | 'error';
  current_client?: string;
  completed_count: number;
  total_count: number;
  percentage: number;
  message: string;
  failed_clients: string[];
  last_error?: string;
  generated_reports?: string[];
}

export interface ReportAnalysis {
  date: string;
  formatted_date: string;
  total_clients: number;
  clients_with_reports: number;
  missing_clients: string[];
  existing_reports: string[];
  securities_file?: string;
  transactions_file?: string;
  files_exist: boolean;
}

export interface DatabaseUpdateResponse {
  success: boolean;
  message: string;
  processing_time: number;
  snapshot_date?: string;
  client_code?: string;
  error?: string;
}