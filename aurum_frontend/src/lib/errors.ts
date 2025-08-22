export class ApiError extends Error {
  constructor(
    public status: number,
    public message: string,
    public data?: unknown
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export function handleApiError(error: unknown): ApiError {
  if (typeof error === 'object' && error !== null && 'response' in error) {
    const apiError = error as { response: { status: number; data?: { message?: string; error?: string } } };
    const status = apiError.response.status;
    const message = apiError.response.data?.message || apiError.response.data?.error || 'API Error';
    return new ApiError(status, message, apiError.response.data);
  }
  
  if (typeof error === 'object' && error !== null && 'request' in error) {
    return new ApiError(0, 'Network Error - Unable to connect to server');
  }
  
  if (error instanceof Error) {
    return new ApiError(0, error.message);
  }
  
  return new ApiError(0, 'Unknown Error');
}

export function isApiError(error: unknown): error is ApiError {
  return error instanceof ApiError;
}