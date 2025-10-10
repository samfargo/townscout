// API URL resolution utility
export function resolveApiUrl(path: string): string {
  // In production/deployment, use relative paths
  // In development, could use env var for API base URL
  const base = process.env.NEXT_PUBLIC_API_URL || '';
  return `${base}${path}`;
}

export async function fetchApi<T>(path: string, options?: RequestInit): Promise<T> {
  const url = resolveApiUrl(path);
  const response = await fetch(url, options);
  
  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${response.statusText}`);
  }
  
  return response.json() as Promise<T>;
}

