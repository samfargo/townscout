// Places API service for geocoding and place suggestions
import { fetchApi } from './api';

export interface PlaceSuggestion {
  id: string;
  label: string;
  sublabel?: string;
  description?: string;
  lat?: number;
  lon?: number;
  structured_formatting?: {
    main_text: string;
    secondary_text: string;
  };
}

export interface PlaceDetails {
  id: string;
  label: string;
  lat: number;
  lon: number;
  formatted_address?: string;
}

export async function fetchPlaceSuggestions(options: {
  query: string;
  session?: string;
  limit?: number;
}): Promise<{ suggestions: PlaceSuggestion[] }> {
  if (!options.query || options.query.length < 2) {
    return { suggestions: [] };
  }
  
  try {
    const params = new URLSearchParams({ input: options.query });
    if (options.session) {
      params.append('session', options.session);
    }
    if (options.limit) {
      params.append('limit', String(options.limit));
    }
    
    return await fetchApi<{ suggestions: PlaceSuggestion[] }>(
      `/api/places/autocomplete?${params}`
    );
  } catch (error) {
    console.error('Failed to fetch place suggestions:', error);
    return { suggestions: [] };
  }
}

export async function fetchPlaceDetails(
  placeId: string,
  sessionToken?: string
): Promise<PlaceDetails | null> {
  try {
    const params = new URLSearchParams({ place_id: placeId });
    if (sessionToken) {
      params.append('session', sessionToken);
    }
    
    return await fetchApi<PlaceDetails>(`/api/places/details?${params}`);
  } catch (error) {
    console.error('Failed to fetch place details:', error);
    return null;
  }
}

