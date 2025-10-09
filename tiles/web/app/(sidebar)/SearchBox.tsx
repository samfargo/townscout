'use client';
// Handles catalog picks through dropdowns and custom pins.

import React from 'react';
import { useQuery } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Separator } from '@/components/ui/separator';
import {
  addBrand,
  addCategory,
  addCustom,
  customCacheKey,
  ensureCatalogLoaded,
  normalizeMinutes
} from '@/lib/actions';
import { buildCategoryGroups } from '@/lib/services/catalog';
import { fetchPlaceDetails, fetchPlaceSuggestions, type PlaceSuggestion } from '@/lib/services/places';
import { useStore } from '@/lib/state/store';
import { debounce } from '@/lib/utils/debounce';

function createPlacesSession() {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
    return crypto.randomUUID();
  }
  return `session-${Date.now()}`;
}

export default function SearchBox() {
  const catalog = useStore((state) => state.catalog);
  const pois = useStore((state) => state.pois);

  const [placeInput, setPlaceInput] = React.useState('');
  const [placesQuery, setPlacesQuery] = React.useState('');
  const [pending, setPending] = React.useState<string | null>(null);
  const [session] = React.useState(() => createPlacesSession());
  const detailsCacheRef = React.useRef<Record<string, string>>({});

  const debouncedSetPlacesQuery = React.useMemo(() => debounce(setPlacesQuery, 200), []);

  React.useEffect(() => {
    debouncedSetPlacesQuery(placeInput.trim());
  }, [placeInput, debouncedSetPlacesQuery]);

  React.useEffect(() => () => debouncedSetPlacesQuery.cancel(), [debouncedSetPlacesQuery]);

  React.useEffect(() => {
    ensureCatalogLoaded().catch((err) => {
      console.error('Failed to load catalog', err);
    });
  }, []);

  const categoryGroups = React.useMemo(() => {
    if (!catalog.loaded) return [];
    return buildCategoryGroups({
      categories: catalog.categories,
      brands: catalog.brands,
      cat_to_brands: catalog.catToBrands
    });
  }, [catalog]);

  const precomputedBrands = React.useMemo(() => {
    if (!catalog.loaded) return [];
    const ids = new Set<string>();
    for (const brandIds of Object.values(catalog.catToBrands)) {
      for (const brandId of brandIds) {
        ids.add(String(brandId));
      }
    }
    return catalog.brands
      .filter((brand) => ids.has(brand.id))
      .slice()
      .sort((a, b) => a.label.localeCompare(b.label));
  }, [catalog.loaded, catalog.brands, catalog.catToBrands]);

  const placesResult = useQuery({
    queryKey: ['places-autocomplete', session, placesQuery],
    queryFn: async () => {
      if (!placesQuery) return { suggestions: [] as PlaceSuggestion[] };
      return fetchPlaceSuggestions({ query: placesQuery, session, limit: 6 });
    },
    enabled: placesQuery.length >= 2,
    keepPreviousData: true
  });

  const placeSuggestions = placesResult.data?.suggestions ?? [];

  const poisById = React.useMemo(() => new Set(pois.map((poi) => poi.id)), [pois]);
  const customLabels = React.useMemo(
    () =>
      new Set(
        pois
          .filter((poi) => poi.type === 'custom')
          .map((poi) => poi.label.trim().toLowerCase())
      ),
    [pois]
  );

  const isPoiActive = React.useCallback(
    (id: string | null | undefined, label?: string) => {
      if (id && poisById.has(id)) return true;
      if (label) {
        const normalized = label.trim().toLowerCase();
        if (normalized && customLabels.has(normalized)) return true;
      }
      return false;
    },
    [poisById, customLabels]
  );

  const handleAddCategory = async (group: (typeof categoryGroups)[number]) => {
    const id = group.ids.join(',');
    setPending(`category:${id}`);
    try {
      await addCategory(id, group.label, group.ids);
    } catch (err) {
      console.error('Failed to add category', err);
    } finally {
      setPending(null);
    }
  };

  const handleAddBrand = async (brand: { id: string; label: string }) => {
    setPending(`brand:${brand.id}`);
    try {
      await addBrand(brand.id, brand.label);
    } catch (err) {
      console.error('Failed to add brand', err);
    } finally {
      setPending(null);
    }
  };

  const handleAddCustom = async (suggestion: PlaceSuggestion) => {
    setPending(`custom:${suggestion.id}`);
    try {
      let lon = suggestion.lon;
      let lat = suggestion.lat;
      let label = suggestion.label;
      if (lon == null || lat == null) {
        const details = await fetchPlaceDetails(suggestion.id, session);
        lon = details.lon;
        lat = details.lat;
        label = details.label;
      }
      if (lon == null || lat == null) {
        throw new Error('Place has no coordinates');
      }
      const key = customCacheKey(lon, lat);
      detailsCacheRef.current[suggestion.id] = key;
      await addCustom(lon, lat, label, normalizeMinutes(30));
      setPlaceInput('');
      setPlacesQuery('');
    } catch (err) {
      console.error('Failed to add custom place', err);
    } finally {
      setPending(null);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Add filters</CardTitle>
        <p className="text-xs text-slate-500">
          Pick a category, a place of interest, or create a custom location.
        </p>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-2">
          <DropdownSection label="Categories">
            {!catalog.loaded && (
              <p className="px-1 text-xs text-slate-500">Loading catalog…</p>
            )}
            {catalog.loaded && categoryGroups.length === 0 && (
              <p className="px-1 text-xs text-red-600">Catalog unavailable right now.</p>
            )}
            {catalog.loaded && categoryGroups.length > 0 && (
              <div className="space-y-2">
                {categoryGroups.map((group) => {
                  const id = group.ids.join(',');
                  const active = isPoiActive(id);
                  const loading = pending === `category:${id}`;
                  return (
                    <CatalogRow key={id} title={group.label}>
                      <Button
                        size="sm"
                        variant={active ? 'secondary' : 'default'}
                        disabled={active || loading}
                        onClick={() => handleAddCategory(group)}
                      >
                        {active ? 'Added' : loading ? 'Adding…' : 'Add'}
                      </Button>
                    </CatalogRow>
                  );
                })}
              </div>
            )}
          </DropdownSection>
          <DropdownSection label="POI">
            {!catalog.loaded && (
              <p className="px-1 text-xs text-slate-500">Loading POIs…</p>
            )}
            {catalog.loaded && precomputedBrands.length === 0 && (
              <p className="px-1 text-xs text-slate-500">No precomputed POIs available.</p>
            )}
            {catalog.loaded && precomputedBrands.length > 0 && (
              <div className="space-y-2">
                {precomputedBrands.map((brand) => {
                  const active = isPoiActive(brand.id);
                  const loading = pending === `brand:${brand.id}`;
                  return (
                    <CatalogRow key={brand.id} title={brand.label}>
                      <Button
                        size="sm"
                        variant={active ? 'secondary' : 'outline'}
                        disabled={active || loading}
                        onClick={() => handleAddBrand(brand)}
                      >
                        {active ? 'Added' : loading ? 'Adding…' : 'Add'}
                      </Button>
                    </CatalogRow>
                  );
                })}
              </div>
            )}
          </DropdownSection>
        </div>
        <Separator />
        <div className="space-y-3">
          <LabelledField label="Custom location">
            <Input
              value={placeInput}
              onChange={(event) => setPlaceInput(event.target.value)}
              placeholder="Search for an address or place"
            />
          </LabelledField>
          {placesResult.isFetching && (
            <p className="text-xs text-slate-500">Searching…</p>
          )}
          {placesResult.error && (
            <p className="text-xs text-red-600">Unable to reach Places autocomplete right now.</p>
          )}
          {!placesResult.isFetching && placeSuggestions.length === 0 && placeInput.length >= 2 && (
            <p className="text-xs text-slate-500">No matches found.</p>
          )}
          <div className="space-y-2">
            {placeSuggestions.map((suggestion) => {
              const computedKey =
                suggestion.lon != null && suggestion.lat != null
                  ? customCacheKey(suggestion.lon, suggestion.lat)
                : detailsCacheRef.current[suggestion.id];
              const active = isPoiActive(computedKey, suggestion.label);
              const loading = pending === `custom:${suggestion.id}`;
              return (
                <div
                  key={suggestion.id}
                  className="flex items-center justify-between rounded-lg border border-slate-200 bg-white px-3 py-2"
                >
                  <div className="flex flex-col">
                    <span className="text-sm font-medium text-slate-800">{suggestion.label}</span>
                    {suggestion.sublabel && (
                      <span className="text-xs text-slate-500">{suggestion.sublabel}</span>
                    )}
                  </div>
                  <Button
                    size="sm"
                    variant={active ? 'secondary' : 'default'}
                    disabled={active || loading}
                    onClick={() => handleAddCustom(suggestion)}
                  >
                    {active ? 'Added' : loading ? 'Adding…' : 'Drop'}
                  </Button>
                </div>
              );
            })}
          </div>
          <p className="text-xs text-slate-400">
            Think: friend&apos;s house, work address etc.
          </p>
        </div>
      </CardContent>
    </Card>
  );
}

function LabelledField({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</span>
      {children}
    </label>
  );
}

function CatalogRow({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between rounded-lg border border-slate-200 bg-white px-3 py-2">
      <div className="flex flex-col">
        <span className="text-sm font-medium text-slate-800">{title}</span>
      </div>
      {children}
    </div>
  );
}

function DropdownSection({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <details className="rounded-xl border border-slate-200 bg-slate-50">
      <summary className="cursor-pointer select-none px-3 py-2 text-sm font-semibold text-slate-700">
        {label}
      </summary>
      <div className="space-y-2 border-t border-slate-200 bg-white px-3 py-3 max-h-60 overflow-y-auto">
        {children}
      </div>
    </details>
  );
}
