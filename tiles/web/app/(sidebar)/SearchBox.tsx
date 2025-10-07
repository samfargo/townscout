'use client';

import React from 'react';
import { useQuery } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Separator } from '@/components/ui/separator';
import { Badge } from '@/components/ui/badge';
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

  const [searchTerm, setSearchTerm] = React.useState('');
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

  const normalizedTerm = searchTerm.trim().toLowerCase();

  const matchingCategories = React.useMemo(() => {
    if (!normalizedTerm) return categoryGroups.slice(0, 6);
    return categoryGroups
      .filter((group) => group.label.toLowerCase().includes(normalizedTerm))
      .slice(0, 6);
  }, [categoryGroups, normalizedTerm]);

  const matchingBrands = React.useMemo(() => {
    const brands = catalog.brands;
    if (!normalizedTerm) return brands.slice(0, 8);
    return brands
      .filter((brand) => brand.label.toLowerCase().includes(normalizedTerm))
      .slice(0, 8);
  }, [catalog.brands, normalizedTerm]);

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
        <div>
          <CardTitle>Add coverage filters</CardTitle>
          <p className="mt-1 text-xs text-slate-500">
            Stack multiple place types or drop a custom pin to filter reachable hexes.
          </p>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-3">
          <LabelledField label="Catalog search">
            <Input
              value={searchTerm}
              onChange={(event) => setSearchTerm(event.target.value)}
              placeholder="Search for restaurants, grocery, brands…"
            />
          </LabelledField>
          {!catalog.loaded && (
            <p className="text-xs text-slate-500">Loading catalog…</p>
          )}
          <div className="space-y-2">
            {catalog.loaded && matchingCategories.length === 0 && (
              <p className="text-sm text-slate-500">No categories match “{searchTerm}”.</p>
            )}
            {matchingCategories.map((group) => {
              const id = group.ids.join(',');
              const active = isPoiActive(id);
              const loading = pending === `category:${id}`;
              return (
                <CatalogRow
                  key={id}
                  title={group.label}
                  subtitle={`${group.ids.length} category${group.ids.length > 1 ? ' types' : ''}`}
                >
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
        </div>
        <Separator />
        <div className="space-y-2">
          <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Brands
          </h4>
          {matchingBrands.length === 0 && (
            <p className="text-sm text-slate-500">No brands match “{searchTerm}”.</p>
          )}
          {matchingBrands.map((brand) => {
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
            Custom pins fetch up to {normalizeMinutes(30)} minutes of coverage. Increase the slider
            to expand the radius.
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

function CatalogRow({
  title,
  subtitle,
  children
}: {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex items-center justify-between rounded-xl border border-slate-200 bg-white px-3 py-2">
      <div className="flex flex-col">
        <span className="text-sm font-medium text-slate-800">{title}</span>
        {subtitle && <Badge variant="muted">{subtitle}</Badge>}
      </div>
      {children}
    </div>
  );
}
