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
  clearClimateSelections,
  customCacheKey,
  ensureCatalogLoaded,
  normalizeMinutes,
  setClimateSelections,
  toggleClimateSelection
} from '@/lib/actions';
import { CLIMATE_TYPOLOGY } from '@/lib/data/climate';
import { buildCategoryGroups, fetchPlaceDetails, fetchPlaceSuggestions, type PlaceSuggestion } from '@/lib/services';
import { useStore } from '@/lib/state/store';
import { debounce } from '@/lib/utils';

function createPlacesSession() {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
    return crypto.randomUUID();
  }
  return `session-${Date.now()}`;
}

const brassPrimaryButtonClass =
  'border border-amber-900 bg-amber-800 text-amber-50 shadow-sm transition-transform hover:-translate-y-0.5 hover:bg-amber-900 focus-visible:ring-amber-700';
const brassActiveButtonClass =
  'border border-stone-400 bg-stone-200 text-stone-800 focus-visible:ring-amber-700';

export default function SearchBox() {
  const catalog = useStore((state) => state.catalog);
  const pois = useStore((state) => state.pois);
  const climateSelections = useStore((state) => state.climateSelections);

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
    if (!catalog?.loaded) return [];
    return buildCategoryGroups({
      categories: catalog.categories,
      brands: catalog.brands,
      catToBrands: catalog.catToBrands
    });
  }, [catalog]);

  const precomputedBrands = React.useMemo(() => {
    if (!catalog?.loaded) return [];
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
  }, [catalog]);

  const placesResult = useQuery({
    queryKey: ['places-autocomplete', session, placesQuery],
    queryFn: async () => {
      if (!placesQuery) return { suggestions: [] as PlaceSuggestion[] };
      return fetchPlaceSuggestions({ query: placesQuery, session, limit: 6 });
    },
    enabled: placesQuery.length >= 2,
    placeholderData: (prev) => prev
  });

  const placeSuggestions = placesResult.data?.suggestions ?? [];

  const poisById = React.useMemo(() => {
    const ids = new Set<string>();
    for (const poi of pois) {
      ids.add(poi.id);
      if (poi.type === 'custom' && poi.lon != null && poi.lat != null) {
        ids.add(customCacheKey(poi.lon, poi.lat));
      }
    }
    return ids;
  }, [pois]);
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
    const id = group.id;
    setPending(`category:${id}`);
    try {
      await addCategory(id, group.label, group.brandIds);
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
    <Card className="border-stone-300 bg-[#fbf7ec] p-0 shadow-[0_20px_36px_-30px_rgba(76,54,33,0.28)]">
      <CardHeader className="mb-0 flex flex-col gap-2 rounded-2xl rounded-b-none border-b border-stone-200 bg-[#f2ebd9] p-5">
        <CardTitle className="font-serif text-sm font-semibold text-stone-900">Add filters</CardTitle>
        <p className="text-xs text-stone-600">
          Build your own livable area using the menus below.
        </p>
      </CardHeader>
      <CardContent className="space-y-4 px-5 pb-5 pt-4 text-sm text-stone-700">
        <div className="space-y-2">
          <DropdownSection label="Points of Interest">
            {!catalog?.loaded && (
              <p className="px-1 text-xs text-stone-500">Loading catalog…</p>
            )}
            {catalog?.loaded && categoryGroups.length === 0 && (
              <p className="px-1 text-xs text-red-600">Catalog unavailable right now.</p>
            )}
            {catalog?.loaded && categoryGroups.length > 0 && (
              <div className="space-y-2">
                {categoryGroups.map((group) => {
                  const id = group.id;
                  const active = isPoiActive(id);
                  const loading = pending === `category:${id}`;
                  return (
                    <CatalogRow key={id} title={group.label}>
                      <Button
                        size="sm"
                        variant="outline"
                        className={active ? brassActiveButtonClass : brassPrimaryButtonClass}
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
          <DropdownSection label="Popular Businesses">
            {!catalog?.loaded && (
              <p className="px-1 text-xs text-stone-500">Loading POIs…</p>
            )}
            {catalog?.loaded && precomputedBrands.length === 0 && (
              <p className="px-1 text-xs text-stone-500">No businesses available.</p>
            )}
            {catalog?.loaded && precomputedBrands.length > 0 && (
              <div className="space-y-2">
                {precomputedBrands.map((brand) => {
                  const active = isPoiActive(brand.id);
                  const loading = pending === `brand:${brand.id}`;
                  return (
                    <CatalogRow key={brand.id} title={brand.label}>
                      <Button
                        size="sm"
                        variant="outline"
                        className={
                          active
                            ? brassActiveButtonClass
                            : 'border border-stone-300 bg-[#faf4e5] text-stone-800 shadow-sm transition-transform hover:-translate-y-0.5 hover:bg-[#f6eedb] focus-visible:ring-amber-700'
                        }
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
          <DropdownSection label="Climate">
            <div className="space-y-2">
              {CLIMATE_TYPOLOGY.map((entry) => {
                const active = climateSelections.includes(entry.label);
                const exclusive = active && climateSelections.length === 1;
                return (
                  <div
                    key={entry.label}
                    className="rounded-xl border border-stone-300 bg-[#f7f0de] px-3 py-2 shadow-sm"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="flex-1 space-y-1">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-semibold text-stone-900">{entry.label}</span>
                          {active && (
                            <span className="rounded-full border border-amber-900 bg-amber-800 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-amber-50">
                              Selected
                            </span>
                          )}
                        </div>
                        <p className="text-xs text-stone-600">{entry.meaning}</p>
                        <p className="text-[11px] uppercase tracking-wide text-stone-500">
                          {entry.criteria}
                        </p>
                      </div>
                      <div className="flex flex-col items-end gap-2">
                        <Button
                          size="sm"
                          variant="outline"
                          className={
                            active
                              ? brassActiveButtonClass
                              : 'border border-stone-300 bg-[#faf4e5] text-stone-800 shadow-sm transition-transform hover:-translate-y-0.5 hover:bg-[#f6eedb] focus-visible:ring-amber-700'
                          }
                          onClick={() => toggleClimateSelection(entry.label)}
                        >
                          {active ? 'Remove' : 'Add'}
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          className="border border-stone-300 bg-white text-stone-700 transition-transform hover:-translate-y-0.5 hover:border-amber-900 hover:text-amber-900 focus-visible:ring-amber-700"
                          disabled={exclusive}
                          onClick={() => setClimateSelections([entry.label])}
                        >
                          Only this
                        </Button>
                      </div>
                    </div>
                    <p className="mt-2 text-[11px] text-stone-500">Examples: {entry.examples}</p>
                  </div>
                );
              })}
            </div>
            <div className="mt-3 flex items-center justify-between rounded-xl border border-stone-200 bg-[#fbf7ec] px-3 py-2 text-[11px] text-stone-600">
              <span>
                {climateSelections.length
                  ? `${climateSelections.length} climate ${climateSelections.length === 1 ? 'type' : 'types'} selected`
                  : 'No climate filter applied'}
              </span>
              <Button
                size="sm"
                variant="ghost"
                className="text-stone-600 transition-colors hover:text-amber-900"
                disabled={!climateSelections.length}
                onClick={() => clearClimateSelections()}
              >
                Clear
              </Button>
            </div>
          </DropdownSection>
        </div>
        <Separator className="bg-stone-300/80" />
        <div className="space-y-3">
          <LabelledField label="Custom location">
            <Input
              value={placeInput}
              onChange={(event) => setPlaceInput(event.target.value)}
              placeholder="Search for an address or place"
              className="border-stone-300 bg-[#fdfaf1] text-stone-800 placeholder:text-stone-400 focus-visible:ring-amber-700 focus-visible:ring-offset-[#fbf7ec]"
            />
          </LabelledField>
          {placesResult.isFetching && (
            <p className="text-xs text-stone-500">Searching…</p>
          )}
          {placesResult.error && (
            <p className="text-xs text-red-600">Unable to reach Places autocomplete right now.</p>
          )}
          {!placesResult.isFetching && placeSuggestions.length === 0 && placeInput.length >= 2 && (
            <p className="text-xs text-stone-500">No matches found.</p>
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
                  className="flex items-center justify-between rounded-xl border border-stone-300 bg-[#f9f3e4] px-3 py-2 shadow-sm"
                >
                  <div className="flex flex-col">
                    <span className="text-sm font-medium text-stone-900">{suggestion.label}</span>
                    {suggestion.sublabel && (
                      <span className="text-xs text-stone-500">{suggestion.sublabel}</span>
                    )}
                  </div>
                  <Button
                    size="sm"
                    variant="outline"
                    className={active ? brassActiveButtonClass : brassPrimaryButtonClass}
                    disabled={active || loading}
                    onClick={() => handleAddCustom(suggestion)}
                  >
                    {active ? 'Added' : loading ? 'Adding…' : 'Drop'}
                  </Button>
                </div>
              );
            })}
          </div>
          <p className="text-xs text-stone-500">
            Think: friend's house, work address etc.
          </p>
        </div>
      </CardContent>
    </Card>
  );
}

function LabelledField({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-xs font-semibold uppercase tracking-wide text-stone-500">{label}</span>
      {children}
    </label>
  );
}

function CatalogRow({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between rounded-xl border border-stone-300 bg-[#f7f0de] px-3 py-2 shadow-sm">
      <div className="flex flex-col">
        <span className="text-sm font-medium text-stone-900">{title}</span>
      </div>
      {children}
    </div>
  );
}

function DropdownSection({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <details className="rounded-2xl border border-stone-300 bg-[#f2ebd9] shadow-inner">
      <summary className="cursor-pointer select-none rounded-2xl px-3 py-2 text-sm font-semibold text-stone-700 transition-colors hover:bg-[#ebdfc3]">
        {label}
      </summary>
      <div className="max-h-60 space-y-2 overflow-y-auto border-t border-stone-200 bg-[#fbf7ec] px-3 py-3">
        {children}
      </div>
    </details>
  );
}
