'use client';
// Handles catalog picks through dropdowns and custom pins.

import React from 'react';
import { useQuery } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Separator } from '@/components/ui/separator';
import { Slider } from '@/components/ui/slider';
import {
  addBrand,
  addCategory,
  addCustom,
  clearClimateSelections,
  customCacheKey,
  ensureCatalogLoaded,
  removePOI,
  normalizeMinutes,
  setClimateSelections,
  setAvoidPowerLines,
  toggleClimateSelection,
  setPoliticalLeanRange,
  clearPoliticalLeanRange
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

const POLITICAL_LEAN_LABELS = ['Strong Dem', 'Lean Dem', 'Moderate', 'Lean Rep', 'Strong Rep'] as const;

export default function SearchBox() {
  const catalog = useStore((state) => state.catalog);
  const pois = useStore((state) => state.pois);
  const climateSelections = useStore((state) => state.climateSelections);
  const avoidPowerLines = useStore((state) => state.avoidPowerLines);
  const politicalLeanRange = useStore((state) => state.politicalLeanRange);

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

  const traumaCategoryIds = React.useMemo(() => {
    if (!catalog?.loaded) return new Set<string>();
    return new Set(
      catalog.categories
        .filter((category) => category.group === 'hospital_trauma')
        .map((category) => String(category.id))
    );
  }, [catalog]);

  const hospitalCategoryId = React.useMemo(() => {
    if (!catalog?.loaded) return null;
    const match = catalog.categories.find(
      (category) => category.group === 'hospital' || category.label?.toLowerCase() === 'hospital'
    );
    return match ? String(match.id) : null;
  }, [catalog]);

  const hospitalCategoryGroup = React.useMemo(
    () =>
      categoryGroups.find((group) =>
        hospitalCategoryId ? group.id === hospitalCategoryId : group.label.toLowerCase() === 'hospital'
      ),
    [categoryGroups, hospitalCategoryId]
  );

  const traumaCategoryGroups = React.useMemo(
    () => categoryGroups.filter((group) => traumaCategoryIds.has(group.id)),
    [categoryGroups, traumaCategoryIds]
  );

  const displayCategoryGroups = React.useMemo(
    () =>
      categoryGroups.filter(
        (group) => group.id !== hospitalCategoryGroup?.id && !traumaCategoryIds.has(group.id)
      ),
    [categoryGroups, hospitalCategoryGroup?.id, traumaCategoryIds]
  );

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

  const handleAddHospital = async () => {
    if (!hospitalCategoryGroup) {
      return;
    }
    // Drop any specialty trauma filters before adding the broader hospital bucket.
    traumaCategoryGroups.forEach((group) => {
      if (isPoiActive(group.id)) {
        removePOI(group.id);
      }
    });
    await handleAddCategory(hospitalCategoryGroup);
  };

  const handleToggleTraumaCategory = async (
    group: (typeof categoryGroups)[number],
    nextChecked: boolean
  ) => {
    if (nextChecked) {
      if (hospitalCategoryGroup && isPoiActive(hospitalCategoryGroup.id)) {
        removePOI(hospitalCategoryGroup.id);
      }
      await handleAddCategory(group);
      return;
    }

    setPending(`category:${group.id}`);
    try {
      removePOI(group.id);
    } catch (err) {
      console.error('Failed to remove trauma category', err);
    } finally {
      setPending(null);
    }
  };

  const hospitalActive = hospitalCategoryGroup ? isPoiActive(hospitalCategoryGroup.id) : false;
  const hospitalLoading = hospitalCategoryGroup ? pending === `category:${hospitalCategoryGroup.id}` : false;

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
      let formattedAddress: string | null = suggestion.description ?? null;
      if (lon == null || lat == null) {
        const details = await fetchPlaceDetails(suggestion.id, session);
        lon = details.lon;
        lat = details.lat;
        label = details.label;
        formattedAddress = details.formatted_address ?? formattedAddress;
      }
      if (lon == null || lat == null) {
        throw new Error('Place has no coordinates');
      }
      const key = customCacheKey(lon, lat);
      detailsCacheRef.current[suggestion.id] = key;
      await addCustom(lon, lat, label, normalizeMinutes(30), formattedAddress);
      setPlaceInput('');
      setPlacesQuery('');
    } catch (err) {
      console.error('Failed to add custom place', err);
    } finally {
      setPending(null);
    }
  };

  const poiCategoryCount = React.useMemo(
    () => pois.filter((poi) => poi.type === 'category').length,
    [pois]
  );
  const poiBrandCount = React.useMemo(
    () => pois.filter((poi) => poi.type === 'brand').length,
    [pois]
  );

  const pointsLabel = React.useMemo(() => {
    if (!poiCategoryCount) return 'Points of Interest';
    return `Points of Interest (${poiCategoryCount})`;
  }, [poiCategoryCount]);

  const businessesLabel = React.useMemo(() => {
    if (!poiBrandCount) return 'Popular Businesses';
    return `Popular Businesses (${poiBrandCount})`;
  }, [poiBrandCount]);

  const climateLabel = React.useMemo(() => {
    if (!climateSelections.length) return 'Climate';
    return `Climate (${climateSelections.length})`;
  }, [climateSelections.length]);

  const politicalDropdownLabel = React.useMemo(() => {
    if (!politicalLeanRange) return 'Political views';
    const [min, max] = politicalLeanRange;
    if (min <= 0 && max >= 4) {
      return 'Political views (All)';
    }
    if (min === max) {
      return `Political views (${POLITICAL_LEAN_LABELS[min]})`;
    }
    return `Political views (${POLITICAL_LEAN_LABELS[min]}–${POLITICAL_LEAN_LABELS[max]})`;
  }, [politicalLeanRange]);

  const handlePoliticalRangeChange = React.useCallback((values: number[]) => {
    if (!Array.isArray(values) || values.length !== 2) {
      return;
    }
    const [rawMin, rawMax] = values.slice().sort((a, b) => a - b) as [number, number];
    const nextRange: [number, number] = [
      Math.max(0, Math.min(4, Math.round(rawMin))),
      Math.max(0, Math.min(4, Math.round(rawMax)))
    ];
    setPoliticalLeanRange(nextRange);
  }, [setPoliticalLeanRange]);

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
          <DropdownSection label={pointsLabel}>
            {!catalog?.loaded && (
              <p className="px-1 text-xs text-stone-500">Loading catalog…</p>
            )}
            {catalog?.loaded && categoryGroups.length === 0 && (
              <p className="px-1 text-xs text-red-600">Catalog unavailable right now.</p>
            )}
            {catalog?.loaded && categoryGroups.length > 0 && (
              <div className="space-y-2">
                {hospitalCategoryGroup && (
                  <div className="rounded-xl border border-stone-300 bg-[#f7f0de] px-3 py-2 shadow-sm">
                    <div className="flex items-start justify-between gap-3">
                      <div className="flex flex-col">
                        <span className="text-sm font-medium text-stone-900">
                          {hospitalCategoryGroup.label}
                        </span>
                        <span className="text-[11px] uppercase tracking-wide text-stone-500">
                          Any hospital
                        </span>
                      </div>
                      <Button
                        size="sm"
                        variant="outline"
                        className={hospitalActive ? brassActiveButtonClass : brassPrimaryButtonClass}
                        disabled={hospitalActive || hospitalLoading}
                        onClick={handleAddHospital}
                      >
                        {hospitalActive ? 'Added' : hospitalLoading ? 'Adding…' : 'Add'}
                      </Button>
                    </div>
                    {traumaCategoryGroups.length > 0 && (
                      <div className="mt-2 flex flex-wrap gap-2">
                        {traumaCategoryGroups.map((group) => {
                          const active = isPoiActive(group.id);
                          const loading = pending === `category:${group.id}`;
                          return (
                            <label
                              key={group.id}
                              className="flex items-center gap-2 rounded-lg border border-stone-300 bg-[#fbf7ec] px-2 py-1 text-[11px] text-stone-600 shadow-sm"
                            >
                              <input
                                type="checkbox"
                                className="h-3 w-3 accent-amber-700"
                                checked={active}
                                disabled={loading}
                                onChange={(event) => handleToggleTraumaCategory(group, event.target.checked)}
                              />
                              <span>Only {group.label}</span>
                            </label>
                          );
                        })}
                      </div>
                    )}
                  </div>
                )}
                {displayCategoryGroups.map((group) => {
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
          <DropdownSection label={businessesLabel}>
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
          <DropdownSection label={climateLabel}>
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
          <DropdownSection label={politicalDropdownLabel}>
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-sm font-semibold text-stone-900">Political views filter</span>
                {politicalLeanRange && (
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-6 px-2 text-xs text-stone-600 transition-colors hover:text-amber-900"
                    onClick={() => clearPoliticalLeanRange()}
                  >
                    Clear
                  </Button>
                )}
              </div>
              {politicalLeanRange ? (
                <>
                  <Slider
                    min={0}
                    max={4}
                    step={1}
                    minStepsBetweenThumbs={0}
                    value={politicalLeanRange}
                    onValueChange={handlePoliticalRangeChange}
                    aria-label="Political lean range"
                  />
                  <div className="flex justify-between text-[10px] text-stone-500">
                    {POLITICAL_LEAN_LABELS.map((label) => (
                      <span key={label}>{label}</span>
                    ))}
                  </div>
                  <p className="text-xs text-stone-600">
                    Showing counties from {POLITICAL_LEAN_LABELS[politicalLeanRange[0]]} to {POLITICAL_LEAN_LABELS[politicalLeanRange[1]]}
                  </p>
                  <p className="text-[11px] leading-snug text-stone-500">
                    Hexes without election data hide when you narrow this range.
                  </p>
                </>
              ) : (
                <Button
                  size="sm"
                  variant="outline"
                  className="w-full border-amber-700 text-amber-900 transition-colors hover:bg-amber-50"
                  onClick={() => setPoliticalLeanRange([0, 4])}
                >
                  Enable Political Filter
                </Button>
              )}
            </div>
          </DropdownSection>
          <div className="rounded-2xl border border-stone-300 bg-[#f2ebd9] px-4 py-3 shadow-inner">
            <label className="flex items-start gap-3">
              <input
                type="checkbox"
                className="mt-1 h-4 w-4 accent-amber-700"
                checked={avoidPowerLines}
                onChange={(event) => setAvoidPowerLines(event.target.checked)}
              />
              <div className="space-y-1">
                <span className="text-sm font-semibold text-stone-900">
                  Avoid power lines (high-voltage transmission corridors)
                </span>
                <p className="text-xs leading-snug text-stone-600">
                  Hide hexes within 200m of major overhead transmission lines.
                </p>
              </div>
            </label>
          </div>
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
