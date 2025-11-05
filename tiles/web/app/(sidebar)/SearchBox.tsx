'use client';
// Handles catalog picks through dropdowns and custom pins.

import React from 'react';
import { useQuery } from '@tanstack/react-query';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Separator } from '@/components/ui/separator';
import { Slider } from '@/components/ui/slider';
import {
  addBrand,
  addCategory,
  addCustom,
  changePoiMode,
  clearClimateSelections,
  customCacheKey,
  ensureCatalogLoaded,
  MAX_MINUTES,
  MIN_MINUTES,
  MINUTE_STEP,
  normalizeMinutes,
  removePOI,
  setClimateSelections,
  setAvoidPowerLines,
  setPoiPins,
  toggleClimateSelection,
  setPoliticalLeanRange,
  clearPoliticalLeanRange,
  updateSlider,
  updateSliderPreview
} from '@/lib/actions';
import { CLIMATE_TYPOLOGY } from '@/lib/data/climate';
import { buildCategoryGroups, fetchPlaceDetails, fetchPlaceSuggestions, type PlaceSuggestion } from '@/lib/services';
import { useStore, type Mode, type POI } from '@/lib/state/store';
import { debounce } from '@/lib/utils';
import { getMapController } from '@/lib/map/MapController';

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
  const sliders = useStore((state) => state.sliders);

  const [placeInput, setPlaceInput] = React.useState('');
  const [placesQuery, setPlacesQuery] = React.useState('');
  const [pending, setPending] = React.useState<string | null>(null);
  const [session] = React.useState(() => createPlacesSession());
  const detailsCacheRef = React.useRef<Record<string, string>>({});
  const [sliderPreview, setSliderPreview] = React.useState<Record<string, number>>({});

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

  React.useEffect(() => {
    setSliderPreview((prev) => {
      if (!Object.keys(prev).length) {
        return prev;
      }
      const activeIds = new Set(pois.map((poi) => poi.id));
      let changed = false;
      const next: Record<string, number> = {};
      for (const [id, value] of Object.entries(prev)) {
        if (activeIds.has(id)) {
          next[id] = value;
        } else {
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  }, [pois]);

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

  const politicalRangeValue = React.useMemo<[number, number]>(
    () => (politicalLeanRange ? politicalLeanRange : [0, 4]),
    [politicalLeanRange]
  );

  const miscDropdownLabel = React.useMemo(() => {
    let activeCount = 0;
    if (politicalLeanRange) {
      activeCount += 1;
    }
    if (avoidPowerLines) {
      activeCount += 1;
    }
    return activeCount ? `Miscellaneous (${activeCount})` : 'Miscellaneous';
  }, [avoidPowerLines, politicalLeanRange]);

  const sliderMinutes = React.useMemo(() => {
    if (!pois.length) return [] as number[];
    return pois.map((poi) => {
      const preview = sliderPreview[poi.id];
      if (preview != null && !Number.isNaN(preview)) {
        return Math.max(MIN_MINUTES, Math.min(MAX_MINUTES, preview));
      }
      const stored = sliders[poi.id];
      if (stored != null && !Number.isNaN(stored)) {
        return Math.max(MIN_MINUTES, Math.min(MAX_MINUTES, stored));
      }
      return 30;
    });
  }, [pois, sliderPreview, sliders]);

  const activeFilterCount = pois.length;

  const livableAreaPercent = React.useMemo(() => {
    const base = 100;
    const poiPenalty = Math.min(activeFilterCount * 8, 40);
    const sliderPenalty = sliderMinutes.length
      ? (sliderMinutes.reduce((acc, minutes) => acc + (MAX_MINUTES - minutes), 0) /
          (sliderMinutes.length * (MAX_MINUTES - MIN_MINUTES))) * 40
      : 0;
    const climatePenalty = Math.min(climateSelections.length * 5, 25);
    const corridorPenalty = avoidPowerLines ? 6 : 0;
    const politicsPenalty = politicalLeanRange
      ? (1 - (Math.max(0, politicalLeanRange[1] - politicalLeanRange[0]) / 4)) * 18
      : 0;
    const deduction = poiPenalty + sliderPenalty + climatePenalty + corridorPenalty + politicsPenalty;
    const raw = base - deduction;
    return Math.max(1, Math.min(100, Math.round(raw)));
  }, [
    activeFilterCount,
    sliderMinutes,
    climateSelections.length,
    avoidPowerLines,
    politicalLeanRange
  ]);

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

  const handleClearAllFilters = React.useCallback(() => {
    if (!pois.length && !climateSelections.length && !avoidPowerLines && !politicalLeanRange) {
      return;
    }
    setSliderPreview({});
    pois.forEach((poi) => removePOI(poi.id));
    if (climateSelections.length) {
      clearClimateSelections();
    }
    if (avoidPowerLines) {
      setAvoidPowerLines(false);
    }
    if (politicalLeanRange) {
      clearPoliticalLeanRange();
    }
  }, [
    pois,
    climateSelections.length,
    avoidPowerLines,
    politicalLeanRange,
    setAvoidPowerLines,
    setSliderPreview
  ]);

  const handleSliderPreviewChange = React.useCallback((id: string, value: number | null) => {
    setSliderPreview((prev) => {
      if (value == null || Number.isNaN(value)) {
        if (!(id in prev)) {
          return prev;
        }
        const next = { ...prev };
        delete next[id];
        return next;
      }
      const bounded = Math.max(MIN_MINUTES, Math.min(MAX_MINUTES, value));
      if (prev[id] === bounded) {
        return prev;
      }
      return { ...prev, [id]: bounded };
    });
  }, [setSliderPreview]);

  return (
    <Card className="border-stone-300 bg-[#fbf7ec] p-0 shadow-[0_20px_36px_-30px_rgba(76,54,33,0.28)]">
      <CardHeader className="mb-0 flex flex-col gap-2 rounded-2xl rounded-b-none border-b border-stone-200 bg-[#f2ebd9] px-3 py-4">
        <CardTitle className="font-serif text-sm font-semibold text-stone-900">Add filters</CardTitle>
        <p className="text-xs text-stone-600">
          Build your own livable area using the menus below.
        </p>
      </CardHeader>
      <CardContent className="space-y-6 px-3 pb-5 pt-4 text-sm text-stone-700">
        <LivableAreaSummary percent={livableAreaPercent} areaLabel="MA" />
        <ActiveFiltersSection
          onClearAll={handleClearAllFilters}
          onSliderPreviewChange={handleSliderPreviewChange}
        />
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
        <Separator className="bg-stone-300/80" />
        <p className="text-xs font-semibold uppercase tracking-wide text-stone-500">Add filters</p>
        <div className="-mx-2 space-y-2 sm:-mx-3">
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
          <DropdownSection label={miscDropdownLabel}>
            <div className="space-y-4">
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
                <Slider
                  min={0}
                  max={4}
                  step={1}
                  minStepsBetweenThumbs={0}
                  value={politicalRangeValue}
                  onValueChange={handlePoliticalRangeChange}
                  aria-label="Political lean range"
                />
                <div className="flex justify-between text-[10px] text-stone-500">
                  {POLITICAL_LEAN_LABELS.map((label) => (
                    <span key={label}>{label}</span>
                  ))}
                </div>
                <p className="text-xs text-stone-600">
                  {politicalLeanRange
                    ? `Showing counties from ${POLITICAL_LEAN_LABELS[politicalLeanRange[0]]} to ${POLITICAL_LEAN_LABELS[politicalLeanRange[1]]}`
                    : 'Adjust the slider to focus on a political leaning range.'}
                </p>
                <p className="text-[11px] leading-snug text-stone-500">
                  Hexes without election data hide when you narrow this range.
                </p>
              </div>
              <Separator className="bg-stone-200" />
              <div className="space-y-2">
                <span className="text-sm font-semibold text-stone-900">Power lines filter</span>
                <div className="rounded-2xl border border-stone-300 bg-[#f2ebd9] px-3 py-3 shadow-inner">
                  <label className="flex items-start gap-3">
                    <input
                      type="checkbox"
                      className="mt-1 h-4 w-4 accent-amber-700"
                      checked={avoidPowerLines}
                      onChange={(event) => setAvoidPowerLines(event.target.checked)}
                    />
                    <div className="space-y-1">
                      <span className="text-sm font-medium text-stone-900">
                        Avoid power lines (high-voltage transmission corridors)
                      </span>
                      <p className="text-xs leading-snug text-stone-600">
                        Hide hexes within 200m of major overhead transmission lines.
                      </p>
                    </div>
                  </label>
                </div>
              </div>
            </div>
          </DropdownSection>
        </div>
      </CardContent>
    </Card>
  );
}

function LivableAreaSummary({ percent, areaLabel }: { percent: number; areaLabel: string }) {
  const clamped = Math.max(0, Math.min(100, Math.round(percent)));
  return (
    <div className="rounded-2xl border border-stone-300 bg-[#f2ebd9] p-4 shadow-inner">
      <p className="text-xs font-semibold uppercase tracking-wide text-stone-500">Livable Area</p>
      <div className="mt-2 flex items-baseline gap-2">
        <span className="text-3xl font-bold text-amber-900">{clamped}%</span>
        <span className="text-sm text-stone-600">of {areaLabel} meets your criteria</span>
      </div>
      <ProgressBar value={clamped} />
    </div>
  );
}

function ProgressBar({ value }: { value: number }) {
  const clamped = Math.max(0, Math.min(100, value));
  return (
    <div className="mt-3 h-2 w-full overflow-hidden rounded-full bg-stone-200">
      <div
        className="h-full rounded-full bg-gradient-to-r from-amber-700 via-amber-500 to-stone-200 transition-[width] duration-500 ease-out"
        style={{ width: `${clamped}%` }}
      />
    </div>
  );
}

function ActiveFiltersSection({
  onClearAll,
  onSliderPreviewChange
}: {
  onClearAll: () => void;
  onSliderPreviewChange: (id: string, value: number | null) => void;
}) {
  const pois = useStore((state) => state.pois);
  const sliders = useStore((state) => state.sliders);
  const poiModes = useStore((state) => state.poiModes);
  const defaultMode = useStore((state) => state.mode);
  const loadingPois = useStore((state) => state.loadingPois);
  const showPins = useStore((state) => state.showPins);

  const [localValues, setLocalValues] = React.useState<Record<string, number>>({});
  const [expandedId, setExpandedId] = React.useState<string | null>(null);
  const [modePending, setModePending] = React.useState<Record<string, boolean>>({});
  const [, setDraggingMap] = React.useState<Record<string, boolean>>({});

  React.useEffect(() => {
    setLocalValues((prev) => {
      const next = { ...prev };
      for (const key of Object.keys(next)) {
        if (!(key in sliders)) {
          delete next[key];
        }
      }
      return next;
    });
  }, [sliders]);

  React.useEffect(() => {
    if (expandedId && !pois.some((poi) => poi.id === expandedId)) {
      setExpandedId(null);
    }
  }, [expandedId, pois]);

  const makeSliderValue = React.useCallback(
    (id: string) => localValues[id] ?? sliders[id] ?? 30,
    [localValues, sliders]
  );

  const toggleExpanded = React.useCallback((id: string) => {
    setExpandedId((current) => (current === id ? null : id));
  }, []);

  const handleModeChange = React.useCallback(async (id: string, target: Mode) => {
    setModePending((prev) => ({ ...prev, [id]: true }));
    try {
      await changePoiMode(id, target);
    } catch (error) {
      console.error('Failed to change travel mode', error);
    } finally {
      setModePending((prev) => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
    }
  }, []);

  const handleSliderChange = React.useCallback((id: string, values: number[]) => {
    const next = values[0] ?? MIN_MINUTES;
    setDraggingMap((prev) => {
      if (prev[id]) {
        return prev;
      }
      const copy = { ...prev, [id]: true };
      getMapController()?.setDragging(true);
      return copy;
    });
    setLocalValues((prev) => ({ ...prev, [id]: next }));
    onSliderPreviewChange(id, next);
    updateSliderPreview(id, next);
  }, [onSliderPreviewChange]);

  const handleSliderCommit = React.useCallback((id: string, values: number[]) => {
    const next = values[0] ?? MIN_MINUTES;
    getMapController()?.setDragging(false);
    setDraggingMap((prev) => {
      if (!prev[id]) return prev;
      const copy = { ...prev };
      delete copy[id];
      return copy;
    });
    setLocalValues((prev) => {
      if (!(id in prev)) return prev;
      const copy = { ...prev };
      delete copy[id];
      return copy;
    });
    onSliderPreviewChange(id, null);
    updateSlider(id, next);
  }, [onSliderPreviewChange]);

  const header = (
    <div className="flex items-center justify-between">
      <p className="text-sm font-semibold text-stone-900">Active Filters ({pois.length})</p>
      {pois.length > 0 && (
        <button
          type="button"
          className="text-xs font-semibold text-amber-900 underline-offset-2 transition-colors hover:underline"
          onClick={onClearAll}
        >
          Clear all
        </button>
      )}
    </div>
  );

  if (!pois.length) {
    return (
      <div className="space-y-2">
        {header}
        <p className="text-xs text-stone-500">
          No filters applied yet. Add filters below to start shaping your livable area.
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {header}
      <div className="space-y-2">
        {pois.map((poi) => {
          const sliderValue = makeSliderValue(poi.id);
          const currentMode = poiModes[poi.id] ?? defaultMode;
          const isLoading = loadingPois.has(poi.id);
          const pinsVisible = Boolean(showPins[poi.id]);
          const expanded = expandedId === poi.id;
          const pendingMode = modePending[poi.id] ?? false;
          const typeLabel = poi.type === 'category' ? null : poi.type;

          return (
            <div
              key={poi.id}
              className="overflow-hidden rounded-2xl border border-stone-300 bg-[#fdf1df] shadow-sm transition-shadow hover:shadow"
            >
              <div className="flex items-center justify-between gap-3 px-3 py-2">
                <div className="flex min-w-0 flex-1 items-start gap-2">
                  <FilterTypeDot type={poi.type} />
                  <div className="flex min-w-0 flex-col">
                    <span className="truncate text-sm font-medium text-stone-900">{poi.label}</span>
                    {typeLabel && (
                      <span className="text-[11px] uppercase tracking-wide text-stone-500">
                        {typeLabel}
                      </span>
                    )}
                    {poi.formattedAddress && (
                      <span className="truncate text-[11px] text-stone-500">
                        {poi.formattedAddress}
                      </span>
                    )}
                  </div>
                  {isLoading && (
                    <div className="flex items-center gap-1 text-amber-800">
                      <Spinner />
                      <span className="text-[11px] font-medium">Computing…</span>
                    </div>
                  )}
                </div>
                <div className="flex items-center gap-1.5">
                  <Button
                    size="icon"
                    variant="ghost"
                    className={`h-8 w-8 rounded-full border border-transparent text-stone-600 transition-colors hover:border-amber-900 hover:text-amber-900 ${
                      expanded ? 'bg-white' : ''
                    }`}
                    aria-label={`Configure ${poi.label}`}
                    onClick={() => toggleExpanded(poi.id)}
                  >
                    <GearIcon className="h-4 w-4" />
                  </Button>
                  <Button
                    size="icon"
                    variant="ghost"
                    className="h-8 w-8 rounded-full border border-transparent text-stone-600 transition-colors hover:border-amber-900 hover:text-amber-900"
                    aria-label={`Remove ${poi.label}`}
                    onClick={() => {
                      onSliderPreviewChange(poi.id, null);
                      removePOI(poi.id);
                    }}
                  >
                    <span aria-hidden className="text-base leading-none">
                      ×
                    </span>
                  </Button>
                </div>
              </div>
              {expanded && (
                <div className="space-y-3 border-t border-stone-200 bg-[#fbf7ec] px-3 py-3 text-xs text-stone-600">
                  <div className="flex items-center justify-between">
                    <span className="text-[11px] uppercase tracking-wide text-stone-500">
                      Max travel time
                    </span>
                    <Badge variant="outline" className="border-amber-300 text-amber-900">
                      {sliderValue} min
                    </Badge>
                  </div>
                  <Slider
                    min={MIN_MINUTES}
                    max={MAX_MINUTES}
                    step={MINUTE_STEP}
                    value={[sliderValue]}
                    disabled={isLoading}
                    onValueChange={(values) => handleSliderChange(poi.id, values)}
                    onValueCommit={(values) => handleSliderCommit(poi.id, values)}
                    aria-label={`Maximum travel time for ${poi.label}`}
                  />
                  <div className="flex items-center gap-3">
                    <div className="flex items-center gap-2">
                      <span className="text-[11px] uppercase tracking-wide text-stone-500 whitespace-nowrap">
                        Travel mode
                      </span>
                      <ModeToggleCompact
                        value={currentMode}
                        disabled={isLoading || pendingMode}
                        onChange={(mode) => {
                          if (mode === currentMode) return;
                          void handleModeChange(poi.id, mode);
                        }}
                      />
                    </div>
                    <label className="ml-auto flex items-center gap-2 whitespace-nowrap text-xs font-semibold text-stone-600">
                      <input
                        type="checkbox"
                        className="h-3 w-3 accent-amber-700"
                        checked={pinsVisible}
                        disabled={isLoading}
                        onChange={(event) => setPoiPins(poi.id, event.target.checked)}
                      />
                      <span className="text-[11px] uppercase tracking-wide text-stone-500">
                        Show pins
                      </span>
                    </label>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function FilterTypeDot({ type }: { type: POI['type'] }) {
  const typeClasses: Record<POI['type'], string> = {
    category: 'bg-amber-700',
    brand: 'bg-emerald-600',
    custom: 'bg-indigo-600'
  };
  return <span className={`mt-1 h-2.5 w-2.5 flex-shrink-0 rounded-full ${typeClasses[type]}`} />;
}

function GearIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.5}
      strokeLinecap="round"
      strokeLinejoin="round"
      {...props}
    >
      <path d="M11.983 15.25a3.25 3.25 0 1 0 0-6.5 3.25 3.25 0 0 0 0 6.5Z" />
      <path d="M19.63 13.6c.05-.52.05-1.04 0-1.56l1.76-1.36a.5.5 0 0 0 .12-.64l-1.76-3.05a.5.5 0 0 0-.62-.2l-2.06.83a6.42 6.42 0 0 0-1.35-.78l-.32-2.18a.5.5 0 0 0-.5-.43h-3.53a.5.5 0 0 0-.5.43l-.32 2.18a6.42 6.42 0 0 0-1.35.78l-2.06-.83a.5.5 0 0 0-.62.2L2.5 10.04a.5.5 0 0 0 .12.64l1.76 1.36c-.05.52-.05 1.04 0 1.56l-1.76 1.36a.5.5 0 0 0-.12.64l1.76 3.05a.5.5 0 0 0 .62.2l2.06-.83c.42.33.87.6 1.35.78l.32 2.18a.5.5 0 0 0 .5.43h3.53a.5.5 0 0 0 .5-.43l.32-2.18c.48-.18.93-.45 1.35-.78l2.06.83a.5.5 0 0 0 .62-.2l1.76-3.05a.5.5 0 0 0-.12-.64Z" />
    </svg>
  );
}

function Spinner() {
  return (
    <svg
      className="h-4 w-4 animate-spin"
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      viewBox="0 0 24 24"
    >
      <circle
        className="opacity-25"
        cx="12"
        cy="12"
        r="10"
        stroke="currentColor"
        strokeWidth="4"
      ></circle>
      <path
        className="opacity-75"
        fill="currentColor"
        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
      ></path>
    </svg>
  );
}

function ModeToggleCompact({
  value,
  onChange,
  disabled
}: {
  value: Mode;
  onChange: (mode: Mode) => void;
  disabled?: boolean;
}) {
  return (
    <div className="flex items-center gap-1">
      <Button
        size="sm"
        variant="outline"
        className={`h-7 px-3 text-xs ${
          value === 'drive'
            ? 'border-amber-900 bg-amber-800 text-amber-50 shadow-sm'
            : 'border-stone-300 bg-white text-stone-700'
        }`}
        disabled={disabled || value === 'drive'}
        onClick={() => onChange('drive')}
      >
        Drive
      </Button>
      <Button
        size="sm"
        variant="outline"
        className={`h-7 px-3 text-xs ${
          value === 'walk'
            ? 'border-amber-900 bg-amber-800 text-amber-50 shadow-sm'
            : 'border-stone-300 bg-white text-stone-700'
        }`}
        disabled={disabled || value === 'walk'}
        onClick={() => onChange('walk')}
      >
        Walk
      </Button>
    </div>
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
    <details className="block w-full overflow-hidden rounded-2xl border border-stone-300 bg-[#f2ebd9] shadow-inner">
      <summary className="cursor-pointer select-none rounded-2xl px-3 py-3 text-sm font-semibold text-stone-700 transition-colors hover:bg-[#ebdfc3]">
        {label}
      </summary>
      <div className="max-h-60 space-y-2 overflow-y-auto border-t border-stone-200 bg-[#fbf7ec] px-3 py-3">
        {children}
      </div>
    </details>
  );
}
