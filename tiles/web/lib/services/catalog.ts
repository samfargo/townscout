// Catalog service for managing brands and categories
import { fetchApi } from './api';

export interface Brand {
  id: string;
  name?: string;
  label: string;
  group?: string;
}

export interface Category {
  id: string | number;
  label: string;
  group?: string;
}

export interface Catalog {
  brands: Brand[];
  categories: Category[];
  loaded: boolean;
  catToBrands: Record<string, string[]>;
}

export interface CategoryGroup {
  id: string;
  label: string;
  brandIds: string[];
}

export async function fetchCatalog(): Promise<Catalog> {
  const payload = await fetchApi<any>('/api/catalog');

  const categories: Category[] = (payload.categories ?? []).map((cat: any) => {
    const id = cat?.id ?? cat?.category_id ?? cat?.key ?? cat?.value ?? null;
    return {
      id: id != null ? String(id) : '',
      label: cat?.label ?? cat?.name ?? (id != null ? `Category ${id}` : 'Category'),
      group: cat?.group
    };
  }).filter((cat: Category) => Boolean(cat.id));

  const brands: Brand[] = (payload.brands ?? []).map((brand: any) => {
    const id = brand?.id ?? brand?.brand_id ?? brand?.value ?? null;
    return {
      id: id != null ? String(id) : '',
      label: brand?.label ?? brand?.name ?? (id != null ? String(id) : 'Brand'),
      name: brand?.name,
      group: brand?.group
    };
  }).filter((brand: Brand) => Boolean(brand.id));

  const rawMapping = payload.catToBrands ?? payload.cat_to_brands ?? {};
  const catToBrands: Record<string, string[]> = {};
  for (const [key, value] of Object.entries(rawMapping)) {
    const normalizedKey = String(key);
    const normalizedValues = Array.isArray(value)
      ? value.map((item) => String(item))
      : [];
    catToBrands[normalizedKey] = normalizedValues;
  }

  return {
    categories,
    brands,
    catToBrands,
    loaded: true
  };
}

export function buildCategoryGroups(input: {
  categories: Category[];
  brands: Brand[];
  catToBrands: Record<string, string[]>;
}): CategoryGroup[] {
  return input.categories.map((cat) => ({
    id: String(cat.id),
    label: cat.label,
    brandIds: input.catToBrands[String(cat.id)] || []
  }));
}
