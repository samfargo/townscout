// Utility for combining classNames (similar to clsx/cn pattern)
export function cn(...inputs: (string | undefined | null | boolean)[]): string {
  return inputs.filter(Boolean).join(' ');
}
