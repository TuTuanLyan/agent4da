import en from "./en.json";
import vi from "./vi.json";

export type Locale = "en" | "vi";
type Messages = Record<string, string>;

const DICTS: Record<Locale, Messages> = { en, vi };

export function t(key: string, locale: Locale = "vi"): string {
  return DICTS[locale]?.[key] ?? DICTS.en[key] ?? key;
}
