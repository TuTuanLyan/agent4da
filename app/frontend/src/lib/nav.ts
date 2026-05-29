import {
  MessageSquareText,
  History,
  BookOpen,
  Activity,
  Settings,
  type LucideIcon,
} from "lucide-react";

export type NavItem = {
  href: string;
  label: string;
  icon: LucideIcon;
};

/** Sidebar entries, in the order from docs/WEB_APP_PLAN.md Section 6.1. */
export const NAV_ITEMS: ReadonlyArray<NavItem> = [
  { href: "/ask", label: "Ask", icon: MessageSquareText },
  { href: "/history", label: "History", icon: History },
  { href: "/catalog", label: "Catalog", icon: BookOpen },
  { href: "/pipelines", label: "Pipelines", icon: Activity },
  { href: "/settings", label: "Settings", icon: Settings },
];
