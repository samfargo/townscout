"use client";

import * as React from "react";

import { cn } from "@/lib/utils/cn";

export interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  variant?: "default" | "muted" | "outline";
}

export function Badge({ className, variant = "default", ...props }: BadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold transition-colors",
        variant === "default" && "border-transparent bg-slate-900 text-slate-50",
        variant === "muted" && "border-transparent bg-slate-100 text-slate-600",
        variant === "outline" && "border-slate-200 text-slate-700",
        className
      )}
      {...props}
    />
  );
}
