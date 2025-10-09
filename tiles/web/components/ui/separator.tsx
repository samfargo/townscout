'use client';
// Separator component for horizontal and vertical dividers.

import * as React from "react";

import { cn } from "@/lib/utils/cn";

export interface SeparatorProps extends React.HTMLAttributes<HTMLDivElement> {
  orientation?: "horizontal" | "vertical";
}

export function Separator({ className, orientation = "horizontal", ...props }: SeparatorProps) {
  return (
    <div
      className={cn(
        "bg-slate-200",
        orientation === "horizontal" ? "my-4 h-px w-full" : "mx-4 h-full w-px",
        className
      )}
      {...props}
    />
  );
}
