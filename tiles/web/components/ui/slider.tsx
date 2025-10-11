'use client';
// Styled Radix slider with optional value display.

import * as React from "react";
import * as SliderPrimitive from "@radix-ui/react-slider";

import { cn } from "@/lib/utils";

export interface SliderProps extends React.ComponentPropsWithoutRef<typeof SliderPrimitive.Root> {
  withValue?: boolean;
}

const Slider = React.forwardRef<React.ElementRef<typeof SliderPrimitive.Root>, SliderProps>(
  ({ className, withValue = false, ...props }, ref) => (
    <div className={cn("flex items-center gap-3", withValue && "w-full")}>
      <SliderPrimitive.Root
        ref={ref}
        className={cn("relative flex h-4 w-full touch-none select-none items-center", className)}
        {...props}
      >
        <SliderPrimitive.Track className="relative h-1.5 w-full grow overflow-hidden rounded-full bg-stone-200">
          <SliderPrimitive.Range className="absolute h-full bg-amber-800" />
        </SliderPrimitive.Track>
        <SliderPrimitive.Thumb className="block h-4 w-4 rounded-full border border-amber-900 bg-[#f8f1e2] shadow focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-amber-700" />
      </SliderPrimitive.Root>
      {withValue && (
        <span className="min-w-[3rem] rounded-full bg-slate-100 px-2 py-1 text-center text-xs font-semibold text-slate-600">
          {props.value?.[0] ?? props.defaultValue?.[0] ?? 0} min
        </span>
      )}
    </div>
  )
);
Slider.displayName = SliderPrimitive.Root.displayName;

export { Slider };
