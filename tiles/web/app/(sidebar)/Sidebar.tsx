'use client';
// Assembles the sidebar layout with search, filters, and sharing.

import React from 'react';

import { Separator } from '@/components/ui/separator';
import HoverBox from '@/app/(sidebar)/HoverBox';
import FiltersPanel from '@/app/(sidebar)/FiltersPanel';
import SearchBox from '@/app/(sidebar)/SearchBox';
import ShareButton from '@/app/(shared)/ShareButton';

export default function Sidebar() {
  return (
    <aside className="flex h-full min-h-0 w-[var(--sidebar-width)] shrink-0 flex-col overflow-hidden border-r border-stone-200 bg-[#f6f1e2] shadow-[8px_0_32px_rgba(120,94,61,0.08)]">
      <div className="flex flex-1 flex-col gap-6 overflow-y-auto px-7 py-8">
        <Header />
        <SearchBox />
        <FiltersPanel />
        <HoverBox />
        <Separator className="bg-stone-300/70" />
        <ShareButton />
      </div>
    </aside>
  );
}

function Header() {
  return (
    <div className="flex items-center gap-3 rounded-2xl border border-stone-300 bg-[#fbf7ec] px-3 py-2 shadow-sm">
      <div className="flex h-12 w-12 items-center justify-center rounded-xl border border-amber-900 bg-amber-800 text-lg font-semibold text-amber-50 shadow-[0_6px_15px_-10px_rgba(88,59,33,0.9)]">
        TS
      </div>
      <div>
        <h1 className="font-serif text-lg font-semibold text-stone-900">TownScout</h1>
        <p className="text-sm text-stone-600">
          Explore drive-time and walk-time coverage across the U.S.
        </p>
      </div>
    </div>
  );
}
