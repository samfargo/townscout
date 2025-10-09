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
    <aside className="flex h-full min-h-0 w-[var(--sidebar-width)] shrink-0 flex-col overflow-hidden border-r border-slate-200 bg-white shadow-sidebar">
      <div className="flex flex-1 flex-col gap-6 overflow-y-auto px-7 py-8">
        <Header />
        <SearchBox />
        <FiltersPanel />
        <HoverBox />
        <Separator />
        <ShareButton />
      </div>
    </aside>
  );
}

function Header() {
  return (
    <div className="flex items-center gap-3">
      <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br from-blue-600 to-sky-400 text-lg font-semibold text-white">
        TS
      </div>
      <div>
        <h1 className="text-lg font-semibold text-slate-900">TownScout</h1>
        <p className="text-sm text-slate-500">
          Explore drive-time and walk-time coverage across the U.S.
        </p>
      </div>
    </div>
  );
}
