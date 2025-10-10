'use client';
// Assembles the sidebar layout with search, filters, and sharing.

import React from 'react';
import Image from 'next/image';

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
    <div className="flex items-center justify-center rounded-2xl border border-stone-300 bg-[#fbf7ec] px-4 py-3 shadow-sm">
      <Image
        src="/vicinity.png"
        alt="Vicinity wordmark"
        width={928}
        height={394}
        className="h-auto w-40"
        priority
      />
    </div>
  );
}
