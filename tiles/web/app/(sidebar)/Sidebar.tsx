'use client';

import React from 'react';

import { Button } from '@/components/ui/button';
import { Separator } from '@/components/ui/separator';
import HoverBox from '@/app/(sidebar)/HoverBox';
import FiltersPanel from '@/app/(sidebar)/FiltersPanel';
import SearchBox from '@/app/(sidebar)/SearchBox';
import ShareButton from '@/app/(shared)/ShareButton';
import { changeMode } from '@/lib/actions';
import { useMode } from '@/lib/state/selectors';

export default function Sidebar() {
  return (
    <aside className="flex h-full w-[var(--sidebar-width)] flex-col border-r border-slate-200 bg-white shadow-sidebar">
      <div className="flex h-full flex-col gap-6 overflow-y-auto px-7 py-8">
        <Header />
        <ModeSwitcher />
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

function ModeSwitcher() {
  const mode = useMode();
  const [pending, setPending] = React.useState(false);

  const change = async (target: "drive" | "walk") => {
    if (mode === target) return;
    setPending(true);
    try {
      await changeMode(target);
    } finally {
      setPending(false);
    }
  };

  return (
    <div className="flex items-center justify-between rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
      <div>
        <p className="text-sm font-semibold text-slate-800">Travel mode</p>
        <p className="text-xs text-slate-500">Switch between drive and walk coverage.</p>
      </div>
      <div className="flex items-center gap-2">
        <Button
          size="sm"
          variant={mode === 'drive' ? 'default' : 'outline'}
          disabled={pending}
          onClick={() => {
            void change('drive');
          }}
        >
          Drive
        </Button>
        <Button
          size="sm"
          variant={mode === 'walk' ? 'default' : 'outline'}
          disabled={pending}
          onClick={() => {
            void change('walk');
          }}
        >
          Walk
        </Button>
      </div>
    </div>
  );
}
