'use client';
// Provides copyable sharing links for the current view.

import React from 'react';

import { Button } from '@/components/ui/button';

export default function ShareButton() {
  const [copied, setCopied] = React.useState(false);

  const handleShare = React.useCallback(async () => {
    const url = window.location.href;
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(url);
      } else {
        const textarea = document.createElement('textarea');
        textarea.value = url;
        textarea.style.position = 'fixed';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.focus();
        textarea.select();
        document.execCommand('copy');
        document.body.removeChild(textarea);
      }
      setCopied(true);
      setTimeout(() => setCopied(false), 1800);
    } catch (err) {
      console.error('Failed to copy link', err);
    }
  }, []);

  return (
    <div className="flex items-center justify-between rounded-2xl border border-stone-300 bg-[#f3ecd9] px-4 py-3 shadow-[0_12px_28px_-24px_rgba(76,54,33,0.25)]">
      <div>
        <p className="text-sm font-semibold text-stone-900">Share this view</p>
        <p className="text-xs text-stone-500">Copy a link with the current filters applied.</p>
      </div>
      <Button
        size="sm"
        variant="outline"
        className={
          copied
            ? 'border border-stone-400 bg-stone-200 text-stone-800 focus-visible:ring-amber-700'
            : 'border border-amber-900 bg-amber-800 text-amber-50 shadow-sm transition-transform hover:-translate-y-0.5 hover:bg-amber-900 focus-visible:ring-amber-700'
        }
        onClick={handleShare}
      >
        {copied ? 'Copied' : 'Copy link'}
      </Button>
    </div>
  );
}
