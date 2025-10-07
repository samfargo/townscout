'use client';

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
    <div className="flex items-center justify-between rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
      <div>
        <p className="text-sm font-semibold text-slate-800">Share this view</p>
        <p className="text-xs text-slate-500">Copy a link with the current filters applied.</p>
      </div>
      <Button size="sm" variant="outline" onClick={handleShare}>
        {copied ? 'Copied' : 'Copy link'}
      </Button>
    </div>
  );
}
