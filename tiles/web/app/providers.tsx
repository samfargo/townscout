'use client';
// Wraps the app with a shared React Query client and restores persisted filters.

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import * as React from "react";

import { restorePersistedFilters } from "@/lib/actions";
import { useStore } from "@/lib/state/store";

type ProvidersProps = {
  children: React.ReactNode;
};

function FiltersHydrator() {
  const persistApi = useStore.persist;

  React.useEffect(() => {
    const unsubscribe = persistApi?.onFinishHydration?.(() => {
      void restorePersistedFilters();
    });

    if (persistApi?.hasHydrated?.()) {
      void restorePersistedFilters();
    }

    return () => {
      unsubscribe?.();
    };
  }, [persistApi]);

  return null;
}

export function Providers({ children }: ProvidersProps) {
  const [queryClient] = React.useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 5 * 60 * 1000,
            retry: 1,
            refetchOnWindowFocus: false
          }
        }
      })
  );

  return (
    <QueryClientProvider client={queryClient}>
      <FiltersHydrator />
      {children}
    </QueryClientProvider>
  );
}
