// Root layout wiring metadata and global styles.
import type { Metadata } from "next";
import { Providers } from "./providers";

import "@/styles/tailwind.css";
import "@/styles/globals.css";

export const metadata: Metadata = {
  title: "TownScout",
  description: "Explore drive-time and walk-time coverage for the places that matter."
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="bg-[var(--color-bg)] text-slate-900 antialiased">
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
