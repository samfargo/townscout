import dynamic from "next/dynamic";

import Sidebar from "@/app/(sidebar)/Sidebar";

const MapCanvas = dynamic(() => import("@/app/(map)/MapCanvas"), { ssr: false });

export default function HomePage() {
  return (
    <div className="flex min-h-screen w-full">
      <Sidebar />
      <main className="relative flex-1">
        <div className="absolute inset-0">
          <MapCanvas />
        </div>
        <div className="pointer-events-none absolute bottom-4 left-1/2 w-max -translate-x-1/2 rounded-full bg-white/80 px-3 py-1 text-xs text-slate-600 shadow">
          Map data © OpenStreetMap contributors
        </div>
      </main>
    </div>
  );
}
