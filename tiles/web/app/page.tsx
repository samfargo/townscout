// Composes the map and sidebar into the homepage shell.
import dynamic from "next/dynamic";

import Sidebar from "@/app/(sidebar)/Sidebar";
import HoverBox from "@/app/(sidebar)/HoverBox";

const MapCanvas = dynamic(() => import("@/app/(map)/MapCanvas"), { ssr: false });

export default function HomePage() {
  return (
    <div className="flex h-screen min-h-0 w-full overflow-hidden">
      <Sidebar />
      <main className="relative flex-1 min-h-0 overflow-hidden">
        <div className="absolute inset-0">
          <MapCanvas />
        </div>
        <div className="pointer-events-none absolute inset-0 z-10">
          <div className="pointer-events-auto absolute bottom-6 right-6">
            <HoverBox />
          </div>
        </div>
        <div className="pointer-events-none absolute bottom-4 left-1/2 w-max -translate-x-1/2 rounded-full bg-white/80 px-3 py-1 text-xs text-slate-600 shadow">
          Map data © OpenStreetMap contributors
        </div>
      </main>
    </div>
  );
}
