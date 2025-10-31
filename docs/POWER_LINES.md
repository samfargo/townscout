We’re adding a new livability filter to vicinity: “Avoid power lines (high-voltage transmission corridors)”.

User behavior
	•	Add a toggle in the UI called “Avoid power lines (high-voltage transmission corridors)”.
	•	When this toggle is ON, any hex that lies within 200 meters of a major overhead transmission line should be excluded from the map hex shading.
	•	Default is OFF.

Data source
	•	Use OpenStreetMap power infrastructure data. We consider “major overhead transmission lines” to be power=line features carrying high voltage (≈100 kV and up).
	•	Below is an Overpass query that returns those high-voltage transmission lines in Massachusetts. It filters:

[out:json][timeout:90];
rel["boundary"="administrative"]["admin_level"="4"]["name"="Massachusetts"];
map_to_area->.ma;

(
  way
    ["power"="line"]
    ["voltage"~"(^1[01][0-9] ?kV$|^[12-9][0-9]{4,}$|kV)"]
    (area.ma)
    (if:length() > 10);
  relation
    ["power"="line"]
    ["voltage"~"(^1[01][0-9] ?kV$|^[12-9][0-9]{4,}$|kV)"]
    (area.ma);
);
out body;
>;
out skel qt;

What I want built
	1.	Add an offline data step (similar to our other make d_* steps) that:
	•	runs an Overpass query (or whatever method is most effcient for aquiring the data),
	•	exports the result as line geometry (GeoJSON or shapefile - whichever fits the architecture better),
	•	buffers each line by 200 meters,
	•	unions / dissolves those buffers,
	•	intersects that buffer with our H3 hex grid,
	•	and produces a boolean column on each hex like near_power_corridor = true.
	2.	Plumb that boolean into the H3 attributes we serve to the frontend tiles / API so that, for any given hex, we know if it’s within 200 m of a high-voltage corridor.
	3.	In the frontend:
	•	Add the “Avoid power lines” toggle to the sidebar alongside other criteria.
	•	When it’s enabled, unshade hexes where near_power_corridor === true.

Key expectations
	•	Do not use distribution lines (power=minor_line) or local poles.
	•	We are only trying to avoid large visible transmission corridors people don’t want in their backyard.
	•	Assume buffer distance is 200 meters for now; make it easy to change later.

Deliverables:
	•	ETL code / script to generate near_power_corridor for all hexes.
	•	Update to the data schema so that flag is available in tiles or via API.
	•	Frontend change that consumes that flag and filters results when the toggle is ON.

Please implement this end to end in an organized fashion. Ensure you reuse code where applicable, and do not create redunancies.