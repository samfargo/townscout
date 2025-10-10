// Reference data for TownScout climate typology.

export type ClimateTypologyEntry = {
  label: string;
  criteria: string;
  meaning: string;
  examples: string;
};

export const CLIMATE_TYPOLOGY: ClimateTypologyEntry[] = [
  {
    label: "Arctic Cold",
    criteria: "Summer < 60°F and winter < 30°F",
    meaning: "Long, frigid winters with short, cool summers.",
    examples: "Interior Alaska, Northern Rockies peaks"
  },
  {
    label: "Cold Seasonal",
    criteria: "Summer ≥ 65°F, winter < 32°F, precip > 20\"",
    meaning: "Hot summers and snowy winters with plenty of moisture.",
    examples: "Upper Midwest, Northern New England"
  },
  {
    label: "Mild Continental",
    criteria: "Summer 70–80°F, winter 32–45°F, precip 25–50\"",
    meaning: "Warm summers, chilly winters, and distinct seasons.",
    examples: "Midwest, Northeast"
  },
  {
    label: "Cool Maritime",
    criteria: "Summer < 70°F, winter > 35°F, precip > 35\"",
    meaning: "Mild year-round with damp, overcast stretches.",
    examples: "Pacific Northwest coast"
  },
  {
    label: "Warm Humid",
    criteria: "Summer > 80°F, winter > 45°F, precip > 40\"",
    meaning: "Hot, sticky summers and gentle winters.",
    examples: "Deep South, Southeast"
  },
  {
    label: "Hot Dry (Desert)",
    criteria: "Summer > 80°F, precip < 10\"",
    meaning: "Extremely hot with very little rainfall.",
    examples: "Arizona deserts, Southern Nevada"
  },
  {
    label: "Warm Semi-Arid",
    criteria: "Summer 75–85°F, precip 10–20\"",
    meaning: "Hot and dry most of the year with a short wet season.",
    examples: "Texas Panhandle, Inland California valleys"
  },
  {
    label: "Mediterranean Mild",
    criteria: "Summer > 75°F, precip < 30\", winter wetter than summer",
    meaning: "Dry, sunny summers followed by mild, wet winters.",
    examples: "Coastal California"
  },
  {
    label: "Mountain Mixed",
    criteria: "Summer 60–75°F, winter < 35°F",
    meaning: "Wide seasonal swings with cooler temperatures overall.",
    examples: "Rocky Mountains, Appalachian highlands"
  }
];
