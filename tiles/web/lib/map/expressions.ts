export type MapExpression = any[];

export function buildMinutesExpression(
  anchorMap: Record<string, number>,
  maxMinutes: number
): MapExpression {
  const UNREACHABLE = 65535;
  const maxSeconds = maxMinutes * 60;

  // Build match expression for anchor lookup
  // Format: ['match', expression, label1, output1, label2, output2, ..., fallback]
  const anchorIds = Object.keys(anchorMap);
  const matchArgs: (string | number)[] = [];
  
  for (const anchorId of anchorIds) {
    const parsedId = parseInt(anchorId, 10);
    const seconds = Number(anchorMap[anchorId]);
    if (!Number.isFinite(parsedId)) {
      continue;
    }
    if (!Number.isFinite(seconds)) {
      continue;
    }
    matchArgs.push(parsedId);  // Match key (as number)
    matchArgs.push(seconds);   // Match value in seconds
  }

  const terms: MapExpression[] = [];

  for (let i = 0; i < 20; i++) {
    const hexToAnchorSec: MapExpression = [
      'coalesce',
      ['get', `a${i}_s`],
      UNREACHABLE
    ];
    
    // Use match expression to look up anchor ID
    const anchorToPoiSec: MapExpression = [
      'match',
      ['get', `a${i}_id`],
      ...matchArgs,
      UNREACHABLE  // fallback
    ];

    terms.push(['+', hexToAnchorSec, anchorToPoiSec]);
  }

  return ['<=', ['min', ...terms], maxSeconds];
}
