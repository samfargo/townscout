import React from 'react';

interface SparklineProps {
  data: number[];
  width?: number;
  height?: number;
  color?: string;
  fillColor?: string;
  type?: 'line' | 'bar';
  className?: string;
  showMonthLabels?: boolean;
}

const MONTH_LABELS = ['J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D'];

export function Sparkline({
  data,
  width = 100,
  height = 24,
  color = '#78716c',
  fillColor = '#d6d3d1',
  type = 'line',
  className = '',
  showMonthLabels = false
}: SparklineProps) {
  if (!data || data.length === 0) {
    return null;
  }

  const validData = data.filter((val) => typeof val === 'number' && Number.isFinite(val));
  if (validData.length === 0) {
    return <div className={className} style={{ width, height }} />;
  }

  const min = Math.min(...validData);
  const max = Math.max(...validData);
  const range = max - min || 1;

  const padding = 2;
  const usableHeight = height - padding * 2;
  const usableWidth = width - padding * 2;

  if (type === 'bar') {
    const barWidth = usableWidth / data.length;
    const bars = data.map((val, i) => {
      if (typeof val !== 'number' || !Number.isFinite(val)) {
        return null;
      }
      const barHeight = ((val - min) / range) * usableHeight;
      const x = padding + i * barWidth;
      const y = padding + usableHeight - barHeight;
      return (
        <rect
          key={i}
          x={x}
          y={y}
          width={Math.max(barWidth - 1, 1)}
          height={barHeight}
          fill={fillColor}
          stroke={color}
          strokeWidth="0.5"
        />
      );
    });

    const labelHeight = showMonthLabels ? 12 : 0;
    const totalHeight = height + labelHeight;

    return (
      <svg width={width} height={totalHeight} className={className} viewBox={`0 0 ${width} ${totalHeight}`}>
        <g transform={`translate(0, 0)`}>
          {bars}
        </g>
        {showMonthLabels && (
          <g transform={`translate(0, ${height})`}>
            {MONTH_LABELS.map((label, i) => {
              const x = padding + i * barWidth + barWidth / 2;
              return (
                <text
                  key={i}
                  x={x}
                  y={10}
                  textAnchor="middle"
                  fontSize="8"
                  fill={color}
                  className="select-none"
                >
                  {label}
                </text>
              );
            })}
          </g>
        )}
      </svg>
    );
  }

  // Line type
  const points = data
    .map((val, i) => {
      if (typeof val !== 'number' || !Number.isFinite(val)) {
        return null;
      }
      const x = padding + (i / (data.length - 1)) * usableWidth;
      const y = padding + usableHeight - ((val - min) / range) * usableHeight;
      return `${x},${y}`;
    })
    .filter(Boolean)
    .join(' ');

  // Create filled area path
  const firstPoint = data.findIndex((val) => typeof val === 'number' && Number.isFinite(val));
  const lastPoint = data.length - 1 - [...data].reverse().findIndex((val) => typeof val === 'number' && Number.isFinite(val));
  
  const areaPath = points
    ? `M ${padding + (firstPoint / (data.length - 1)) * usableWidth},${padding + usableHeight} L ${points.split(' ')[0]} L ${points.split(' ').join(' L ')} L ${padding + (lastPoint / (data.length - 1)) * usableWidth},${padding + usableHeight} Z`
    : '';

  const labelHeight = showMonthLabels ? 12 : 0;
  const totalHeight = height + labelHeight;
  const barWidth = usableWidth / (data.length - 1);

  return (
    <svg width={width} height={totalHeight} className={className} viewBox={`0 0 ${width} ${totalHeight}`}>
      <g transform={`translate(0, 0)`}>
        {areaPath && <path d={areaPath} fill={fillColor} opacity="0.3" />}
        <polyline
          points={points}
          fill="none"
          stroke={color}
          strokeWidth="1.5"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </g>
      {showMonthLabels && (
        <g transform={`translate(0, ${height})`}>
          {MONTH_LABELS.map((label, i) => {
            const x = padding + i * barWidth;
            return (
              <text
                key={i}
                x={x}
                y={10}
                textAnchor="middle"
                fontSize="8"
                fill={color}
                className="select-none"
              >
                {label}
              </text>
            );
          })}
        </g>
      )}
    </svg>
  );
}

