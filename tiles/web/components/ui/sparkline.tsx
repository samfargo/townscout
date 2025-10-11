import React from 'react';

interface SparklineProps {
  data: number[];
  width?: number;
  height?: number;
  color?: string;
  fillColor?: string;
  type?: 'line' | 'bar';
  className?: string;
}

export function Sparkline({
  data,
  width = 100,
  height = 24,
  color = '#78716c',
  fillColor = '#d6d3d1',
  type = 'line',
  className = ''
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

    return (
      <svg width={width} height={height} className={className} viewBox={`0 0 ${width} ${height}`}>
        {bars}
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

  return (
    <svg width={width} height={height} className={className} viewBox={`0 0 ${width} ${height}`}>
      {areaPath && <path d={areaPath} fill={fillColor} opacity="0.3" />}
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

