import { useMemo, useState } from "react";

export function VirtualList({
  items,
  rowHeight = 36,
  height = 380,
  overscan = 8,
  className = "",
  renderItem,
}) {
  const [scrollTop, setScrollTop] = useState(0);
  const totalHeight = items.length * rowHeight;
  const visibleCount = Math.ceil(height / rowHeight);
  const startIndex = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
  const endIndex = Math.min(items.length, startIndex + visibleCount + overscan * 2);

  const visibleItems = useMemo(() => items.slice(startIndex, endIndex), [items, startIndex, endIndex]);

  return (
    <div
      className={`virtual-list ${className}`.trim()}
      style={{ height, overflowY: "auto" }}
      onScroll={(e) => setScrollTop(e.currentTarget.scrollTop)}
    >
      <div style={{ height: totalHeight, position: "relative" }}>
        <div style={{ transform: `translateY(${startIndex * rowHeight}px)` }}>
          {visibleItems.map((item, idx) => renderItem(item, startIndex + idx))}
        </div>
      </div>
    </div>
  );
}
