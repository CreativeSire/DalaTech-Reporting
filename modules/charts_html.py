"""
charts_html.py — Chart generators for HTML-based PDF reports.

All functions return base64-encoded PNG strings for direct embedding in HTML.
Pass _for_print=True when generating charts for the PDF snapshot — this uses
smaller figure sizes with proportionally larger fonts so text stays legible
at the compressed display size inside the PDF layout.
"""

import io
import base64
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from matplotlib import transforms as mtransforms
import numpy as np
import pandas as pd

# ── DALA colour palette ───────────────────────────────────────────────────────
C_NAVY   = '#1B2B5E'
C_RED    = '#E8192C'
C_ACCENT = '#2E86C1'
C_GREEN  = '#1E8449'
C_AMBER  = '#C0922A'
C_GRAY   = '#DDE3ED'
C_BG     = '#FFFFFF'
C_TEXT   = '#1A1A2E'
C_MUTED  = '#7A849E'
C_GRID   = '#EAEEF5'

DPI = 220


# ── Internal helpers ──────────────────────────────────────────────────────────

def _save_base64(fig, tight_pad: float = 0.1) -> str:
    """Serialize figure to base64-encoded PNG."""
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=DPI, bbox_inches='tight',
                facecolor=C_BG, edgecolor='none', pad_inches=tight_pad)
    plt.close(fig)
    buf.seek(0)
    return f"data:image/png;base64,{base64.b64encode(buf.read()).decode('utf-8')}"


def _naira(v):
    """Compact Naira label."""
    if v >= 1_000_000:
        return f'\u20a6{v/1_000_000:.1f}M'
    if v >= 1_000:
        return f'\u20a6{v/1_000:.0f}K'
    return f'\u20a6{v:,.0f}'


def _qty(v):
    """Compact quantity label."""
    s = f'{v:.2f}'.rstrip('0').rstrip('.')
    return s


def _shorten_label(value, max_chars: int) -> str:
    """Shorten labels for dense print charts."""
    text = str(value or '').strip()
    if len(text) <= max_chars:
        return text
    clipped = text[: max_chars - 1].rstrip(' ,-/')
    return f'{clipped}...'


def _unique_short_labels(values, max_chars: int) -> list[str]:
    """Shorten labels while preserving uniqueness for chart categories."""
    counts = {}
    labels = []
    for value in values:
        base = _shorten_label(value, max_chars)
        idx = counts.get(base, 0) + 1
        counts[base] = idx
        if idx == 1:
            labels.append(base)
        else:
            suffix = f' #{idx}'
            room = max(4, max_chars - len(suffix))
            alt = _shorten_label(value, room)
            labels.append(f'{alt}{suffix}')
    return labels


def _compact_store_label(value: str, max_base_chars: int = 12, max_loc_chars: int = 8) -> str:
    """Shorten store labels for narrow print cards while keeping location context."""
    text = str(value or '').strip()
    if not text:
        return ''
    if text.startswith('Other Stores'):
        return text
    if ',' not in text:
        return _shorten_label(text, max_base_chars + 4)

    base, *rest = [part.strip() for part in text.split(',') if part.strip()]
    location = rest[-1] if rest else ''
    base = (
        base.replace('Supermarket', 'Sup')
            .replace('Market', 'Mkt')
            .replace('Square', 'Sq')
            .replace('Prince Ebeano', 'Prince E')
            .replace('Renees', 'Renees')
    )
    location = location.split()[0] if location else ''
    base_short = _shorten_label(base, max_base_chars)
    location_short = _shorten_label(location.title(), max_loc_chars) if location else ''
    if location_short:
        return f'{base_short} - {location_short}'
    return base_short


def _compact_stock_label(value: str) -> str:
    """Create a short single-line SKU label for vertical print treatment."""
    text = str(value or '').strip()
    if not text:
        return ''
    text = text.replace('(12x)', '').replace('(6x)', '').strip()
    parts = [p for p in text.split() if p]
    if not parts:
        return ''

    size = parts[0]
    stop_words = {'yoghurt', 'yogurt', 'drink', 'sweetened', 'unsweetened', 'greek'}
    descriptors = [p for p in parts[1:] if p.lower() not in stop_words]
    focus = descriptors[0] if descriptors else (parts[1] if len(parts) > 1 else '')
    if len(descriptors) >= 2 and focus.lower() in {'high', 'low'}:
        focus = f'{focus} {descriptors[1]}'

    focus = (
        focus.replace('Strawberry', 'Strawb.')
             .replace('Vanilla', 'Vanilla')
             .replace('Mango', 'Mango')
             .replace('Low Fat', 'LowFat')
        .replace('Mixed', 'Mix')
    )
    focus = _shorten_label(focus.title(), 7)
    return f'{focus} {size}'.strip() if focus else size


# ═══════════════════════════════════════════════════════════════════════════════
#  CHART FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def chart_top_stores(top_stores_df, width_in=6.0, height_in=2.5,
                     _for_print: bool = False,
                     total_store_count: int | None = None,
                     total_revenue: float | None = None) -> str:
    """Horizontal bar chart — top stores by revenue. Returns base64 PNG."""
    df = top_stores_df.copy()
    if df.empty:
        return ""

    df = df.sort_values('Revenue', ascending=False)
    if _for_print:
        actual_store_count = max(int(total_store_count or 0), len(df))
        if len(df) > 4:
            head = df.head(4).copy()
            if total_revenue is not None:
                tail_revenue = max(0.0, float(total_revenue) - float(head['Revenue'].sum()))
            else:
                tail_revenue = float(df.iloc[4:]['Revenue'].sum())
            if tail_revenue > 0:
                head.loc[len(head)] = {'Store': f'Other Stores ({actual_store_count - 4})', 'Revenue': tail_revenue}
            df = head.reset_index(drop=True)
        else:
            df = df.head(4).reset_index(drop=True)
    else:
        df = df.head(8).reset_index(drop=True)
    n = len(df)

    if _for_print:
        fig_h = max(1.12, n * 0.42)
        fig_w = 3.4
        fs_val   = 11.0
        fs_tick  = 10.2
        fs_xlab  = 8.2
        bar_h    = 0.56
        lw_grid  = 0.8
        spine_lw = 0.6
        shortened = [_compact_store_label(value, 12, 8) for value in df['Store'].tolist()]
        if len(df) > 0 and str(df.iloc[-1]['Store']).startswith('Other Stores'):
            shortened[-1] = f'Other Stores ({actual_store_count - 4})'
        df['Store'] = shortened
    else:
        fig_h, fig_w = height_in, width_in
        fs_val, fs_tick, fs_xlab = 8, 9, 9
        bar_h, lw_grid, spine_lw = 0.6, 1.0, 1.0

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    positions = np.arange(n)
    colors = []
    for i, store_name in enumerate(df['Store']):
        if i == 0:
            colors.append(C_RED)
        elif _for_print and str(store_name).startswith('Other Stores'):
            colors.append('#94A3B8')
        else:
            colors.append(C_NAVY)
    bars   = ax.barh(positions, df['Revenue'], color=colors, height=bar_h,
                     linewidth=0)
    ax.invert_yaxis()
    ax.set_yticks(positions)
    ax.set_yticklabels(df['Store'])

    max_val = max(float(df['Revenue'].max()), 1.0)
    label_transform = mtransforms.blended_transform_factory(ax.transAxes, ax.transData)
    for bar, val in zip(bars, df['Revenue']):
        if _for_print:
            ax.text(
                0.985,
                bar.get_y() + bar.get_height() / 2,
                _naira(float(val)),
                va='center',
                ha='right',
                fontsize=fs_val,
                fontweight='bold',
                color='#2A2A2A',
                transform=label_transform,
                clip_on=False,
                bbox={'facecolor': 'white', 'edgecolor': 'none', 'pad': 0.18, 'alpha': 0.96},
            )
        else:
            ax.text(
                float(val) + max_val * 0.015,
                bar.get_y() + bar.get_height() / 2,
                _naira(float(val)),
                va='center',
                ha='left',
                fontsize=fs_val,
                fontweight='bold',
                color='#333',
            )

    ax.set_xlim(0, max_val * (1.15 if _for_print else 1.28))
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _naira(x)))

    for sp in ['top', 'right', 'left']:
        ax.spines[sp].set_visible(False)
    ax.spines['bottom'].set_linewidth(spine_lw)
    ax.spines['bottom'].set_color(C_GRAY)

    ax.tick_params(axis='y', length=0, labelsize=fs_tick, pad=3)
    if _for_print:
        ax.tick_params(axis='x', labelbottom=False, length=0)
        ax.set_xlabel('')
        ax.margins(y=0.16)
    else:
        ax.tick_params(axis='x', labelsize=max(fs_xlab - 1, 6), colors=C_MUTED, length=2)
        ax.set_xlabel('Revenue', fontsize=fs_xlab, color=C_MUTED)
    ax.grid(axis='x', alpha=0.35, color=C_GRAY, linewidth=lw_grid)

    plt.tight_layout(pad=0.3 if _for_print else 1.0)
    return _save_base64(fig, tight_pad=0.04 if _for_print else 0.1)


def chart_product_qty(product_qty_df, width_in=3.0, height_in=2.2,
                      _for_print: bool = False) -> str:
    """Horizontal bar chart — top SKUs by quantity."""
    df = product_qty_df.head(6 if not _for_print else 5).copy()
    if df.empty:
        return ""

    n = len(df)
    df = df.iloc[::-1].reset_index(drop=True)

    if _for_print:
        fig_h = max(0.85, n * 0.28)
        fig_w = 3.4
        fs_val, fs_tick, fs_xlab = 10, 9, 8
        bar_h = 0.55
    else:
        fig_h, fig_w = height_in, width_in
        fs_val, fs_tick, fs_xlab = 8, 8, 8
        bar_h = 0.6

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    bars = ax.barh(df['SKU'], df['Quantity'], color=C_ACCENT, height=bar_h,
                   linewidth=0)

    max_val = max(df['Quantity'])
    for bar, val in zip(bars, df['Quantity']):
        ax.text(val + max_val * 0.02, bar.get_y() + bar.get_height() / 2,
                _qty(val), va='center', ha='left', fontsize=fs_val, color='#333')

    ax.set_xlim(0, max_val * 1.3)
    for sp in ['top', 'right', 'left']:
        ax.spines[sp].set_visible(False)
    ax.spines['bottom'].set_color(C_GRAY)
    ax.tick_params(axis='y', length=0, labelsize=fs_tick)
    ax.tick_params(axis='x', labelsize=max(fs_xlab - 1, 6), colors=C_MUTED)
    ax.set_xlabel('Carton Packs', fontsize=fs_xlab, color=C_MUTED)
    ax.grid(axis='x', alpha=0.35, color=C_GRAY)

    plt.tight_layout(pad=0.3 if _for_print else 1.0)
    return _save_base64(fig, tight_pad=0.04 if _for_print else 0.1)


def chart_product_value(product_value_df, width_in=3.0, height_in=2.2,
                        _for_print: bool = False) -> str:
    """Horizontal bar chart — top SKUs by revenue."""
    df = product_value_df.sort_values('Revenue', ascending=False).head(6 if not _for_print else 5).copy()
    if df.empty:
        return ""

    n = len(df)
    df = df.reset_index(drop=True)

    if _for_print:
        fig_h = max(1.08, n * 0.38)
        fig_w = 3.4
        fs_val, fs_tick, fs_xlab = 10.8, 10.1, 8.2
        bar_h = 0.54
        df['SKU'] = _unique_short_labels(df['SKU'].tolist(), 16)
    else:
        fig_h, fig_w = height_in, width_in
        fs_val, fs_tick, fs_xlab = 8, 8, 8
        bar_h = 0.6

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    positions = np.arange(n)
    colors = [C_RED] * n
    bars = ax.barh(positions, df['Revenue'], color=colors, height=bar_h, linewidth=0)
    ax.invert_yaxis()
    ax.set_yticks(positions)
    ax.set_yticklabels(df['SKU'])

    max_val = max(float(df['Revenue'].max()), 1.0)
    label_transform = mtransforms.blended_transform_factory(ax.transAxes, ax.transData)
    for bar, val in zip(bars, df['Revenue']):
        if _for_print:
            ax.text(
                0.985,
                bar.get_y() + bar.get_height() / 2,
                _naira(float(val)),
                va='center',
                ha='right',
                fontsize=fs_val,
                fontweight='bold',
                color='#2A2A2A',
                transform=label_transform,
                clip_on=False,
                bbox={'facecolor': 'white', 'edgecolor': 'none', 'pad': 0.18, 'alpha': 0.96},
            )
        else:
            ax.text(
                float(val) + max_val * 0.015,
                bar.get_y() + bar.get_height() / 2,
                _naira(float(val)),
                va='center',
                ha='left',
                fontsize=fs_val,
                fontweight='bold',
                color='#333',
            )

    ax.set_xlim(0, max_val * (1.16 if _for_print else 1.28))
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _naira(x)))

    for sp in ['top', 'right', 'left']:
        ax.spines[sp].set_visible(False)
    ax.spines['bottom'].set_color(C_GRAY)
    ax.spines['bottom'].set_linewidth(0.6 if _for_print else 1.0)

    ax.tick_params(axis='y', length=0, labelsize=fs_tick, pad=3)
    if _for_print:
        ax.tick_params(axis='x', labelbottom=False, length=0)
        ax.set_xlabel('')
        ax.margins(y=0.16)
    else:
        ax.tick_params(axis='x', labelsize=max(fs_xlab - 1, 6), colors=C_MUTED, length=2)
        ax.set_xlabel('Revenue', fontsize=fs_xlab, color=C_MUTED)
    ax.grid(axis='x', alpha=0.35, color=C_GRAY)

    plt.tight_layout(pad=0.3 if _for_print else 1.0)
    return _save_base64(fig, tight_pad=0.04 if _for_print else 0.1)


def chart_daily_trend(daily_sales_df, width_in=6.5, height_in=1.8) -> str:
    """Area + line chart — daily revenue trend (interactive dashboard use)."""
    df = daily_sales_df.copy()
    if df.empty or len(df) < 2:
        return ""

    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')

    fig, ax = plt.subplots(figsize=(width_in, height_in))

    x = df['Date'].values
    y = df['Revenue'].values

    ax.fill_between(x, y, alpha=0.15, color=C_NAVY)
    ax.plot(x, y, color=C_NAVY, linewidth=2, marker='o', markersize=4)

    peak_idx = int(np.argmax(y))
    ax.scatter(x[peak_idx], y[peak_idx], color=C_RED, s=60, zorder=5)

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: _naira(v)))

    n_days = (df['Date'].iloc[-1] - df['Date'].iloc[0]).days + 1
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2 if n_days <= 14 else 5))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha='right', fontsize=7)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color(C_GRAY)
    ax.spines['bottom'].set_color(C_GRAY)
    ax.tick_params(axis='y', labelsize=7, colors=C_MUTED)
    ax.grid(axis='y', alpha=0.3, color=C_GRAY)

    plt.tight_layout()
    return _save_base64(fig)


def chart_reorder(reorder_df, width_in=6.0, height_in=1.7) -> str:
    """Horizontal bar chart — stores by order count."""
    df = reorder_df.copy()
    if df.empty:
        return ""

    df = df.head(8).iloc[::-1].reset_index(drop=True)

    fig, ax = plt.subplots(figsize=(width_in, height_in))

    colors = [C_GREEN if status == 'Repeat Customer' else C_AMBER
              for status in df['Status']]

    bars = ax.barh(df['Store'], df['Order Count'], color=colors, height=0.6)

    max_orders = df['Order Count'].max()
    for bar, val in zip(bars, df['Order Count']):
        label = f'{int(val)} order{"s" if val != 1 else ""}'
        ax.text(val + max_orders * 0.02, bar.get_y() + bar.get_height() / 2,
                label, va='center', ha='left', fontsize=7)

    ax.set_xlim(0, max_orders * 1.3)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    for sp in ['top', 'right', 'left']:
        ax.spines[sp].set_visible(False)
    ax.tick_params(axis='y', length=0, labelsize=6.5)
    ax.tick_params(axis='x', labelsize=6.5, colors=C_MUTED)
    ax.set_xlabel('Number of Orders', fontsize=7, color=C_MUTED)
    ax.grid(axis='x', alpha=0.3, color=C_GRAY)

    plt.tight_layout(pad=0.2)
    return _save_base64(fig)


def chart_store_heatmap(store_heatmap_df, width_in=6.2, height_in=1.5) -> str:
    """Matplotlib heatmap — top stores (rows) x dates (columns)."""
    import matplotlib.colors as mcolors
    df = store_heatmap_df.copy()
    if df.empty:
        return ""

    df['Date'] = pd.to_datetime(df['Date'])
    pivot = df.pivot_table(index='Store', columns='Date', values='Orders',
                           fill_value=0, aggfunc='sum')
    pivot = pivot.reindex(sorted(pivot.columns), axis=1)
    store_rank = pivot.sum(axis=1).sort_values(ascending=False)
    pivot = pivot.loc[store_rank.head(8).index]

    n_stores = len(pivot)
    n_dates  = len(pivot.columns)
    height_in = max(1.25, min(1.7, n_stores * 0.12 + 0.65))

    fig, ax = plt.subplots(figsize=(width_in, height_in))

    cmap = mcolors.LinearSegmentedColormap.from_list(
        'dala', ['#F4F6FA', '#A8B8D8', C_NAVY]
    )
    ax.imshow(pivot.values, aspect='auto', cmap=cmap,
              vmin=0, vmax=max(pivot.values.max(), 1))

    ax.set_yticks(range(n_stores))
    ax.set_yticklabels(
        [s[:20] + ('…' if len(s) > 20 else '') for s in pivot.index],
        fontsize=5.8, color=C_TEXT
    )

    date_labels = [d.strftime('%d %b') for d in pivot.columns]
    tick_step = max(1, n_dates // 10)
    ax.set_xticks(range(0, n_dates, tick_step))
    ax.set_xticklabels(date_labels[::tick_step], rotation=45, ha='right', fontsize=5.6)
    ax.tick_params(axis='both', length=0)

    for spine in ax.spines.values():
        spine.set_visible(False)

    plt.tight_layout(pad=0.15)
    return _save_base64(fig)


def chart_dual_trend(daily_sales_df, width_in=6.5, height_in=2.0,
                     _for_print: bool = False) -> str:
    """
    Dual-axis trend: filled area = revenue (left axis), dashed line = quantity (right axis).
    _for_print=True uses print-optimised sizing so text is legible at PDF scale.
    """
    df = daily_sales_df.copy()
    if df.empty or len(df) < 2:
        return ""

    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')

    if _for_print:
        fig_w, fig_h = 5.5, 1.15
        fs_tick  = 11
        fs_yleft = 11
        fs_yright= 11
        fs_legend= 8
        lw_rev   = 2.8    # revenue line weight
        lw_qty   = 2.0    # quantity line weight
        ms_peak  = 40     # peak marker size
        alpha_fill = 0.22
    else:
        fig_w, fig_h = width_in, height_in
        fs_tick  = 7
        fs_yleft = 7
        fs_yright= 7
        fs_legend= 7
        lw_rev   = 1.8
        lw_qty   = 1.6
        ms_peak  = 40
        alpha_fill = 0.18

    fig, ax1 = plt.subplots(figsize=(fig_w, fig_h))
    ax2 = ax1.twinx()

    rev = df['Revenue'].values
    qty = df['Quantity'].values

    # Revenue — bold filled area (primary story)
    ax1.fill_between(df['Date'], rev, alpha=alpha_fill, color=C_RED, zorder=2)
    ax1.plot(df['Date'], rev, color=C_RED, linewidth=lw_rev,
             zorder=3, label='Revenue', solid_capstyle='round')

    # Peak dot
    peak_idx = int(np.argmax(rev))
    ax1.scatter(df['Date'].iloc[peak_idx], rev[peak_idx],
                color=C_RED, s=ms_peak, zorder=5,
                edgecolors='white', linewidths=1.2)

    # Quantity — subtle secondary line
    ax2.plot(df['Date'], qty, color=C_NAVY, linewidth=lw_qty,
             linestyle='--', zorder=2, alpha=0.75, label='Quantity')

    # ── Y-axis formatting ──────────────────────────────────────────────────
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: _naira(v)))
    ax1.tick_params(axis='y', labelsize=fs_yleft, colors='#888', length=2)
    ax2.tick_params(axis='y', labelsize=fs_yright, colors='#888', length=2)
    ax2.set_ylabel('Qty', fontsize=fs_yright, color='#AAA', labelpad=2)

    # Tidy up right y-axis ticks to integer packs
    ax2.yaxis.set_major_locator(mticker.MaxNLocator(nbins=4, integer=True))

    # ── X-axis dates ──────────────────────────────────────────────────────
    if _for_print:
        tick_count = min(5, len(df))
        tick_idx = sorted(set(int(round(i)) for i in np.linspace(0, len(df) - 1, tick_count)))
        tick_dates = [df['Date'].iloc[i] for i in tick_idx]
        ax1.set_xticks(tick_dates)
    else:
        n_days = len(df)
        interval = max(1, n_days // 6)
        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=interval))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
    plt.setp(ax1.xaxis.get_majorticklabels(),
             rotation=0, ha='center', fontsize=fs_tick)

    # ── Spines ────────────────────────────────────────────────────────────
    ax1.spines['top'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    ax1.spines['left'].set_color('#DDD')
    ax1.spines['left'].set_linewidth(0.8)
    ax1.spines['bottom'].set_color('#DDD')
    ax1.spines['bottom'].set_linewidth(0.8)
    ax1.spines['right'].set_visible(False)
    ax2.spines['left'].set_visible(False)
    ax2.spines['right'].set_color('#DDD')
    ax2.spines['right'].set_linewidth(0.8)
    ax2.spines['bottom'].set_visible(False)

    # ── Grid ──────────────────────────────────────────────────────────────
    ax1.grid(axis='y', alpha=0.35, color='#EEE', linewidth=0.7, zorder=1)
    ax1.set_axisbelow(True)

    # ── Legend ────────────────────────────────────────────────────────────
    if not _for_print:
        h1, l1 = ax1.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax1.legend(h1 + h2, l1 + l2,
                   loc='upper right', fontsize=fs_legend,
                   framealpha=0.9, edgecolor='#EEE',
                   ncol=2, handlelength=1.5, columnspacing=1.0,
                   borderpad=0.4, labelspacing=0.3)

    plt.tight_layout(pad=0.4 if _for_print else 0.5)
    return _save_base64(fig, tight_pad=0.04 if _for_print else 0.1)


def chart_stock_vertical(closing_stock_df, width_in=1.8, height_in=3.0,
                         _for_print: bool = False) -> str:
    """Vertical bar chart — current stock level by SKU."""
    df = closing_stock_df.copy()
    if df.empty:
        return ""

    df = df.sort_values('Closing Stock (Cartons)', ascending=False).head(4 if _for_print else 10)

    if _for_print:
        n = len(df)
        fig_w = 1.28
        fig_h = 1.24
        fs_val   = 8.8
        fs_tick  = 5.5
        fs_ytick = 7.0
        bar_w    = 0.5
        lw_v     = 0.7
    else:
        fig_w, fig_h = width_in, height_in
        fs_val, fs_tick, fs_ytick = 6.5, 5.5, 6.5
        bar_w, lw_v = 0.65, 1.0

    labels = [_compact_stock_label(v) for v in df['SKU'].tolist()] if _for_print else _unique_short_labels(df['SKU'].tolist(), 12)
    vals   = df['Closing Stock (Cartons)'].values

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    bars = ax.bar(range(len(vals)), vals, color=C_NAVY, width=bar_w, linewidth=0)

    for bar, val in zip(bars, vals):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(vals) * 0.015,
            f'{val:.0f}',
            ha='center', va='bottom',
            fontsize=fs_val, fontweight='bold', color=C_NAVY,
        )

    ax.set_xticks(range(len(vals)))
    ax.set_xticklabels(
        labels,
        rotation=90 if _for_print else 90,
        fontsize=fs_tick,
        color=C_TEXT,
        ha='center',
        va='top',
    )
    ax.set_ylim(0, max(vals) * (1.20 if _for_print else 1.28))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:.0f}'))
    ax.tick_params(axis='y', labelsize=fs_ytick, colors=C_MUTED, length=2)
    ax.tick_params(axis='x', length=0)

    for sp in ['top', 'right', 'left']:
        ax.spines[sp].set_visible(False)
    ax.spines['bottom'].set_color(C_GRAY)
    ax.spines['bottom'].set_linewidth(lw_v)
    ax.grid(axis='y', alpha=0.3, color=C_GRID)

    if _for_print:
        ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=3, integer=True))
        plt.tight_layout(pad=0.20, rect=(0, 0.18, 1, 1))
    else:
        plt.tight_layout(pad=0.4)
    return _save_base64(fig, tight_pad=0.04 if _for_print else 0.1)


def chart_sparkline(daily_sales_df, width_in=1.5, height_in=0.4) -> str:
    """Mini sparkline for KPI card (interactive dashboard only)."""
    df = daily_sales_df.copy()
    if df.empty or len(df) < 2:
        return ""

    df = df.sort_values('Date')
    fig, ax = plt.subplots(figsize=(width_in, height_in))

    x = range(len(df))
    y = df['Revenue'].values

    ax.fill_between(x, y, alpha=0.3, color=C_NAVY)
    ax.plot(x, y, color=C_NAVY, linewidth=1.5)
    ax.axis('off')
    ax.set_ylim(0, max(y) * 1.1)

    plt.tight_layout(pad=0)
    return _save_base64(fig)


def chart_weekly_bars(weekly_pct, color=C_NAVY, width_in=2.0, height_in=0.7) -> str:
    """4-bar mini chart showing week-by-week % contribution (interactive dashboard)."""
    if not weekly_pct or all(v == 0 for v in weekly_pct):
        return ""

    weeks = ['W1', 'W2', 'W3', 'W4']
    vals  = list(weekly_pct)[:4]

    fig, ax = plt.subplots(figsize=(width_in, height_in))
    fig.patch.set_facecolor('none')
    ax.set_facecolor('none')

    bar_colors = [C_RED if v == max(vals) else color for v in vals]
    bars = ax.bar(weeks, vals, color=bar_colors, width=0.6, linewidth=0)

    for bar, pct in zip(bars, vals):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.5,
            f'{pct:.0f}%',
            ha='center', va='bottom',
            fontsize=6.5, fontweight='bold', color=C_TEXT,
        )

    ax.set_ylim(0, max(vals) * 1.55)
    ax.axis('off')
    plt.tight_layout(pad=0.1)
    return _save_base64(fig)
