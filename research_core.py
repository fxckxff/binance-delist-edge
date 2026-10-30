"""Shared research helpers. These models do not establish executable returns."""
import random
import statistics


def cluster_bootstrap(rows, n=2000, seed=42):
    """Resample announcement clusters, preserving all symbols in each cluster."""
    groups = {}
    for cluster, value in rows:
        groups.setdefault(cluster, []).append(value)
    if len(groups) < 2:
        return None
    rng = random.Random(seed)
    groups = list(groups.values())
    sums = [sum(g) for g in groups]
    sizes = [len(g) for g in groups]
    means = []
    for _ in range(n):
        indices = rng.choices(range(len(groups)), k=len(groups))
        means.append(sum(sums[i] for i in indices) / sum(sizes[i] for i in indices))
    means.sort()
    return means[int(.025*n)], means[min(n-1, int(.975*n))]


def portfolio(events, simulate, stop, cost, equity0=1000., risk=.01, leverage=3.):
    """One position at a time; skip overlaps, including simultaneous announcements.

    Deterministic symbol order is a research convention, not an optimized selection.
    Statistics of independent event returns must be reported separately.
    """
    trades, skipped = [], {}
    equity = peak = equity0
    drawdown = 0.
    busy_until = -1
    for ev in sorted(events, key=lambda e: (e['release_ms'], e['symbol'])):
        if equity <= 0:
            reason = 'account_blown'
        else:
            r = simulate(ev)
            reason = r if isinstance(r, str) else None
            if reason is None and r['entry_t'] <= busy_until:
                reason = 'overlapping_position'
        if reason:
            skipped[reason] = skipped.get(reason, 0) + 1
            continue
        notional = min(equity * risk / ((stop + cost) / 10000), equity * leverage) if stop else equity * .5
        pnl = max(-equity, notional * r['net_bps'] / 10000)
        trades.append({**r, 'symbol': ev['symbol'], 'release_ms': ev['release_ms'],
                       'notional': notional, 'pnl': pnl})
        busy_until = r['exit_t']
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, (peak-equity)/peak)
    if not trades:
        return None
    nets = [t['net_bps'] for t in trades]
    total = equity-equity0
    return dict(trades=len(trades), skipped=skipped,
                win_rate=sum(x > 0 for x in nets)/len(nets), median_net_bps=statistics.median(nets),
                mean_net_bps=statistics.mean(nets), total_pnl_usd=round(total, 2),
                final_equity=round(equity, 2), max_dd_pct=round(drawdown*100, 2),
                avg_notional=statistics.mean(t['notional'] for t in trades),
                top5_share_pct=round(sum(sorted((t['pnl'] for t in trades), reverse=True)[:5])/total*100, 1) if total else 0.,
                account_blown=equity <= 0)
