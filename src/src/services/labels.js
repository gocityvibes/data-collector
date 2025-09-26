export function labelReversals(candlesAsc, reversalsNewestFirst, opts = {}) {
  const {
    tick = 1,
    confirmPoints = 8,
    confirmBars = 12,
    stopPoints = 8,
    minStrongPoints = 10,
    failBackPoints = 6,
    failBars = 8
  } = opts;

  const out = [];
  const tIdx = candlesAsc.map(c => Date.parse(c.ts_utc));
  const idxAtOrAfter = (ts) => {
    const t = Date.parse(ts);
    for (let i = 0; i < tIdx.length; i++) if (tIdx[i] >= t) return i;
    return -1;
  };

  for (const r of reversalsNewestFirst) {
    const i0 = idxAtOrAfter(r.ts_utc);
    if (i0 < 0) { out.push({ ...r, label: null }); continue; }

    const pivotPrice = r.direction === "up"
      ? Number(candlesAsc[i0].low)
      : Number(candlesAsc[i0].high);

    let mfe = 0, mae = 0, barsToMfe = 0, barsToMae = 0;
    const end = Math.min(candlesAsc.length - 1, i0 + Math.max(confirmBars, failBars));

    for (let j = i0 + 1; j <= end; j++) {
      const hi = Number(candlesAsc[j].high);
      const lo = Number(candlesAsc[j].low);
      const fav = r.direction === "up" ? (hi - pivotPrice) : (pivotPrice - lo);
      const adv = r.direction === "up" ? (pivotPrice - lo) : (hi - pivotPrice);
      if (fav > mfe) { mfe = fav; barsToMfe = j - i0; }
      if (adv > mae) { mae = adv; barsToMae = j - i0; }
    }

    const mfePts = Math.round(mfe / tick);
    const maePts = Math.round(mae / tick);
    const strong = Number(r.points ?? 0) >= minStrongPoints;

    let label = null, label_reason = null;
    if (mfePts >= confirmPoints && maePts <= stopPoints && barsToMfe <= confirmBars) {
      label = "gold";
      label_reason = `mfe=${mfePts}>=${confirmPoints} within ${barsToMfe} bars, mae=${maePts}<=${stopPoints}`;
    } else if (strong && mfePts < confirmPoints && maePts >= failBackPoints && barsToMae <= failBars) {
      label = "hard_negative";
      label_reason = `strong=${r.points} ticks, mae=${maePts}>=${failBackPoints} within ${barsToMae} bars`;
    }

    out.push({
      ...r,
      mfe_points: mfePts,
      mae_points: maePts,
      bars_to_mfe: barsToMfe,
      bars_to_mae: barsToMae,
      label,
      label_reason
    });
  }
  return out;
}

export function pd_labelOne(candlesAsc, rev, { tick, confirmPoints = 8, confirmBars = 12, stopPoints = 8, minStrongPoints = 10, failBackPoints = 6, failBars = 8 }) {
  const tRev = Date.parse(rev.ts_utc);
  let i0 = -1;
  for (let i = 0; i < candlesAsc.length; i++) {
    if (Date.parse(candlesAsc[i].ts_utc) >= tRev) { i0 = i; break; }
  }
  if (i0 < 0) return { label: null, mfePts: 0, maePts: 0 };

  const pivotPrice = rev.direction === "up" ? Number(candlesAsc[i0].low) : Number(candlesAsc[i0].high);
  let mfe = 0, mae = 0, barsToMfe = 0, barsToMae = 0;
  const end = Math.min(candlesAsc.length - 1, i0 + Math.max(confirmBars, failBars));

  for (let j = i0 + 1; j <= end; j++) {
    const hi = Number(candlesAsc[j].high);
    const lo = Number(candlesAsc[j].low);
    const fav = rev.direction === "up" ? (hi - pivotPrice) : (pivotPrice - lo);
    const adv = rev.direction === "up" ? (pivotPrice - lo) : (hi - pivotPrice);
    if (fav > mfe) { mfe = fav; barsToMfe = j - i0; }
    if (adv > mae) { mae = adv; barsToMae = j - i0; }
  }

  const mfePts = Math.round(mfe / tick);
  const maePts = Math.round(mae / tick);
  const strong = Number(rev.points ?? 0) >= minStrongPoints;

  let label = null;
  if (mfePts >= confirmPoints && maePts <= stopPoints && barsToMfe <= confirmBars) {
    label = "gold";
  } else if (strong && mfePts < confirmPoints && maePts >= failBackPoints && barsToMae <= failBars) {
    label = "hard_negative";
  }
  return { label, mfePts, maePts };
}
