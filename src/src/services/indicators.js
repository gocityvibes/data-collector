export function sma(arr, p) {
  const out = Array(arr.length).fill(null);
  if (p <= 0) return out;
  let sum = 0;
  for (let i = 0; i < arr.length; i++) {
    const v = arr[i];
    sum += v;
    if (i >= p) sum -= arr[i - p];
    if (i >= p - 1) out[i] = sum / p;
  }
  return out;
}
export function ema(arr, p) {
  const out = Array(arr.length).fill(null);
  if (p <= 0) return out;
  const k = 2 / (p + 1);
  let prev = null;
  for (let i = 0; i < arr.length; i++) {
    const v = arr[i];
    if (v == null) { out[i] = prev; continue; }
    if (prev == null) { prev = v; out[i] = prev; }
    else { prev = v * k + prev * (1 - k); out[i] = prev; }
  }
  return out;
}
export function rsi(closes, p = 14) {
  const out = Array(closes.length).fill(null);
  if (p <= 0 || closes.length < p + 1) return out;
  let gains = 0, losses = 0;
  for (let i = 1; i <= p; i++) {
    const ch = closes[i] - closes[i - 1];
    if (ch >= 0) gains += ch; else losses -= ch;
  }
  let avgG = gains / p;
  let avgL = losses / p;
  out[p] = avgL === 0 ? 100 : 100 - 100 / (1 + avgG / avgL);
  for (let i = p + 1; i < closes.length; i++) {
    const ch = closes[i] - closes[i - 1];
    const g = ch > 0 ? ch : 0;
    const l = ch < 0 ? -ch : 0;
    avgG = (avgG * (p - 1) + g) / p;
    avgL = (avgL * (p - 1) + l) / p;
    out[i] = avgL === 0 ? 100 : 100 - 100 / (1 + avgG / avgL);
  }
  return out;
}
export function macd(closes, fast = 12, slow = 26, signal = 9) {
  const emaF = ema(closes, fast);
  const emaS = ema(closes, slow);
  const macdLine = closes.map((_, i) =>
    (emaF[i] == null || emaS[i] == null) ? null : (emaF[i] - emaS[i])
  );
  const signalInput = macdLine.map(v => (v == null ? null : v));
  const signalLine = [];
  let prev = null;
  const k = 2 / (signal + 1);
  for (let i = 0; i < signalInput.length; i++) {
    const v = signalInput[i];
    if (v == null) { signalLine[i] = null; continue; }
    if (prev == null) { prev = v; signalLine[i] = prev; }
    else { prev = v * k + prev * (1 - k); signalLine[i] = prev; }
  }
  const hist = macdLine.map((v, i) => (v == null || signalLine[i] == null) ? null : (v - signalLine[i]));
  return { macdLine, signalLine, hist };
}
export function atr(candlesAsc, p = 14) {
  const out = Array(candlesAsc.length).fill(null);
  if (p <= 0 || candlesAsc.length < p + 1) return out;
  const tr = Array(candlesAsc.length).fill(null);
  for (let i = 0; i < candlesAsc.length; i++) {
    const h = candlesAsc[i].high, l = candlesAsc[i].low;
    if (i === 0) tr[i] = h - l;
    else {
      const pc = candlesAsc[i - 1].close;
      tr[i] = Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
    }
  }
  let sum = 0;
  for (let i = 1; i <= p; i++) sum += tr[i];
  out[p] = sum / p;
  for (let i = p + 1; i < candlesAsc.length; i++) {
    out[i] = (out[i - 1] * (p - 1) + tr[i]) / p;
  }
  return out;
}
export function bollinger(closes, p = 20, k = 2) {
  const mid = sma(closes, p);
  const outU = Array(closes.length).fill(null);
  const outL = Array(closes.length).fill(null);
  let sum = 0, sumSq = 0;
  for (let i = 0; i < closes.length; i++) {
    const v = closes[i];
    sum += v; sumSq += v * v;
    if (i >= p) { sum -= closes[i - p]; sumSq -= closes[i - p] * closes[i - p]; }
    if (i >= p - 1) {
      const mean = sum / p;
      const variance = Math.max(0, sumSq / p - mean * mean);
      const std = Math.sqrt(variance);
      outU[i] = mid[i] + k * std;
      outL[i] = mid[i] - k * std;
    }
  }
  return { mid, upper: outU, lower: outL };
}
