#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build ICEM dashboard payload (data.js) from warehouse, processed, and raw caches."""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "scripts"))

from feature_engineering import (  # noqa: E402
    SECTOR_COMMODITY_WEIGHTS,
    SECTOR_ETF_WEIGHTS,
    SECTOR_UPSTREAM_BOARDS,
)

DISPLAY = json.loads((DASHBOARD / "config_display.json").read_text(encoding="utf-8"))
COMMODITY_LABELS: dict[str, str] = DISPLAY["commodity_labels"]
ETF_CODE_BY_LABEL: dict[str, str] = DISPLAY["etf_code_by_label"]
BOARD_CODES: dict[str, str] = DISPLAY["upstream_board_codes"]
MACRO_LABELS: dict[str, str] = DISPLAY["macro_series"]

HISTORY_DAYS_INDEX = 252
HISTORY_DAYS_COMMODITY = 120
HISTORY_DAYS_BOARD = 120
SPARK_POINTS = 60

MOA_ROOT = ROOT.parent / "market_opportunity_analysis"
V15_OBS_PATH = MOA_ROOT / "indicator_system" / "scripts" / "compute_out" / "warehouse" / "indicator_observations.csv"
V15_REGISTRY_PATH = MOA_ROOT / "indicator_system" / "scripts" / "references" / "csindex_primary_indices.json"

# 同日期多源时优先采用较新的管线（V15 日更链通常领先 feature_panel 一日）
QUOTE_SOURCE_RANK = {
    "v15_m01": 30,
    "index_ohlcv": 20,
    "feature_panel": 10,
    "index_daily_raw": 10,
}

_v15_obs_cache: pd.DataFrame | None = None
_sector_m01_cache: dict[str, str] | None = None


def _ts_code_file(ts_code: str) -> str:
    return ts_code.replace(".", "_")


def _safe_float(x: Any) -> float | None:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _format_trade_date(ts: Any) -> str:
    if hasattr(ts, "strftime"):
        return ts.strftime("%Y-%m-%d")
    s = str(ts).replace("-", "").replace("/", "")[:8]
    if len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return str(ts)[:10]


def _sector_to_m01() -> dict[str, str]:
    global _sector_m01_cache
    if _sector_m01_cache is not None:
        return _sector_m01_cache
    if V15_REGISTRY_PATH.is_file():
        reg = json.loads(V15_REGISTRY_PATH.read_text(encoding="utf-8"))
        _sector_m01_cache = {
            x["track"]: x["m01_id"] for x in reg.get("indices", []) if x.get("track") and x.get("m01_id")
        }
    else:
        _sector_m01_cache = {}
    return _sector_m01_cache


def _load_v15_obs() -> pd.DataFrame:
    global _v15_obs_cache
    if _v15_obs_cache is not None:
        return _v15_obs_cache
    if V15_OBS_PATH.is_file():
        _v15_obs_cache = pd.read_csv(V15_OBS_PATH, dtype=str)
    else:
        _v15_obs_cache = pd.DataFrame()
    return _v15_obs_cache


def _panel_close_df(ts_code: str) -> pd.DataFrame:
    code = _ts_code_file(ts_code)
    for path, src in [
        (ROOT / "data" / "processed" / f"feature_panel_{code}.csv", "feature_panel"),
        (ROOT / "data" / "raw" / "index_daily" / f"{code}.csv", "index_daily_raw"),
    ]:
        if not path.is_file():
            continue
        df = pd.read_csv(path, dtype=str)
        if df.empty or "trade_date" not in df.columns:
            continue
        close_col = "close_t" if "close_t" in df.columns else "close"
        if close_col not in df.columns:
            continue
        out = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(df["trade_date"].astype(str)),
                "close": pd.to_numeric(df[close_col], errors="coerce"),
                "source": src,
            }
        )
        return out.dropna(subset=["close"]).sort_values("trade_date")
    return pd.DataFrame()


def _warehouse_close_df(ts_code: str) -> pd.DataFrame:
    from data_warehouse import load_index_ohlcv

    wh = load_index_ohlcv(ts_code)
    if wh.empty or "trade_date" not in wh.columns or "close" not in wh.columns:
        return pd.DataFrame()
    out = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(wh["trade_date"].astype(str)),
            "close": pd.to_numeric(wh["close"], errors="coerce"),
            "source": "index_ohlcv",
        }
    )
    return out.dropna(subset=["close"]).sort_values("trade_date")


def _v15_close_df(sector_id: str) -> pd.DataFrame:
    m01 = _sector_to_m01().get(sector_id)
    if not m01:
        return pd.DataFrame()
    obs = _load_v15_obs()
    if obs.empty:
        return pd.DataFrame()
    g = obs[(obs["id"] == m01) & (obs["status"] == "ok")].copy()
    if g.empty:
        return pd.DataFrame()
    g["trade_date"] = pd.to_datetime(g["period_label"].astype(str), format="%Y%m%d", errors="coerce")
    g["close"] = pd.to_numeric(g["value"], errors="coerce")
    g["source"] = "v15_m01"
    return g.dropna(subset=["trade_date", "close"]).sort_values("trade_date")


def _merged_index_closes(ts_code: str, sector_id: str) -> pd.DataFrame:
    """合并 feature_panel / warehouse / V15 M01，取各交易日最新可用收盘。"""
    frames = [_panel_close_df(ts_code), _warehouse_close_df(ts_code), _v15_close_df(sector_id)]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, ignore_index=True)
    merged["rank"] = merged["source"].map(QUOTE_SOURCE_RANK).fillna(0)
    merged = merged.sort_values(["trade_date", "rank"])
    merged = merged.drop_duplicates(subset=["trade_date"], keep="last")
    return merged.sort_values("trade_date").reset_index(drop=True)


def _latest_merged_quote(ts_code: str, sector_id: str) -> dict[str, Any]:
    df = _merged_index_closes(ts_code, sector_id)
    if df.empty:
        return {}
    df = df.sort_values("trade_date")
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else None
    pct = None
    if prev is not None and float(prev["close"]) != 0:
        pct = (float(last["close"]) / float(prev["close"]) - 1) * 100
    src = str(last["source"])
    src_label = {
        "v15_m01": "V15 M01",
        "index_ohlcv": "指数 warehouse",
        "feature_panel": "特征面板",
        "index_daily_raw": "Tushare 缓存",
    }.get(src, src)
    return {
        "trade_date": _format_trade_date(last["trade_date"]),
        "close": round(float(last["close"]), 4),
        "pct_chg_d": round(pct, 4) if pct is not None else None,
        "quote_source": src,
        "quote_source_label": src_label,
    }


def _pct_series_tail(series: pd.Series, n: int) -> list[dict[str, Any]]:
    s = series.dropna().astype(float)
    if s.empty:
        return []
    tail = s.tail(n)
    base = float(tail.iloc[0]) if tail.iloc[0] else None
    out: list[dict[str, Any]] = []
    for idx, val in tail.items():
        d = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
        pct = ((val / base - 1) * 100) if base and base != 0 else 0.0
        out.append({"date": d, "value": round(val, 4), "pct_from_start": round(pct, 3)})
    return out


def _read_index_close_tail(ts_code: str, sector_id: str, n: int) -> list[dict[str, Any]]:
    merged = _merged_index_closes(ts_code, sector_id)
    if merged.empty:
        return []
    s = pd.Series(merged["close"].astype(float).values, index=merged["trade_date"])
    return _pct_series_tail(s, n)


def _latest_feature_snapshot(ts_code: str, sector_id: str) -> dict[str, Any]:
    """技术面/估值取 feature_panel 末行；收盘与行情日期优先合并后的最新报价。"""
    code = _ts_code_file(ts_code)
    panel = ROOT / "data" / "processed" / f"feature_panel_{code}.csv"
    out: dict[str, Any] = {}
    panel_last_date: str | None = None

    if panel.is_file():
        df = pd.read_csv(panel, dtype=str)
        if not df.empty:
            df = df.sort_values("trade_date")
            row = df.iloc[-1]
            panel_last_date = _format_trade_date(row["trade_date"])
            for k in [
                "rsi_14d",
                "pe_ttm",
                "pb",
                "pe_percentile_1y",
                "ma_deviation_20d",
                "chg_1w",
                "chg_1m",
            ]:
                if k in row.index:
                    out[k] = _safe_float(row[k])

    quote = _latest_merged_quote(ts_code, sector_id)
    if quote:
        out["trade_date"] = quote["trade_date"].replace("-", "")
        out["close_t"] = quote["close"]
        if quote.get("pct_chg_d") is not None:
            out["pct_chg_d"] = quote["pct_chg_d"]
        out["quote_source"] = quote.get("quote_source")
        out["quote_source_label"] = quote.get("quote_source_label")
    elif panel.is_file():
        df = pd.read_csv(panel, dtype=str)
        if not df.empty:
            row = df.sort_values("trade_date").iloc[-1]
            out["trade_date"] = str(row["trade_date"]).replace("-", "")[:8]
            out["close_t"] = _safe_float(row.get("close_t") or row.get("close"))
            out["pct_chg_d"] = _safe_float(row.get("pct_chg_d"))

    if panel_last_date and quote:
        out["panel_last_date"] = panel_last_date.replace("-", "")
        if panel_last_date < quote["trade_date"]:
            out["quote_newer_than_panel"] = True
    return out


def _load_commodity_daily() -> pd.DataFrame:
    p = ROOT / "data" / "warehouse" / "commodity_daily.csv"
    if not p.is_file():
        return pd.DataFrame()
    df = pd.read_csv(p)
    if "date" not in df.columns:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


def _commodity_series(commodity_df: pd.DataFrame, key: str, n: int) -> list[dict[str, Any]]:
    col = f"{key}_close"
    if col not in commodity_df.columns:
        return []
    sub = commodity_df[["date", col]].dropna()
    if sub.empty:
        return []
    s = pd.Series(sub[col].astype(float).values, index=sub["date"])
    return _pct_series_tail(s, n)


def _raw_futures_last_date(key: str) -> str | None:
    """Optional per品种 raw 缓存最新交易日（与 fetch_industry 命名一致）。"""
    p = ROOT / "data" / "raw" / "industry" / f"futures_{key}.csv"
    if not p.is_file():
        return None
    try:
        df = pd.read_csv(p, usecols=["date"], dtype=str)
        if df.empty:
            return None
        d = pd.to_datetime(df["date"], errors="coerce").max()
        if pd.isna(d):
            return None
        return d.strftime("%Y-%m-%d")
    except (ValueError, KeyError, pd.errors.EmptyDataError):
        return None


def _commodity_latest(commodity_df: pd.DataFrame, key: str) -> dict[str, Any]:
    col = f"{key}_close"
    if col not in commodity_df.columns:
        return {"key": key, "label": COMMODITY_LABELS.get(key, key), "missing": True}
    sub = commodity_df[["date", col]].dropna()
    if len(sub) < 2:
        return {"key": key, "label": COMMODITY_LABELS.get(key, key), "missing": True}
    last = sub.iloc[-1]
    prev = sub.iloc[-2]
    close = float(last[col])
    prev_close = float(prev[col])
    chg_1d = ((close / prev_close - 1) * 100) if prev_close else None
    chg_20d = None
    if len(sub) >= 21:
        base = float(sub.iloc[-21][col])
        if base:
            chg_20d = (close / base - 1) * 100
    wh_date = last["date"].strftime("%Y-%m-%d")
    raw_date = _raw_futures_last_date(key)
    as_of = wh_date
    quote_source = "commodity_daily"
    if raw_date and raw_date > wh_date:
        as_of = raw_date
        quote_source = "raw_futures_cache"
    return {
        "key": key,
        "label": COMMODITY_LABELS.get(key, key),
        "date": as_of,
        "warehouse_date": wh_date,
        "raw_cache_date": raw_date,
        "quote_source": quote_source,
        "close": round(close, 2),
        "chg_1d_pct": round(chg_1d, 3) if chg_1d is not None else None,
        "chg_20d_pct": round(chg_20d, 3) if chg_20d is not None else None,
        "spark": _commodity_series(commodity_df, key, SPARK_POINTS),
    }


def _commodity_panel_meta(commodity_panel: list[dict[str, Any]], commodity_df: pd.DataFrame) -> dict[str, Any]:
    dates = [c["date"] for c in commodity_panel if c.get("date")]
    wh_max = None
    if not commodity_df.empty and "date" in commodity_df.columns:
        wh_max = pd.to_datetime(commodity_df["date"]).max()
        wh_max = wh_max.strftime("%Y-%m-%d") if not pd.isna(wh_max) else None
    panel_as_of = max(dates) if dates else wh_max
    stale = []
    if panel_as_of:
        for c in commodity_panel:
            d = c.get("date")
            if d and d < panel_as_of:
                stale.append({"key": c["key"], "label": c["label"], "date": d})
    return {
        "as_of": panel_as_of,
        "warehouse_table_max_date": wh_max,
        "count": len(commodity_panel),
        "stale_vs_panel": stale,
    }


def _read_upstream_board(board_name: str, n: int) -> list[dict[str, Any]]:
    code = BOARD_CODES.get(board_name)
    if not code:
        return []
    p = ROOT / "data" / "raw" / "industry" / f"upstream_board_{code}.csv"
    if not p.is_file():
        return []
    df = pd.read_csv(p)
    close_col = f"{board_name}_close"
    if close_col not in df.columns or "date" not in df.columns:
        return []
    df["date"] = pd.to_datetime(df["date"])
    s = pd.Series(df[close_col].astype(float).values, index=df["date"])
    return _pct_series_tail(s, n)


def _read_etf(label: str, n: int) -> list[dict[str, Any]]:
    code = ETF_CODE_BY_LABEL.get(label)
    if not code:
        return []
    p = ROOT / "data" / "raw" / "industry" / f"etf_daily_{code}.csv"
    if not p.is_file():
        return []
    df = pd.read_csv(p)
    close_col = f"{label}_close"
    if close_col not in df.columns:
        return []
    df["date"] = pd.to_datetime(df["date"])
    s = pd.Series(df[close_col].astype(float).values, index=df["date"])
    return _pct_series_tail(s, n)


def _load_macro_monthly() -> pd.DataFrame:
    """Warehouse macro + PMI from raw cache when warehouse PMI columns are empty."""
    p = ROOT / "data" / "warehouse" / "macro_monthly.csv"
    df = pd.read_csv(p) if p.is_file() else pd.DataFrame()

    pmi_path = ROOT / "data" / "raw" / "industry" / "pmi.csv"
    if not pmi_path.is_file():
        return df

    pmi = pd.read_csv(pmi_path)
    if pmi.empty or "date" not in pmi.columns:
        return df

    need_pmi = df.empty or "pmi_manufacturing" not in df.columns or df["pmi_manufacturing"].notna().sum() == 0
    if not need_pmi:
        return df

    pmi = pmi.copy()
    pmi["date"] = pd.to_datetime(pmi["date"])
    if df.empty:
        return pmi

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    pmi_cols = [c for c in ["pmi_manufacturing", "pmi_non_manufacturing"] if c in pmi.columns]
    if not pmi_cols:
        return df

    merged = df.drop(columns=[c for c in pmi_cols if c in df.columns], errors="ignore")
    merged = merged.merge(pmi[["date"] + pmi_cols], on="date", how="outer")
    return merged.sort_values("date").reset_index(drop=True)


def _macro_tail(macro: pd.DataFrame, col: str, n: int = 36) -> list[dict[str, Any]]:
    if macro.empty or col not in macro.columns:
        return []
    df = macro.copy()
    date_col = "date" if "date" in df.columns else df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).tail(n)
    out = []
    for _, r in df.iterrows():
        v = _safe_float(r[col])
        if v is None:
            continue
        d = r[date_col]
        out.append({"date": d.strftime("%Y-%m-%d"), "value": round(v, 3)})
    return out


def _energy_index_tail(n: int = 120) -> list[dict[str, Any]]:
    p = ROOT / "data" / "raw" / "industry" / "energy_index.csv"
    if not p.is_file():
        return []
    df = pd.read_csv(p)
    if "date" not in df.columns or "energy_index" not in df.columns:
        return []
    df["date"] = pd.to_datetime(df["date"])
    s = pd.Series(df["energy_index"].astype(float).values, index=df["date"])
    return _pct_series_tail(s, n)


def _margin_meta() -> dict[str, Any]:
    series = _margin_tail(90)
    as_of = series[-1]["date"] if series else None
    latest_yi = series[-1]["value"] if series else None
    return {
        "as_of": as_of,
        "latest_trillion": round(latest_yi / 10000, 4) if latest_yi is not None else None,
        "latest_billion": latest_yi,
        "points": len(series),
    }


def _margin_tail(n: int = 90) -> list[dict[str, Any]]:
    p = ROOT / "data" / "warehouse" / "fund_flow_daily.csv"
    if not p.is_file():
        return []
    df = pd.read_csv(p)
    if "date" not in df.columns or "margin_balance" not in df.columns:
        return []
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").tail(n)
    out = []
    for _, r in df.iterrows():
        v = _safe_float(r["margin_balance"])
        if v is None:
            continue
        out.append({"date": r["date"].strftime("%Y-%m-%d"), "value": round(v / 1e8, 2)})
    return out


def _extract_predictions(sector_blob: dict[str, Any]) -> dict[str, Any]:
    primary = sector_blob.get("primary_model") or (
        (sector_blob.get("model_selection") or {}).get("primary", {}).get("model")
    )
    models = sector_blob.get("models") or {}
    preds_out: list[dict[str, Any]] = []
    if primary and primary in models:
        for p in models[primary].get("predictions") or []:
            preds_out.append(
                {
                    "horizon": p.get("target_date"),
                    "calendar_date": p.get("calendar_date"),
                    "predicted_close": _safe_float(p.get("predicted_close")),
                    "direction": p.get("predicted_direction"),
                    "ci_low": _safe_float(p.get("confidence_lower")),
                    "ci_high": _safe_float(p.get("confidence_upper")),
                }
            )
    return {"predictions": preds_out}


def build() -> dict[str, Any]:
    sectors_cfg = json.loads((ROOT / "config" / "sector_indices.json").read_text(encoding="utf-8"))
    sectors = sectors_cfg.get("sectors", {})

    pred_path = ROOT / "data" / "processed" / "predictions_latest.json"
    predictions = {}
    meta: dict[str, Any] = {}
    if pred_path.is_file():
        raw_pred = json.loads(pred_path.read_text(encoding="utf-8"))
        meta = raw_pred.get("_meta") or {}
        predictions = {k: v for k, v in raw_pred.items() if not k.startswith("_")}

    commodity_df = _load_commodity_daily()
    macro_df = _load_macro_monthly()

    all_commodity_keys = sorted(COMMODITY_LABELS.keys())
    commodity_panel = [_commodity_latest(commodity_df, k) for k in all_commodity_keys]
    commodity_panel = [c for c in commodity_panel if not c.get("missing")]
    commodity_meta = _commodity_panel_meta(commodity_panel, commodity_df)

    sector_rows: list[dict[str, Any]] = []
    sector_detail: dict[str, Any] = {}

    for sid in sorted(sectors.keys()):
        cfg = sectors[sid]
        name = cfg.get("name_zh", sid)
        val_enabled = cfg.get("valuation_enabled", True)
        primary = cfg.get("primary") or {}
        ts_code = primary.get("ts_code", "")

        blob = predictions.get(sid) or {}
        row: dict[str, Any] = {
            "sector_id": sid,
            "name_zh": name,
            "valuation_enabled": val_enabled,
            "ts_code": ts_code,
            "index_name": primary.get("name", ""),
        }

        if not val_enabled:
            row["status"] = blob.get("valuation_status") or "disabled"
            row["reason"] = blob.get("reason") or cfg.get("valuation_note", "")
            sector_rows.append(row)
            continue

        snap = _latest_feature_snapshot(ts_code, sid) if ts_code else {}
        pred_info = _extract_predictions(blob) if blob else {}
        quote_date = snap.get("trade_date")
        if quote_date and len(str(quote_date)) == 8:
            quote_date_disp = f"{quote_date[:4]}-{quote_date[4:6]}-{quote_date[6:8]}"
        else:
            quote_date_disp = quote_date
        model_date = blob.get("last_data_date")

        row.update(
            {
                "last_data_date": quote_date_disp or model_date,
                "model_last_data_date": model_date,
                "quote_source_label": snap.get("quote_source_label"),
                "last_close": snap.get("close_t") or _safe_float(blob.get("last_close")),
                "pct_chg_d": snap.get("pct_chg_d"),
                "rsi_14d": snap.get("rsi_14d"),
                "pe_ttm": snap.get("pe_ttm"),
                "pe_pct_1y": snap.get("pe_percentile_1y"),
                "t1_direction": (pred_info.get("predictions") or [{}])[0].get("direction"),
                "t1_predicted": (pred_info.get("predictions") or [{}])[0].get("predicted_close"),
            }
        )
        sector_rows.append(row)

        weights = SECTOR_COMMODITY_WEIGHTS.get(sid, {})
        linked_commodities = sorted(weights.items(), key=lambda x: -x[1])[:4]
        boards = SECTOR_UPSTREAM_BOARDS.get(sid, {})
        linked_boards = sorted(boards.items(), key=lambda x: -x[1])
        etfs = SECTOR_ETF_WEIGHTS.get(sid, {})

        sector_detail[sid] = {
            "sector_id": sid,
            "name_zh": name,
            "ts_code": ts_code,
            "index_name": primary.get("name", ""),
            "snapshot": snap,
            "index_quote": _latest_merged_quote(ts_code, sid) if ts_code else {},
            "model_last_data_date": model_date,
            "prediction": pred_info,
            "index_history": _read_index_close_tail(ts_code, sid, HISTORY_DAYS_INDEX),
            "linked_commodities": [
                {
                    "key": k,
                    "label": COMMODITY_LABELS.get(k, k),
                    "weight": w,
                    "latest": _commodity_latest(commodity_df, k),
                    "history": _commodity_series(commodity_df, k, HISTORY_DAYS_COMMODITY),
                }
                for k, w in linked_commodities
            ],
            "upstream_boards": [
                {
                    "name": bname,
                    "weight": w,
                    "history": _read_upstream_board(bname, HISTORY_DAYS_BOARD),
                }
                for bname, w in linked_boards
            ],
            "linked_etfs": [
                {"label": label, "weight": w, "history": _read_etf(label, HISTORY_DAYS_BOARD)}
                for label, w in sorted(etfs.items(), key=lambda x: -x[1])
            ],
        }

    macro_block: dict[str, Any] = {}
    for col, label in MACRO_LABELS.items():
        series = _macro_tail(macro_df, col, n=48)
        macro_block[col] = {"label": label, "series": series, "has_data": len(series) > 0}

    quote_dates = [
        r.get("last_data_date")
        for r in sector_rows
        if r.get("valuation_enabled", True) and r.get("last_data_date")
    ]
    index_quote_as_of = max(quote_dates) if quote_dates else None

    payload = {
        "dashboard_id": DISPLAY["dashboard_id"],
        "title_zh": DISPLAY["title_zh"],
        "title_en": DISPLAY["title_en"],
        "distinction_note": DISPLAY["distinction_note"],
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "as_of_date": meta.get("as_of_date") or date.today().isoformat(),
        "index_quote_as_of": index_quote_as_of,
        "last_trading_date": meta.get("last_trading_date"),
        "sector_overview": sector_rows,
        "sector_detail": sector_detail,
        "commodity_panel": commodity_panel,
        "commodity_panel_meta": commodity_meta,
        "macro": macro_block,
        "energy_index": _energy_index_tail(120),
        "margin_balance": _margin_tail(90),
        "margin_balance_meta": _margin_meta(),
        "metric_glossary": DISPLAY.get("metric_glossary", {}),
        "data_health": _data_health(),
    }
    return payload


def _data_health() -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    pred = ROOT / "data" / "processed" / "predictions_latest.json"
    checks.append(
        {
            "name": "predictions_latest.json",
            "ok": pred.is_file(),
            "mtime": datetime.fromtimestamp(pred.stat().st_mtime).isoformat(timespec="seconds")
            if pred.is_file()
            else None,
        }
    )
    wh = ROOT / "data" / "warehouse" / "commodity_daily.csv"
    if wh.is_file():
        df = pd.read_csv(wh, usecols=["date"])
        checks.append(
            {
                "name": "commodity_daily",
                "ok": True,
                "last_date": str(df["date"].max()),
                "rows": len(df),
            }
        )
    else:
        checks.append({"name": "commodity_daily", "ok": False})
    ind = ROOT / "data" / "raw" / "industry"
    n_futures = len(list(ind.glob("futures_*.csv"))) if ind.is_dir() else 0
    n_boards = len(list(ind.glob("upstream_board_*.csv"))) if ind.is_dir() else 0
    checks.append({"name": "industry_futures_files", "ok": n_futures > 0, "count": n_futures})
    checks.append({"name": "upstream_board_files", "ok": n_boards > 0, "count": n_boards})
    return checks


def write_data_js(payload: dict[str, Any], out_path: Path) -> None:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    out_path.write_text(
        f"/* ICEM — auto-generated {payload.get('generated_at', '')} */\n"
        f"window.ICEM_DATA = {body};\n",
        encoding="utf-8",
    )


def main() -> int:
    payload = build()
    out = DASHBOARD / "data.js"
    write_data_js(payload, out)
    n_sectors = len(payload.get("sector_overview", []))
    n_commodities = len(payload.get("commodity_panel", []))
    print(f"ICEM data.js written: {out} ({n_sectors} sectors, {n_commodities} commodities)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
