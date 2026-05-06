"""
Analyze alternative score strategies and generate comparison artifacts.
"""

from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.cleaner.cleaner import PipelineCleaner
from pipeline.config import PipelineConfig
from pipeline.feature_engine.engine import FeatureEngine
from pipeline.loader.event_stream import EventStreamBuilder
from pipeline.loader.raw_data_loader import RawDataLoader


ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"


@dataclass(frozen=True)
class PageSpec:
    slug: str
    title: str


PAGE_SPECS = (
    PageSpec("new_arrivals", "Новинки"),
    PageSpec("sale", "Распродажа"),
    PageSpec("men", "Мужское"),
    PageSpec("jackets", "Куртки"),
)


def _env_int(name: str, default: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def _env_optional_int(name: str) -> Optional[int]:
    raw_value = os.environ.get(name)
    if raw_value is None or not raw_value.strip():
        return None
    try:
        value = int(raw_value)
    except ValueError:
        return None
    return value if value > 0 else None


def _env_optional_date(name: str) -> Optional[date]:
    raw_value = os.environ.get(name)
    if raw_value is None or not raw_value.strip():
        return None
    try:
        return date.fromisoformat(raw_value.strip())
    except ValueError:
        return None


def _resolve_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for column in candidates:
        if column in df.columns:
            return column
    return None


def _build_variant_to_product_lookup(shop_df: pd.DataFrame) -> Dict[str, int]:
    barcode_column = _resolve_first_existing_column(
        shop_df,
        [
            "Штрих-код",
            "barcode",
            "variant_barcode",
            "variant_barcodes",
        ],
    )
    product_id_column = _resolve_first_existing_column(
        shop_df,
        [
            "ID товара",
            "product_id",
        ],
    )
    if barcode_column is None or product_id_column is None:
        return {}

    mapping: Dict[str, int] = {}
    for _, row in shop_df.iterrows():
        barcode = str(row.get(barcode_column, "")).strip()
        product_id = row.get(product_id_column)
        if not barcode or not str(product_id).strip():
            continue
        try:
            mapping[barcode] = int(float(str(product_id).replace(",", ".")))
        except ValueError:
            continue
    return mapping


def _season_group_for_month(month: int) -> str:
    if month in {2, 3, 4, 5, 6, 7}:
        return "SS"
    if month in {8, 9, 10, 11, 12, 1}:
        return "AW"
    return "UNKNOWN"


def _season_group_from_code(season_code: str) -> str:
    season_code = str(season_code or "").strip().upper()
    if season_code.startswith(("SS", "SU")):
        return "SS"
    if season_code.startswith(("AW", "FW", "HO")):
        return "AW"
    return "UNKNOWN"


def _matches_page(df: pd.DataFrame, slug: str) -> pd.Series:
    base = df["in_stock"].astype(bool) & df["current_on_site"].astype(bool)
    if slug == "new_arrivals":
        return base & df["is_new"].astype(bool)
    if slug == "sale":
        return base & df["is_sale"].astype(bool)
    if slug == "men":
        return base & df["gender"].astype(str).str.lower().eq("male")
    if slug == "jackets":
        return base & df["category_name"].astype(str).isin(["Куртки", "Куртка", "Пуховики", "Жилеты"])
    return base


def _compact_records(df: pd.DataFrame, limit: int = 10) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for _, row in df.head(limit).iterrows():
        records.append(
            {
                "product_id": int(row["product_id"]),
                "title": str(row.get("title", "")),
                "brand": str(row.get("vendor", "")),
                "category_name": str(row.get("category_name", "")),
                "price": float(row.get("price", 0.0)),
                "views": int(row.get("views", 0)),
                "cart_adds": int(row.get("cart_adds", 0)),
                "purchases": int(row.get("purchases", 0)),
                "revenue": float(row.get("revenue", 0.0)),
                "popularity": float(row.get("popularity", 0.0)),
                "novelty": float(row.get("novelty", 0.0)),
                "boost": float(row.get("boost", 1.0)),
                "final_score": float(row.get("final_score", 0.0)),
            }
        )
    return records


def _load_base_frames(
    *,
    config: PipelineConfig,
    source_row_limit: int,
    event_row_limit: int,
    event_file_limit: int,
):
    loader = RawDataLoader(config)
    loader.validate_sources()
    shop_df = loader.load_shop_products(minimal=True, nrows=source_row_limit if source_row_limit < 999999 else None)
    categories_df = loader.load_categories()
    cleaner = PipelineCleaner.from_raw_sources(shop_products_df=shop_df, categories_df=categories_df)
    cleaned_df = cleaner.clean()

    event_builder = EventStreamBuilder(loader)
    variant_lookup = _build_variant_to_product_lookup(shop_df)
    daily_events_df = event_builder.aggregate_daily_products(
        variant_lookup,
        max_files=event_file_limit if event_file_limit < 999 else None,
        max_rows_per_file=event_row_limit if event_row_limit < 999999 else None,
        as_of_date=config.as_of_date.isoformat() if config.as_of_date else None,
    )
    return loader, shop_df, cleaned_df, daily_events_df, event_builder, variant_lookup


def _build_product_event_frame(
    event_builder: EventStreamBuilder,
    variant_lookup: Dict[str, int],
    *,
    max_files: Optional[int],
    max_rows_per_file: Optional[int],
    as_of_date: Optional[date],
) -> pd.DataFrame:
    frame = event_builder.to_frame(max_files=max_files, max_rows_per_file=max_rows_per_file)
    if frame.empty:
        return pd.DataFrame()

    frame = frame.loc[frame["product_insales_id"].astype(str).str.strip().ne("")].copy()
    frame["product_id"] = frame["product_insales_id"].map(variant_lookup)
    frame = frame.dropna(subset=["product_id", "event_date"]).copy()
    frame["product_id"] = frame["product_id"].astype(int)
    if as_of_date is not None:
        frame = frame.loc[frame["event_date"] <= as_of_date.isoformat()].copy()
    return frame


def _seasonality_diagnostics(
    cleaned_df: pd.DataFrame,
    daily_events_df: pd.DataFrame,
    *,
    as_of_date: date,
    window_days: Optional[int],
    window_start: Optional[date],
    window_end: Optional[date],
) -> Dict[str, object]:
    engine = FeatureEngine(
        catalog_df=cleaned_df.copy(),
        events_df=daily_events_df.copy(),
        as_of_date=as_of_date,
        window_days=window_days,
        window_start=window_start,
        window_end=window_end,
        strategy="baseline",
    )
    event_data = engine.aggregate_events()

    frame = cleaned_df.copy()
    for column, series in event_data.items():
        frame[column] = frame["product_id"].map(series).fillna(0)

    frame["season_group"] = frame["season_code"].map(_season_group_from_code)
    current_group = _season_group_for_month(as_of_date.month)
    frame["is_in_season"] = frame["season_group"].eq(current_group)
    frame["log_views"] = frame["views"].map(lambda value: math.log1p(float(value)))
    frame["log_revenue"] = frame["revenue"].map(lambda value: math.log1p(float(value)))

    season_summary = (
        frame.groupby(["season_code", "season_group", "is_in_season"], dropna=False)
        .agg(
            products=("product_id", "count"),
            mean_views=("views", "mean"),
            median_views=("views", "median"),
            mean_cart_adds=("cart_adds", "mean"),
            mean_purchases=("purchases", "mean"),
            mean_revenue=("revenue", "mean"),
            purchase_rate=("purchases", lambda values: float((pd.to_numeric(values, errors="coerce").fillna(0) > 0).mean())),
        )
        .reset_index()
        .sort_values(["is_in_season", "products"], ascending=[False, False])
    )

    in_season = frame["is_in_season"].astype(int)
    correlations = {
        "in_season_vs_views": float(in_season.corr(frame["views"])) if frame["views"].std() > 0 else 0.0,
        "in_season_vs_log_views": float(in_season.corr(frame["log_views"])) if frame["log_views"].std() > 0 else 0.0,
        "in_season_vs_purchases": float(in_season.corr(frame["purchases"])) if frame["purchases"].std() > 0 else 0.0,
        "in_season_vs_revenue": float(in_season.corr(frame["log_revenue"])) if frame["log_revenue"].std() > 0 else 0.0,
    }

    in_block = frame.loc[frame["is_in_season"]]
    out_block = frame.loc[~frame["is_in_season"]]
    comparison = {
        "current_season_group": current_group,
        "in_season_products": int(len(in_block)),
        "out_of_season_products": int(len(out_block)),
        "mean_views_ratio_in_vs_out": float((in_block["views"].mean() + 1.0) / (out_block["views"].mean() + 1.0)) if len(out_block) else 0.0,
        "mean_purchases_ratio_in_vs_out": float((in_block["purchases"].mean() + 1.0) / (out_block["purchases"].mean() + 1.0)) if len(out_block) else 0.0,
        "mean_revenue_ratio_in_vs_out": float((in_block["revenue"].mean() + 1.0) / (out_block["revenue"].mean() + 1.0)) if len(out_block) else 0.0,
    }

    return {
        "summary_rows": season_summary.to_dict(orient="records"),
        "correlations": correlations,
        "comparison": comparison,
    }


def _user_noise_diagnostics(product_event_frame: pd.DataFrame) -> Dict[str, object]:
    if product_event_frame.empty:
        return {"empty": True}

    frame = product_event_frame.copy()
    frame["customer_mindbox_id"] = frame["customer_mindbox_id"].fillna("").astype(str).str.strip()
    frame = frame.loc[frame["customer_mindbox_id"].ne("")]
    if frame.empty:
        return {"empty": True}

    diagnostics: Dict[str, object] = {"by_intent": {}}
    for intent in ["product_view", "cart_add", "purchase"]:
        intent_frame = frame.loc[frame["intent"].eq(intent)].copy()
        if intent_frame.empty:
            diagnostics["by_intent"][intent] = {"events": 0}
            continue

        total_events = int(len(intent_frame))
        customer_counts = (
            intent_frame.groupby("customer_mindbox_id")
            .size()
            .sort_values(ascending=False)
        )
        unique_customers = int(customer_counts.shape[0])
        top_1pct_n = max(1, math.ceil(unique_customers * 0.01))
        top_5pct_n = max(1, math.ceil(unique_customers * 0.05))
        top_10pct_n = max(1, math.ceil(unique_customers * 0.10))

        if intent == "product_view":
            product_customer_counts = (
                intent_frame.groupby(["product_id", "customer_mindbox_id"])
                .size()
                .reset_index(name="customer_views")
            )
            product_totals = (
                intent_frame.groupby("product_id")
                .size()
                .reset_index(name="total_views")
            )
            dominant = (
                product_customer_counts.groupby("product_id")["customer_views"].max()
                .reset_index(name="max_customer_views")
                .merge(product_totals, on="product_id", how="left")
            )
            dominant["dominant_user_share"] = dominant["max_customer_views"] / dominant["total_views"]
            repeat = (
                product_totals.merge(
                    intent_frame.groupby("product_id")["customer_mindbox_id"].nunique().reset_index(name="unique_viewers"),
                    on="product_id",
                    how="left",
                )
            )
            repeat["repeat_view_ratio"] = repeat["total_views"] / repeat["unique_viewers"].clip(lower=1)
            dominant_stats = {
                "products_with_dominant_share_gt_0_2": int((dominant["dominant_user_share"] > 0.2).sum()),
                "products_with_dominant_share_gt_0_5": int((dominant["dominant_user_share"] > 0.5).sum()),
                "dominant_user_share_mean": float(dominant["dominant_user_share"].mean()),
                "dominant_user_share_p95": float(dominant["dominant_user_share"].quantile(0.95)),
                "repeat_view_ratio_mean": float(repeat["repeat_view_ratio"].mean()),
                "repeat_view_ratio_p95": float(repeat["repeat_view_ratio"].quantile(0.95)),
            }
        else:
            dominant_stats = {}

        diagnostics["by_intent"][intent] = {
            "events": total_events,
            "unique_customers": unique_customers,
            "top_1pct_customer_share": float(customer_counts.head(top_1pct_n).sum() / total_events),
            "top_5pct_customer_share": float(customer_counts.head(top_5pct_n).sum() / total_events),
            "top_10pct_customer_share": float(customer_counts.head(top_10pct_n).sum() / total_events),
            **dominant_stats,
        }

    return diagnostics


def _strategy_comparison(
    cleaned_df: pd.DataFrame,
    daily_events_df: pd.DataFrame,
    *,
    as_of_date: date,
    window_days: Optional[int],
    window_start: Optional[date],
    window_end: Optional[date],
    strategies: List[str],
    enable_seasonality: bool,
) -> Dict[str, object]:
    scored_by_strategy: Dict[str, pd.DataFrame] = {}
    summary_rows: List[Dict[str, object]] = []

    for strategy in strategies:
        engine = FeatureEngine(
            catalog_df=cleaned_df.copy(),
            events_df=daily_events_df.copy(),
            as_of_date=as_of_date,
            window_days=window_days,
            window_start=window_start,
            window_end=window_end,
            strategy=strategy,
            enable_seasonality=enable_seasonality,
        )
        scored = engine.compute_scores()
        scored_by_strategy[strategy] = scored
        scored.to_csv(ARTIFACTS_DIR / f"scored_catalog_{strategy}.csv", index=False, encoding="utf-8-sig")

        summary_rows.append(
            {
                "strategy": strategy,
                "label": FeatureEngine.available_strategies().get(strategy, strategy),
                "seasonality_enabled": enable_seasonality,
                "products": int(len(scored)),
                "nonzero_score_products": int((scored["final_score"] > 0).sum()),
                "products_with_views": int((scored["views"] > 0).sum()),
                "products_with_purchases": int((scored["purchases"] > 0).sum()),
                "mean_popularity": float(scored["popularity"].mean()),
                "mean_novelty": float(scored["novelty"].mean()),
                "mean_final_score": float(scored["final_score"].mean()),
                "max_final_score": float(scored["final_score"].max()),
            }
        )

    baseline_top = set(
        scored_by_strategy["baseline"]
        .sort_values("final_score", ascending=False)
        .head(100)["product_id"]
        .tolist()
    )
    for row in summary_rows:
        strategy = row["strategy"]
        current_top = set(
            scored_by_strategy[strategy]
            .sort_values("final_score", ascending=False)
            .head(100)["product_id"]
            .tolist()
        )
        row["top100_overlap_vs_baseline"] = int(len(current_top & baseline_top))

    showcases: Dict[str, object] = {}
    for strategy, scored in scored_by_strategy.items():
        showcases[strategy] = {}
        for page in PAGE_SPECS:
            page_df = scored.loc[_matches_page(scored, page.slug)].copy()
            showcases[strategy][page.slug] = {
                "title": page.title,
                "count": int(len(page_df)),
                "top10_final_score": _compact_records(page_df.sort_values("final_score", ascending=False)),
                "top10_popularity": _compact_records(page_df.sort_values("popularity", ascending=False)),
                "top10_novelty": _compact_records(page_df.sort_values("novelty", ascending=False)),
            }

    return {
        "summary_rows": summary_rows,
        "showcases": showcases,
    }


def _write_markdown_report(
    *,
    output_path: Path,
    seasonality: Dict[str, object],
    user_noise: Dict[str, object],
    comparison: Dict[str, object],
) -> None:
    lines: List[str] = [
        "# Сравнение вариантов score",
        "",
        "## 1. Диагностика сезонности",
        "",
        f"- Текущий сезонный блок: `{seasonality['comparison']['current_season_group']}`",
        f"- Товаров в сезоне: `{seasonality['comparison']['in_season_products']}`",
        f"- Товаров вне сезона: `{seasonality['comparison']['out_of_season_products']}`",
        f"- Ratio mean views (in/out): `{seasonality['comparison']['mean_views_ratio_in_vs_out']:.4f}`",
        f"- Ratio mean purchases (in/out): `{seasonality['comparison']['mean_purchases_ratio_in_vs_out']:.4f}`",
        f"- Ratio mean revenue (in/out): `{seasonality['comparison']['mean_revenue_ratio_in_vs_out']:.4f}`",
        "",
        "Корреляции:",
        "",
    ]
    for key, value in seasonality["correlations"].items():
        lines.append(f"- `{key}` = `{value:.6f}`")

    lines.extend(
        [
            "",
            "## 2. Диагностика user-noise",
            "",
        ]
    )
    if user_noise.get("empty"):
        lines.append("Нет данных для user-noise анализа.")
    else:
        for intent, metrics in user_noise["by_intent"].items():
            lines.extend(
                [
                    f"### {intent}",
                    "",
                    f"- events: `{metrics.get('events', 0)}`",
                    f"- unique_customers: `{metrics.get('unique_customers', 0)}`",
                    f"- top_1pct_customer_share: `{metrics.get('top_1pct_customer_share', 0.0):.6f}`",
                    f"- top_5pct_customer_share: `{metrics.get('top_5pct_customer_share', 0.0):.6f}`",
                    f"- top_10pct_customer_share: `{metrics.get('top_10pct_customer_share', 0.0):.6f}`",
                ]
            )
            if "repeat_view_ratio_mean" in metrics:
                lines.extend(
                    [
                        f"- repeat_view_ratio_mean: `{metrics['repeat_view_ratio_mean']:.6f}`",
                        f"- repeat_view_ratio_p95: `{metrics['repeat_view_ratio_p95']:.6f}`",
                        f"- dominant_user_share_mean: `{metrics['dominant_user_share_mean']:.6f}`",
                        f"- dominant_user_share_p95: `{metrics['dominant_user_share_p95']:.6f}`",
                    ]
                )
            lines.append("")

    lines.extend(
        [
            "## 3. Сводная таблица стратегий",
            "",
            "| strategy | label | nonzero_score_products | products_with_views | products_with_purchases | mean_popularity | mean_novelty | mean_final_score | max_final_score | top100_overlap_vs_baseline |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in comparison["summary_rows"]:
        lines.append(
            f"| {row['strategy']} | {row['label']} | {row['nonzero_score_products']} | "
            f"{row['products_with_views']} | {row['products_with_purchases']} | "
            f"{row['mean_popularity']:.6f} | {row['mean_novelty']:.6f} | "
            f"{row['mean_final_score']:.6f} | {row['max_final_score']:.6f} | "
            f"{row['top100_overlap_vs_baseline']} |"
        )

    lines.append("")
    lines.append("## 4. Витрины по стратегиям")
    lines.append("")
    for strategy, pages in comparison["showcases"].items():
        lines.append(f"### {strategy}")
        lines.append("")
        for page in PAGE_SPECS:
            payload = pages[page.slug]
            lines.append(f"#### {payload['title']} (count={payload['count']})")
            lines.append("")
            lines.append("| # | product_id | title | price | views | cart_adds | purchases | revenue | final_score |")
            lines.append("|---|---:|---|---:|---:|---:|---:|---:|---:|")
            for idx, item in enumerate(payload["top10_final_score"], start=1):
                lines.append(
                    f"| {idx} | {item['product_id']} | {item['title'].replace('|', '\\|')} | "
                    f"{item['price']:.2f} | {item['views']} | {item['cart_adds']} | "
                    f"{item['purchases']} | {item['revenue']:.2f} | {item['final_score']:.6f} |"
                )
            lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    as_of_date_raw = os.environ.get("PIPELINE_AS_OF_DATE")
    as_of_date = date.fromisoformat(as_of_date_raw) if as_of_date_raw else None
    score_window_days = _env_optional_int("PIPELINE_SCORE_WINDOW_DAYS")
    score_window_start = _env_optional_date("PIPELINE_SCORE_WINDOW_START")
    score_window_end = _env_optional_date("PIPELINE_SCORE_WINDOW_END")
    source_row_limit = _env_int("PIPELINE_SOURCE_ROW_LIMIT", 999999)
    event_row_limit = _env_int("PIPELINE_EVENT_ROW_LIMIT", 999999)
    event_file_limit = _env_int("PIPELINE_EVENT_FILE_LIMIT", 999)
    enable_seasonality = (os.environ.get("PIPELINE_ENABLE_SEASONALITY") or "").strip().lower() in {"1", "true", "yes", "y"}
    strategies_raw = (os.environ.get("PIPELINE_COMPARE_STRATEGIES") or "baseline,funnel,robust,commercial").strip()
    strategies = [part.strip().lower() for part in strategies_raw.split(",") if part.strip()]

    config = PipelineConfig.from_project_root(PROJECT_ROOT, as_of_date=as_of_date)
    loader, shop_df, cleaned_df, daily_events_df, event_builder, variant_lookup = _load_base_frames(
        config=config,
        source_row_limit=source_row_limit,
        event_row_limit=event_row_limit,
        event_file_limit=event_file_limit,
    )

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    cleaned_df.to_csv(config.cleaned_catalog_path, index=False, encoding="utf-8-sig")
    daily_events_df.to_csv(config.product_daily_events_path, index=False, encoding="utf-8-sig")

    product_event_frame = _build_product_event_frame(
        event_builder,
        variant_lookup,
        max_files=event_file_limit if event_file_limit < 999 else None,
        max_rows_per_file=event_row_limit if event_row_limit < 999999 else None,
        as_of_date=config.as_of_date,
    )

    seasonality = _seasonality_diagnostics(
        cleaned_df,
        daily_events_df,
        as_of_date=config.as_of_date,
        window_days=score_window_days,
        window_start=score_window_start,
        window_end=score_window_end,
    )
    user_noise = _user_noise_diagnostics(product_event_frame)
    comparison = _strategy_comparison(
        cleaned_df,
        daily_events_df,
        as_of_date=config.as_of_date,
        window_days=score_window_days,
        window_start=score_window_start,
        window_end=score_window_end,
        strategies=strategies,
        enable_seasonality=enable_seasonality,
    )

    summary_df = pd.DataFrame(comparison["summary_rows"])
    summary_df.to_csv(ARTIFACTS_DIR / "score_strategy_comparison.csv", index=False, encoding="utf-8-sig")

    payload = {
        "as_of_date": config.as_of_date.isoformat() if config.as_of_date else None,
        "window": {
            "days": score_window_days,
            "start": score_window_start.isoformat() if score_window_start else None,
            "end": score_window_end.isoformat() if score_window_end else (config.as_of_date.isoformat() if config.as_of_date else None),
        },
        "enable_seasonality": enable_seasonality,
        "strategies": strategies,
        "seasonality": seasonality,
        "user_noise": user_noise,
        "comparison": comparison,
    }
    (ARTIFACTS_DIR / "score_strategy_analysis.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_markdown_report(
        output_path=ARTIFACTS_DIR / "score_strategy_analysis.md",
        seasonality=seasonality,
        user_noise=user_noise,
        comparison=comparison,
    )

    print("[Analysis] Saved:")
    print(f"  - {ARTIFACTS_DIR / 'score_strategy_comparison.csv'}")
    print(f"  - {ARTIFACTS_DIR / 'score_strategy_analysis.json'}")
    print(f"  - {ARTIFACTS_DIR / 'score_strategy_analysis.md'}")
    for strategy in strategies:
        print(f"  - {ARTIFACTS_DIR / f'scored_catalog_{strategy}.csv'}")


if __name__ == "__main__":
    main()
