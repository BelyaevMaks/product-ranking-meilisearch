"""
Feature Engine - Ranking score calculation for product catalog.

Implements the scoring formulas:
- Popularity: (views * 0.3 + purchases * 0.7) * exp(-λ * age_days)
- Novelty: -log2((purchases + 1) / (total_purchases + 1))
- Final Score: log1p(popularity) * (novelty / 14) * boost

This module is designed to work with the pipeline architecture,
taking cleaned data and producing scored products ready for indexing.
"""

import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ScoreStrategy:
    code: str
    label: str
    use_log_views: bool
    views_weight: float
    purchases_weight: float
    cart_weight: float = 0.0
    use_unique_signals: bool = False
    unique_viewers_weight: float = 0.0
    unique_cart_weight: float = 0.0
    unique_buyers_weight: float = 0.0
    commercial_purchase_weight: float = 0.0
    commercial_units_weight: float = 0.0
    commercial_revenue_weight: float = 0.0


SCORE_STRATEGIES: Dict[str, ScoreStrategy] = {
    "baseline": ScoreStrategy(
        code="baseline",
        label="A. Консервативный спрос",
        use_log_views=False,
        views_weight=0.3,
        purchases_weight=0.7,
    ),
    "funnel": ScoreStrategy(
        code="funnel",
        label="B. Воронка интереса",
        use_log_views=True,
        views_weight=0.2,
        purchases_weight=0.5,
        cart_weight=0.3,
    ),
    "robust": ScoreStrategy(
        code="robust",
        label="C. Устойчивый к шуму",
        use_log_views=True,
        views_weight=0.0,
        purchases_weight=0.2,
        use_unique_signals=True,
        unique_viewers_weight=0.15,
        unique_cart_weight=0.30,
        unique_buyers_weight=0.35,
    ),
    "commercial": ScoreStrategy(
        code="commercial",
        label="D. Коммерческий",
        use_log_views=True,
        views_weight=0.15,
        purchases_weight=0.0,
        commercial_purchase_weight=0.4,
        commercial_units_weight=0.2,
        commercial_revenue_weight=0.4,
    ),
}


class FeatureEngine:
    """Calculates ranking scores based on product events and attributes."""
    
    def __init__(
        self,
        catalog_df: pd.DataFrame,
        events_df: Optional[pd.DataFrame] = None,
        as_of_date: Optional[date] = None,
        window_days: Optional[int] = None,
        window_start: Optional[date] = None,
        window_end: Optional[date] = None,
        strategy: str = "baseline",
        enable_seasonality: bool = False,
    ):
        self.catalog = catalog_df.copy()
        self.events = events_df
        self.as_of_date = as_of_date or date.today()
        self.window_days = window_days if window_days and window_days > 0 else None
        self.window_start = window_start
        self.window_end = window_end
        if strategy not in SCORE_STRATEGIES:
            raise ValueError(f"Unknown score strategy: {strategy}")
        self.strategy = SCORE_STRATEGIES[strategy]
        self.enable_seasonality = enable_seasonality
        
        # Scoring parameters (baseline defaults align with business example)
        # lambda = ln(2) / half_life_days
        self.popularity_half_life_days = self._read_positive_float_env("POPULARITY_HALF_LIFE", 30.0)
        self.lambda_decay = np.log(2.0) / self.popularity_half_life_days
        self.novelty_divisor = 14.0  # Novelty normalization factor
        self.novelty_weight = self._read_positive_float_env("NOVELTY_WEIGHT", 1.0)

        # Boost configuration
        self.boost_in_stock = self._read_positive_float_env("BOOST_IN_STOCK", 1.5)
        self.boost_featured = self._read_positive_float_env("BOOST_FEATURED", 1.3)
        self.boost_sale = self._read_positive_float_env("BOOST_SALE", 1.2)
        self.boost_out_of_stock = self._read_positive_float_env("BOOST_OUT_OF_STOCK", 0.05)
        self.category_boosts = self._read_category_boosts_env("BOOST_CATEGORY_MAP")

    @staticmethod
    def available_strategies() -> Dict[str, str]:
        return {code: strategy.label for code, strategy in SCORE_STRATEGIES.items()}

    @staticmethod
    def _read_positive_float_env(name: str, default: float) -> float:
        raw = (os.environ.get(name) or "").strip()
        if not raw:
            return float(default)
        try:
            value = float(raw)
            return value if value > 0 else float(default)
        except ValueError:
            return float(default)

    @staticmethod
    def _read_category_boosts_env(name: str) -> Dict[str, float]:
        raw = (os.environ.get(name) or "").strip()
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                return {}
            normalized: Dict[str, float] = {}
            for key, value in payload.items():
                try:
                    parsed = float(value)
                    if parsed > 0:
                        normalized[str(key).strip().lower()] = parsed
                except (TypeError, ValueError):
                    continue
            return normalized
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def _to_bool(value: object, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if pd.isna(value):
            return default
        return str(value).strip().lower() in {"true", "1", "yes", "y"}

    def resolve_event_window(self) -> Tuple[Optional[date], Optional[date]]:
        """Resolve the event window used for score calculation.

        Priority:
        1. Explicit window_start / window_end
        2. Rolling window_days ending at as_of_date
        3. Full history up to as_of_date
        """
        end_date = self.window_end or self.as_of_date
        if self.as_of_date is not None and end_date is not None and end_date > self.as_of_date:
            end_date = self.as_of_date

        if self.window_start is not None or self.window_end is not None:
            start_date = self.window_start
            if start_date is not None and end_date is not None and start_date > end_date:
                raise ValueError(
                    f"Invalid score window: start {start_date.isoformat()} > end {end_date.isoformat()}"
                )
            return start_date, end_date

        if self.window_days is not None and end_date is not None:
            start_date = end_date - timedelta(days=self.window_days - 1)
            return start_date, end_date

        return None, end_date
    
    def aggregate_events(self) -> Dict[str, pd.Series]:
        """Aggregate event data by product_id.

        Supports:
        - raw event table with `event_type`
        - daily aggregated event table with `views` / `purchases`
        """
        if self.events is None or len(self.events) == 0:
            print("[FeatureEngine] No product-level events provided, using catalog-prior fallback")
            product_ids = self.catalog["product_id"]
            in_stock = self.catalog.get("in_stock", pd.Series(False, index=self.catalog.index)).astype(bool)
            is_sale = self.catalog.get("is_sale", pd.Series(False, index=self.catalog.index)).astype(bool)
            discount = pd.to_numeric(
                self.catalog.get("discount", pd.Series(0.0, index=self.catalog.index)),
                errors="coerce",
            ).fillna(0.0)

            views = (in_stock.astype(int) * 10 + is_sale.astype(int) * 5 + (discount * 20)).round().astype(int)
            purchases = (is_sale.astype(int) + (in_stock & is_sale).astype(int)).astype(int)
            zeros = pd.Series(0, index=product_ids, dtype="int64")
            zeros_float = pd.Series(0.0, index=product_ids, dtype="float64")
            return {
                "views": pd.Series(views.to_numpy(), index=product_ids, dtype="int64"),
                "cart_adds": zeros,
                "cart_removes": zeros,
                "purchases": pd.Series(purchases.to_numpy(), index=product_ids, dtype="int64"),
                "units_purchased": pd.Series(purchases.to_numpy(), index=product_ids, dtype="int64"),
                "revenue": zeros_float,
                "unique_customers": zeros,
                "unique_viewers": zeros,
                "unique_cart_customers": zeros,
                "unique_buyers": zeros,
            }

        window_start, window_end = self.resolve_event_window()
        if window_start or window_end:
            start_repr = window_start.isoformat() if window_start else "(-inf)"
            end_repr = window_end.isoformat() if window_end else "(+inf)"
            print(f"[FeatureEngine] Event window: {start_repr} .. {end_repr}")

        aggregate_columns = [
            "views",
            "cart_adds",
            "cart_removes",
            "purchases",
            "units_purchased",
            "revenue",
            "unique_customers",
            "unique_viewers",
            "unique_cart_customers",
            "unique_buyers",
        ]
        if {"event_date", "product_id", "views", "purchases"}.issubset(self.events.columns):
            events_frame = self.events.copy()
            if "event_date" in events_frame.columns:
                events_frame = events_frame.dropna(subset=["event_date"])
                events_frame["event_date"] = pd.to_datetime(events_frame["event_date"], errors="coerce").dt.date
                events_frame = events_frame.dropna(subset=["event_date"])
                if window_end is not None:
                    events_frame = events_frame.loc[events_frame["event_date"] <= window_end].copy()
                if window_start is not None:
                    events_frame = events_frame.loc[events_frame["event_date"] >= window_start].copy()
            for column in aggregate_columns:
                if column not in events_frame.columns:
                    events_frame[column] = 0
            aggregated = (
                events_frame.groupby("product_id", dropna=False)[aggregate_columns]
                .sum()
                .reset_index()
            )
            aggregated = aggregated.set_index("product_id")
            aggregated = aggregated.apply(pd.to_numeric, errors="coerce").fillna(0)
        else:
            views_per_product = (
                self.events[self.events['event_type'] == 'view']
                .groupby('product_id')
                .size()
                .rename('views')
            )
            purchases_per_product = (
                self.events[self.events['event_type'] == 'purchase']
                .groupby('product_id')
                .size()
                .rename('purchases')
            )
            zeros_index = views_per_product.index.union(purchases_per_product.index)
            aggregated = pd.DataFrame(index=zeros_index)
            aggregated["views"] = views_per_product.reindex(zeros_index).fillna(0).astype(int)
            aggregated["purchases"] = purchases_per_product.reindex(zeros_index).fillna(0).astype(int)
            for column in aggregate_columns:
                if column not in aggregated.columns:
                    aggregated[column] = 0
        
        views_per_product = aggregated["views"].astype(int)
        purchases_per_product = aggregated["purchases"].astype(int)
        print(
            f"[FeatureEngine] Events aggregated: {len(views_per_product)} products with views, "
            f"{len(purchases_per_product)} products with purchases"
        )
        
        return {column: aggregated[column] for column in aggregate_columns}
    
    def calculate_age_days(self) -> pd.Series:
        """Calculate product age in days from shopify_handle.
        
        Extracts year from handle pattern (e.g., "tshirt-ss-25" → 2025).
        Falls back to 180 days if parsing fails.
        
        Returns:
            Series with age_days for each product
        """
        def _parse_age(handle):
            try:
                parts = str(handle).split('-')
                for part in parts:
                    if part.isdigit() and len(part) == 2:
                        year = int(part)
                        base_year = 2020 if year >= 24 else 2000
                        product_year = base_year + (year - (base_year % 100))
                        
                        # Estimate month from position in handle
                        try:
                            idx = parts.index(part)
                            month_parts = [p for p in parts[idx+1:] if p.isdigit()]
                            month = int(month_parts[0]) if month_parts else 6
                            product_date = datetime(product_year, min(max(month, 1), 12), 15)
                            observation_dt = datetime.combine(self.as_of_date, datetime.min.time())
                            age_days = (observation_dt - product_date).days
                            return max(0, age_days)
                        except:
                            pass
            except Exception as e:
                print(f"[FeatureEngine] Warning: Could not parse date from handle '{handle}': {e}")
            
            return 180.0
        
        if 'created_at' in self.catalog.columns:
            created = pd.to_datetime(self.catalog['created_at'], errors='coerce')
            observation_dt = pd.Timestamp(self.as_of_date)
            age_days = (observation_dt - created).dt.days
            if age_days.notna().any():
                return age_days.fillna(180).clip(lower=0)

        if 'season_code' in self.catalog.columns:
            season_map = {"SS": 3, "AW": 8, "FW": 9, "HO": 10}

            def _from_season(code):
                match = re.fullmatch(r"(SS|AW|HO|FW)-(\d{2})", str(code).strip().upper())
                if not match:
                    return 180.0
                season, year_suffix = match.groups()
                year = 2000 + int(year_suffix)
                product_date = datetime(year, season_map.get(season, 6), 1)
                observation_dt = datetime.combine(self.as_of_date, datetime.min.time())
                return max(0, (observation_dt - product_date).days)

            age_days = self.catalog['season_code'].apply(_from_season)
            if age_days.notna().any():
                return age_days

        if 'shopify_handle' in self.catalog.columns:
            age_days = self.catalog['shopify_handle'].apply(_parse_age)
        elif 'age_days' in self.catalog.columns:
            print("[FeatureEngine] Using existing age_days column")
            age_days = self.catalog['age_days']
        else:
            print("[FeatureEngine] No date info available, using default 180 days")
            age_days = pd.Series(180.0, index=self.catalog.index)
        
        return age_days

    def update_new_flag(self, threshold_days: int = 365) -> None:
        """Refresh is_new after age_days is available."""
        if 'age_days' not in self.catalog.columns:
            self.catalog['is_new'] = False
        else:
            self.catalog['is_new'] = self.catalog['age_days'] < threshold_days

    @staticmethod
    def _season_group_for_month(month: int) -> str:
        if month in {2, 3, 4, 5, 6, 7}:
            return "SS"
        if month in {8, 9, 10, 11, 12, 1}:
            return "AW"
        return "UNKNOWN"

    def calculate_seasonal_boost(self, season_code: str) -> float:
        if not self.enable_seasonality:
            return 1.0
        season_code = str(season_code or "").strip().upper()
        if not season_code:
            return 1.0
        match = re.fullmatch(r"(SS|AW|HO|FW|SU)-(\d{2})", season_code)
        if not match:
            return 1.0

        season, _ = match.groups()
        season_group = "SS" if season in {"SS", "SU"} else "AW"
        current_group = self._season_group_for_month(self.as_of_date.month)
        if season_group == current_group:
            return 1.15
        return 0.90

    @staticmethod
    def _views_signal(views: float, use_log_views: bool) -> float:
        return float(np.log1p(max(views, 0.0))) if use_log_views else float(max(views, 0.0))

    @staticmethod
    def _net_cart(cart_adds: float, cart_removes: float) -> float:
        return float(max(cart_adds - cart_removes, 0.0))

    def calculate_popularity_from_signals(self, row: pd.Series) -> float:
        views = float(row.get("views", 0.0))
        purchases = float(row.get("purchases", 0.0))
        cart_adds = float(row.get("cart_adds", 0.0))
        cart_removes = float(row.get("cart_removes", 0.0))
        units_purchased = float(row.get("units_purchased", 0.0))
        revenue = float(row.get("revenue", 0.0))
        unique_viewers = float(row.get("unique_viewers", 0.0))
        unique_cart_customers = float(row.get("unique_cart_customers", 0.0))
        unique_buyers = float(row.get("unique_buyers", 0.0))
        age_days = float(row.get("age_days", 180.0))

        strategy = self.strategy
        views_signal = self._views_signal(views, strategy.use_log_views)
        net_cart = self._net_cart(cart_adds, cart_removes)

        if strategy.code == "robust":
            base_score = (
                unique_viewers * strategy.unique_viewers_weight
                + unique_cart_customers * strategy.unique_cart_weight
                + unique_buyers * strategy.unique_buyers_weight
                + purchases * strategy.purchases_weight
            )
        elif strategy.code == "commercial":
            commercial_signal = (
                purchases * strategy.commercial_purchase_weight
                + units_purchased * strategy.commercial_units_weight
                + np.log1p(max(revenue, 0.0)) * strategy.commercial_revenue_weight
            )
            base_score = views_signal * strategy.views_weight + commercial_signal
        else:
            base_score = (
                views_signal * strategy.views_weight
                + net_cart * strategy.cart_weight
                + purchases * strategy.purchases_weight
            )

        decay_factor = np.exp(-self.lambda_decay * age_days)
        return float(base_score * decay_factor)
    
    def calculate_popularity(self, views: float, purchases: float, age_days: float) -> float:
        """Calculate popularity score with exponential time decay.
        
        Formula: (views * 0.3 + purchases * 0.7) * exp(-λ * age_days)
        
        Args:
            views: Number of product views
            purchases: Number of purchases
            age_days: Age of product in days
            
        Returns:
            Popularity score (float)
        """
        base_score = views * 0.3 + purchases * 0.7
        decay_factor = np.exp(-self.lambda_decay * age_days)
        
        return float(base_score * decay_factor)
    
    def calculate_novelty(self, purchases: int, total_purchases: int) -> float:
        """Calculate novelty score for cold-start products.
        
        Formula: -log2((purchases + 1) / (total_purchases + 1))
        
        Higher values indicate more novel/rare products.
        Products with fewer purchases get higher novelty scores.
        
        Args:
            purchases: Product-specific purchase count
            total_purchases: Total purchases across all products
            
        Returns:
            Novelty score (float)
        """
        if total_purchases <= 0:
            return 1.0
        
        ratio = (purchases + 1) / (total_purchases + 1)
        novelty = -np.log2(ratio)
        
        return float(novelty)
    
    def calculate_boost(self, row: pd.Series, age_days: float) -> float:
        """Calculate boost multiplier based on product attributes.
        
        Boost factors (multiplicative):
        - In stock: BOOST_IN_STOCK (default ×1.5)
        - Featured flag: BOOST_FEATURED (default ×1.3)
        - On sale: BOOST_SALE (default ×1.2)
        - Out of stock: BOOST_OUT_OF_STOCK (default ×0.05)
        - Category boost: BOOST_CATEGORY_MAP (default ×1.0)
        
        Args:
            row: Product row from DataFrame
            age_days: Age of product in days
            
        Returns:
            Boost multiplier (float)
        """
        boost = 1.0

        # In-stock / out-of-stock multiplier
        in_stock = self._to_bool(row.get("in_stock", True), default=True)
        if in_stock:
            boost *= self.boost_in_stock
        else:
            boost *= self.boost_out_of_stock

        # Featured boost (manual promotion signal, if field exists in catalog)
        is_featured = self._to_bool(row.get("is_featured", row.get("featured", False)), default=False)
        if is_featured:
            boost *= self.boost_featured

        # Sale boost (prefer explicit is_sale, fallback to old_price > price)
        is_sale = self._to_bool(row.get("is_sale", False), default=False)
        if not is_sale:
            old_price = row.get("compare_at_price", None) or row.get("old_price", None)
            current_price = row.get("price", 0)
            try:
                is_sale = pd.notna(old_price) and float(old_price) > float(current_price)
            except (ValueError, TypeError):
                is_sale = False
        if is_sale:
            boost *= self.boost_sale

        # Category-specific manual coefficient
        category_name = str(row.get("category_name", "")).strip().lower()
        if category_name and category_name in self.category_boosts:
            boost *= self.category_boosts[category_name]

        boost *= self.calculate_seasonal_boost(row.get("season_code", ""))
        
        return boost
    
    def calculate_final_score(self, popularity: float, novelty: float, boost: float) -> float:
        """Calculate final ranking score.
        
        Formula: log1p(popularity) * (novelty / 14) * boost
        
        Args:
            popularity: Popularity score
            novelty: Novelty score
            boost: Boost multiplier
            
        Returns:
            Final ranking score (float)
        """
        if popularity <= 0 and novelty == 0:
            return 0.0
        
        final = np.log1p(popularity) * ((novelty * self.novelty_weight) / self.novelty_divisor) * boost
        return float(final)
    
    def compute_scores(self) -> pd.DataFrame:
        """Compute all ranking scores for the entire catalog.
        
        Pipeline:
        1. Aggregate events by product_id
        2. Calculate age_days for each product
        3. For each product: calculate popularity, novelty, boost, final_score
        
        Returns:
            DataFrame with added score columns: views, purchases, age_days, 
            popularity, novelty, boost, final_score
        """
        print("[FeatureEngine] Starting scoring pipeline...")
        
        # Aggregate events
        event_data = self.aggregate_events()
        views_series = event_data['views']
        purchases_series = event_data['purchases']
        
        total_purchases = int(purchases_series.sum()) if len(purchases_series) > 0 else 1
        print(f"[FeatureEngine] Total catalog purchases: {total_purchases}")
        
        # Calculate age for all products at once (vectorized)
        self.catalog['age_days'] = self.calculate_age_days()
        self.update_new_flag()
        
        # Merge event data into catalog
        integer_event_cols = {
            "views",
            "cart_adds",
            "cart_removes",
            "purchases",
            "units_purchased",
            "unique_customers",
            "unique_viewers",
            "unique_cart_customers",
            "unique_buyers",
        }
        for column, series in event_data.items():
            mapped = self.catalog['product_id'].map(series).fillna(0)
            if column in integer_event_cols:
                self.catalog[column] = pd.to_numeric(mapped, errors="coerce").fillna(0).astype(int)
            else:
                self.catalog[column] = pd.to_numeric(mapped, errors="coerce").fillna(0.0)
        
        print(f"[FeatureEngine] Products with views: {(self.catalog['views'] > 0).sum()}")
        print(f"[FeatureEngine] Products with purchases: {(self.catalog['purchases'] > 0).sum()}")
        
        # Calculate scores row by row (cannot be fully vectorized due to boost calculation)
        results = []
        for idx, row in self.catalog.iterrows():
            views = int(row.get('views', 0))
            purchases = int(row.get('purchases', 0))
            age_days = float(row.get('age_days', 180.0))
            
            # Calculate individual scores
            popularity = self.calculate_popularity_from_signals(row)
            novelty = self.calculate_novelty(purchases, total_purchases)
            boost = self.calculate_boost(row, age_days)
            final_score = self.calculate_final_score(popularity, novelty, boost)
            
            results.append({
                'popularity': round(popularity, 6),
                'novelty': round(novelty, 4),
                'boost': round(boost, 3),
                'final_score': round(final_score, 6)
            })
        
        scores_df = pd.DataFrame(results, index=self.catalog.index)
        
        # Merge score columns directly (avoids concat duplicate index issues)
        for col in ['popularity', 'novelty', 'boost', 'final_score']:
            self.catalog[col] = pd.to_numeric(scores_df[col], errors='coerce')
        
        # Round numeric columns for readability
        score_cols = ['popularity', 'novelty', 'boost', 'final_score']
        for col in score_cols:
            if col in self.catalog.columns:
                self.catalog[col] = self.catalog[col].round(6)
        
        print(f"[FeatureEngine] Scoring complete:")
        pop_min = float(self.catalog['popularity'].min())
        pop_max = float(self.catalog['popularity'].max())
        nov_min = float(self.catalog['novelty'].min())
        nov_max = float(self.catalog['novelty'].max())
        fin_min = float(self.catalog['final_score'].min())
        fin_max = float(self.catalog['final_score'].max())
        print(f"  - Popularity range: {pop_min:.4f} - {pop_max:.4f}")
        print(f"  - Novelty range: {nov_min:.4f} - {nov_max:.4f}")
        print(f"  - Final score range: {fin_min:.6f} - {fin_max:.6f}\n")
        
        return self.catalog
    
    def get_ranked_products(
        self, 
        sort_by: str = 'final_score', 
        ascending: bool = False,
        limit: Optional[int] = None,
        filters: Optional[Dict[str, any]] = None
    ) -> pd.DataFrame:
        """Get products ranked by specified metric with optional filters.
        
        Args:
            sort_by: Column to sort by ('final_score', 'popularity', 'novelty')
            ascending: Sort order (False for descending - highest first)
            limit: Optional max number of results
            filters: Optional dict of custom filters
            
        Returns:
            Ranked DataFrame with rank column added
        """
        df = self.catalog.copy()
        
        if sort_by not in df.columns:
            raise ValueError(f"Invalid sort_by column: {sort_by}")
        
        # Apply filters if provided
        if filters:
            for key, value in filters.items():
                if key == 'category':
                    df = df[df['category_name'].str.contains(value, case=False, na=False)]
                elif key == 'gender':
                    df = df[df['gender'] == value]
                elif key == 'min_price':
                    df = df[df['price'] >= float(value)]
                elif key == 'max_price':
                    df = df[df['price'] <= float(value)]
                elif key == 'is_new':
                    df = df[df['is_new'] == value]
                elif key == 'is_sale':
                    df = df[df['is_sale'] == value]
        
        # Sort and rank
        ranked_df = df.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)
        ranked_df.insert(0, 'rank', range(1, len(ranked_df) + 1))
        
        if limit:
            ranked_df = ranked_df.head(limit)
        
        return ranked_df
