"""
Event stream helpers for Mindbox JSON customer actions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import pandas as pd

from .event_intents import classify_event_intent
from .raw_data_loader import RawDataLoader


@dataclass
class EventRecord:
    """Normalized event record derived from JSON customer actions."""

    event_time_utc: Optional[datetime]
    event_date: Optional[str]
    customer_mindbox_id: str
    action_template_name: str
    action_template_system_name: str
    intent: str
    channel: str
    product_insales_id: str
    category_insales_id: str
    quantity: float
    revenue: float


class EventStreamBuilder:
    """Build normalized events and daily aggregates from JSON action logs."""

    def __init__(self, loader: RawDataLoader):
        self.loader = loader

    @staticmethod
    def _parse_event_time(value: str) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _safe_str(value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _safe_float(value: object) -> float:
        if value in (None, ""):
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def iter_normalized_events(
        self,
        *,
        max_files: Optional[int] = None,
        max_rows_per_file: Optional[int] = None,
    ) -> Iterable[EventRecord]:
        for action in self.loader.iter_event_objects(
            max_files=max_files,
            max_rows_per_file=max_rows_per_file,
        ):
            template = action.get("actionTemplate", {}) or {}
            template_ids = template.get("ids", {}) or {}
            template_name = self._safe_str(template.get("name"))
            template_system = self._safe_str(template_ids.get("systemName"))
            customer = action.get("customer", {}) or {}
            customer_ids = customer.get("ids", {}) or {}
            channel = action.get("channel", {}) or {}
            channel_ids = channel.get("ids", {}) or {}
            channel_external_id = self._safe_str(channel_ids.get("externalId") or channel_ids.get("systemName"))
            event_time = self._parse_event_time(self._safe_str(action.get("dateTimeUtc")))
            event_date = event_time.date().isoformat() if event_time else None
            intent = classify_event_intent(template_name, template_system)
            customer_id = self._safe_str(customer_ids.get("mindboxId"))

            if intent == "purchase":
                order = action.get("order", {}) or {}
                for line in order.get("lines", []) or []:
                    product = line.get("product", {}) or {}
                    product_ids = product.get("ids", {}) or {}
                    yield EventRecord(
                        event_time_utc=event_time,
                        event_date=event_date,
                        customer_mindbox_id=customer_id,
                        action_template_name=template_name,
                        action_template_system_name=template_system,
                        intent="purchase",
                        channel=channel_external_id,
                        product_insales_id=self._safe_str(product_ids.get("insalesId")),
                        category_insales_id="",
                        quantity=self._safe_float(line.get("quantity") or 1.0),
                        revenue=self._safe_float(line.get("priceOfLine")),
                    )
                continue

            products = action.get("products", []) or []
            if products:
                for product in products:
                    product_ids = (product or {}).get("ids", {}) or {}
                    yield EventRecord(
                        event_time_utc=event_time,
                        event_date=event_date,
                        customer_mindbox_id=customer_id,
                        action_template_name=template_name,
                        action_template_system_name=template_system,
                        intent=intent,
                        channel=channel_external_id,
                        product_insales_id=self._safe_str(product_ids.get("insalesId")),
                        category_insales_id="",
                        quantity=1.0,
                        revenue=0.0,
                    )
                continue

            categories = action.get("productCategories", []) or []
            if categories:
                for category in categories:
                    category_ids = (category or {}).get("ids", {}) or {}
                    yield EventRecord(
                        event_time_utc=event_time,
                        event_date=event_date,
                        customer_mindbox_id=customer_id,
                        action_template_name=template_name,
                        action_template_system_name=template_system,
                        intent=intent,
                        channel=channel_external_id,
                        product_insales_id="",
                        category_insales_id=self._safe_str(category_ids.get("insalesId")),
                        quantity=1.0,
                        revenue=0.0,
                    )

    def to_frame(
        self,
        *,
        max_files: Optional[int] = None,
        max_rows_per_file: Optional[int] = None,
    ) -> pd.DataFrame:
        rows: List[Dict[str, object]] = []
        for event in self.iter_normalized_events(
            max_files=max_files,
            max_rows_per_file=max_rows_per_file,
        ):
            rows.append(
                {
                    "event_time_utc": event.event_time_utc,
                    "event_date": event.event_date,
                    "customer_mindbox_id": event.customer_mindbox_id,
                    "action_template_name": event.action_template_name,
                    "action_template_system_name": event.action_template_system_name,
                    "intent": event.intent,
                    "channel": event.channel,
                    "product_insales_id": event.product_insales_id,
                    "category_insales_id": event.category_insales_id,
                    "quantity": event.quantity,
                    "revenue": event.revenue,
                }
            )
        return pd.DataFrame(rows)

    def aggregate_daily_intents(
        self,
        *,
        max_files: Optional[int] = None,
        max_rows_per_file: Optional[int] = None,
    ) -> pd.DataFrame:
        frame = self.to_frame(max_files=max_files, max_rows_per_file=max_rows_per_file)
        if frame.empty:
            return pd.DataFrame(columns=["event_date", "intent", "events_count"])

        grouped = (
            frame.dropna(subset=["event_date"])
            .groupby(["event_date", "intent"], dropna=False)
            .size()
            .reset_index(name="events_count")
            .sort_values(["event_date", "events_count"], ascending=[True, False])
            .reset_index(drop=True)
        )
        return grouped

    def aggregate_daily_products(
        self,
        variant_to_product: Dict[str, int],
        *,
        max_files: Optional[int] = None,
        max_rows_per_file: Optional[int] = None,
        as_of_date: Optional[str] = None,
    ) -> pd.DataFrame:
        frame = self.to_frame(max_files=max_files, max_rows_per_file=max_rows_per_file)
        if frame.empty:
            return pd.DataFrame(
                columns=[
                    "event_date",
                    "product_id",
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
            )

        product_frame = frame[frame["product_insales_id"].astype(str).str.strip() != ""].copy()
        if product_frame.empty:
            return pd.DataFrame(
                columns=[
                    "event_date",
                    "product_id",
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
            )

        product_frame["product_id"] = product_frame["product_insales_id"].map(variant_to_product)
        product_frame = product_frame.dropna(subset=["product_id", "event_date"]).copy()
        if product_frame.empty:
            return pd.DataFrame(
                columns=[
                    "event_date",
                    "product_id",
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
            )

        if as_of_date:
            product_frame = product_frame.loc[product_frame["event_date"] <= as_of_date].copy()

        product_frame["product_id"] = product_frame["product_id"].astype(int)
        product_frame["is_view"] = product_frame["intent"].eq("product_view").astype(int)
        product_frame["is_cart_add"] = product_frame["intent"].eq("cart_add").astype(int)
        product_frame["is_cart_remove"] = product_frame["intent"].eq("cart_remove").astype(int)
        product_frame["is_purchase"] = product_frame["intent"].eq("purchase").astype(int)
        product_frame["viewer_customer_id"] = product_frame.apply(
            lambda row: row["customer_mindbox_id"] if row["intent"] == "product_view" else "",
            axis=1,
        )
        product_frame["cart_customer_id"] = product_frame.apply(
            lambda row: row["customer_mindbox_id"] if row["intent"] == "cart_add" else "",
            axis=1,
        )
        product_frame["buyer_customer_id"] = product_frame.apply(
            lambda row: row["customer_mindbox_id"] if row["intent"] == "purchase" else "",
            axis=1,
        )
        product_frame["purchase_units"] = product_frame.apply(
            lambda row: row["quantity"] if row["intent"] == "purchase" else 0.0,
            axis=1,
        )
        product_frame["purchase_revenue"] = product_frame.apply(
            lambda row: row["revenue"] if row["intent"] == "purchase" else 0.0,
            axis=1,
        )

        grouped = (
            product_frame.groupby(["event_date", "product_id"], dropna=False)
            .agg(
                views=("is_view", "sum"),
                cart_adds=("is_cart_add", "sum"),
                cart_removes=("is_cart_remove", "sum"),
                purchases=("is_purchase", "sum"),
                units_purchased=("purchase_units", "sum"),
                revenue=("purchase_revenue", "sum"),
                unique_customers=("customer_mindbox_id", lambda values: values.astype(str).nunique()),
                unique_viewers=("viewer_customer_id", lambda values: values[values.astype(str).str.strip() != ""].astype(str).nunique()),
                unique_cart_customers=("cart_customer_id", lambda values: values[values.astype(str).str.strip() != ""].astype(str).nunique()),
                unique_buyers=("buyer_customer_id", lambda values: values[values.astype(str).str.strip() != ""].astype(str).nunique()),
            )
            .reset_index()
            .sort_values(["event_date", "product_id"])
            .reset_index(drop=True)
        )
        return grouped

    def aggregate_daily_categories(
        self,
        category_lookup: Dict[str, str],
        *,
        max_files: Optional[int] = None,
        max_rows_per_file: Optional[int] = None,
        as_of_date: Optional[str] = None,
    ) -> pd.DataFrame:
        frame = self.to_frame(max_files=max_files, max_rows_per_file=max_rows_per_file)
        if frame.empty:
            return pd.DataFrame(
                columns=[
                    "event_date",
                    "category_insales_id",
                    "category_name",
                    "category_views",
                    "unique_customers",
                ]
            )

        category_frame = frame[frame["category_insales_id"].astype(str).str.strip() != ""].copy()
        if category_frame.empty:
            return pd.DataFrame(
                columns=[
                    "event_date",
                    "category_insales_id",
                    "category_name",
                    "category_views",
                    "unique_customers",
                ]
            )

        if as_of_date:
            category_frame = category_frame.loc[category_frame["event_date"] <= as_of_date].copy()

        category_frame = category_frame.loc[category_frame["intent"].eq("category_view")].copy()
        if category_frame.empty:
            return pd.DataFrame(
                columns=[
                    "event_date",
                    "category_insales_id",
                    "category_name",
                    "category_views",
                    "unique_customers",
                ]
            )

        category_frame["category_name"] = category_frame["category_insales_id"].map(
            lambda value: category_lookup.get(str(value).strip(), "")
        )
        grouped = (
            category_frame.groupby(["event_date", "category_insales_id", "category_name"], dropna=False)
            .agg(
                category_views=("intent", "size"),
                unique_customers=("customer_mindbox_id", lambda values: values.astype(str).nunique()),
            )
            .reset_index()
            .sort_values(["event_date", "category_views"], ascending=[True, False])
            .reset_index(drop=True)
        )
        return grouped
