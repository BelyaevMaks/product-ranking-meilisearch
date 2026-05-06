"""
Kixbox Ranking API Service — FastAPI-based product ranking and search service.

Integrates with Meilisearch to provide:
- Health checks
- Trending products (by final_score)
- New arrivals (filtered by is_new, sorted by novelty)
- Sale items (filtered by is_sale, sorted by final_score)
- Full-text search with category/gender filters

Usage:
    pip install fastapi uvicorn meilisearch pydantic
    python src/api_service.py

Meilisearch indexes (variant «один индекс на стратегию»):
    MEILISEARCH_INDEX_BASE=kixbox_products → kixbox_products_baseline, …_funnel, …_robust, …_commercial
    MEILISEARCH_DEFAULT_STRATEGY=baseline — индекс по умолчанию, если ?strategy не передан
    MEILISEARCH_INDEX_OVERRIDE=… — один UID для всех запросов (legacy / отладка)

Залить все стратегии: python src/index_all_strategy_indexes.py
"""

import logging
import os
import sys
import time
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

import meilisearch
import pandas as pd
import requests as http_requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.meilisearch_strategy_indexes import (
    get_default_strategy,
    get_index_base,
    get_index_override,
    resolve_index_uid,
    strategy_labels,
    normalize_strategy,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MEILISEARCH_URL = os.environ.get("MEILISEARCH_URL", "http://localhost:7700")
MEILISEARCH_MASTER_KEY = os.environ.get(
    "MEILISEARCH_MASTER_KEY",
    "Yu4HfgPAV9gWM5nrutsfJi0b0RsXSFK8tiPw8Zx__Co",
)
SCORED_CATALOG_PATH = PROJECT_ROOT / "artifacts" / "scored_catalog.csv"
PIPELINE_RUN_METADATA_PATH = PROJECT_ROOT / "artifacts" / "pipeline_run_metadata.json"


def _primary_scored_catalog_path() -> Path:
    """Prefer per-strategy CSV for UI totals; fall back to legacy scored_catalog.csv."""
    candidate = PROJECT_ROOT / "artifacts" / f"scored_catalog_{get_default_strategy()}.csv"
    if candidate.exists():
        return candidate
    return SCORED_CATALOG_PATH


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _escape_filter_value(value: str) -> str:
    """Escape single quotes for Meilisearch filter strings."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _resolve_score_window_metadata() -> Dict[str, Optional[str]]:
    """Describe the score window used to build the current scored catalog."""
    if PIPELINE_RUN_METADATA_PATH.exists():
        try:
            payload = json.loads(PIPELINE_RUN_METADATA_PATH.read_text(encoding="utf-8"))
            score_window = payload.get("score_window") or {}
            if score_window:
                days_value = score_window.get("days")
                days_repr = str(days_value) if days_value is not None else None
                start_value = score_window.get("start")
                end_value = score_window.get("end") or payload.get("as_of_date")
                mode = "explicit_range" if start_value or end_value else "full_history"
                if days_value:
                    label = f"Последние {days_value} дн. до {end_value}"
                    mode = "rolling_days"
                elif start_value or end_value:
                    label = f"{start_value or '(-inf)'} .. {end_value or '(+inf)'}"
                else:
                    label = f"Вся история до {payload.get('as_of_date') or '(дата расчёта не указана)'}"
                return {
                    "mode": mode,
                    "label": label,
                    "days": days_repr,
                    "start": start_value,
                    "end": end_value,
                    "as_of_date": payload.get("as_of_date"),
                }
        except Exception:
            pass

    as_of_date = (os.environ.get("PIPELINE_AS_OF_DATE") or "").strip() or None
    window_days_raw = (os.environ.get("PIPELINE_SCORE_WINDOW_DAYS") or "").strip()
    window_start = (os.environ.get("PIPELINE_SCORE_WINDOW_START") or "").strip() or None
    window_end = (os.environ.get("PIPELINE_SCORE_WINDOW_END") or "").strip() or None

    window_days: Optional[int] = None
    if window_days_raw:
        try:
            parsed = int(window_days_raw)
            if parsed > 0:
                window_days = parsed
        except ValueError:
            window_days = None

    if window_start or window_end:
        start_repr = window_start or "(-inf)"
        end_repr = window_end or as_of_date or "(+inf)"
        label = f"{start_repr} .. {end_repr}"
        mode = "explicit_range"
    elif window_days is not None:
        end_repr = as_of_date or "(текущая дата расчёта)"
        label = f"Последние {window_days} дн. до {end_repr}"
        mode = "rolling_days"
    else:
        end_repr = as_of_date or "(дата расчёта не указана)"
        label = f"Вся история до {end_repr}"
        mode = "full_history"

    return {
        "mode": mode,
        "label": label,
        "days": str(window_days) if window_days is not None else None,
        "start": window_start,
        "end": window_end or as_of_date,
        "as_of_date": as_of_date,
    }


# ---------------------------------------------------------------------------
# Pydantic models for request/response
# ---------------------------------------------------------------------------
class SearchRequest(BaseModel):
    """Query model for /products/search endpoint."""

    q: str = Field(default="", description="Search query string")
    category: Optional[str] = Field(default=None, description="Filter by category_name")
    brand: Optional[str] = Field(default=None, description="Filter by brand")
    gender: Optional[str] = Field(default=None, description="Filter by gender ('male' | 'female')")
    min_price: Optional[float] = Field(default=None, ge=0, description="Minimum price filter")
    max_price: Optional[float] = Field(default=None, ge=0, description="Maximum price filter")
    is_new: Optional[bool] = Field(default=None, description="Filter by new arrivals")
    is_sale: Optional[bool] = Field(default=None, description="Filter by sale items")
    page_tag: Optional[str] = Field(default=None, description="Filter by storefront page alias")
    limit: int = Field(default=20, ge=1, le=100, description="Max results to return")
    sort_by: str = Field(
        default="final_score",
        description="Sort field: final_score | popularity | novelty | price_desc | price_asc",
    )
    strategy: Optional[str] = Field(
        default=None,
        description="Score strategy index: baseline | funnel | robust | commercial",
    )


class ProductItem(BaseModel):
    """Single product returned in API responses."""

    id: int
    product_id: int
    title: str
    brand: str
    vendor: str = ""
    article: str = ""
    product_url: str = ""
    price: float
    discount: float = 0.0
    gender: str
    category_id: str = ""
    category_name: str
    is_new: bool
    is_sale: bool
    in_stock: bool
    age_days: int = 0
    views: int = 0
    purchases: int = 0
    popularity: float
    novelty: float
    boost: float = 1.0
    final_score: float
    created_at: str = ""


class APIResponse(BaseModel):
    """Standard wrapper for all API responses."""

    success: bool
    count: int
    total: Optional[int] = None
    processing_time_ms: int
    query: Optional[str] = None
    hits: List[ProductItem] = []
    strategy: Optional[str] = None
    index_uid: Optional[str] = None


# ---------------------------------------------------------------------------
# Core Ranking Engine — wraps Meilisearch client
# ---------------------------------------------------------------------------
class RankingAPI:
    """Encapsulates all Meilisearch interactions for the ranking pipeline."""

    def __init__(self, url: str = MEILISEARCH_URL, api_key: str = MEILISEARCH_MASTER_KEY):
        self.url = url
        self.client = meilisearch.Client(url, api_key)

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _strategy_and_uid(strategy: Optional[str]) -> tuple[str, str]:
        try:
            code = normalize_strategy(strategy)
            uid = resolve_index_uid(strategy)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return code, uid

    def _get_index(self, index_uid: str):
        """Return the Meilisearch index handle for a resolved UID."""
        try:
            return self.client.index(index_uid)
        except meilisearch.errors.MeilisearchCommunicationError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Meilisearch communication error: {exc}",
            ) from exc

    @staticmethod
    def _page_filter_clauses(page_tag: str) -> List[str]:
        normalized = str(page_tag).strip().lower()
        if normalized == "new_arrivals":
            return ["is_new = true"]
        if normalized == "sale":
            return ["is_sale = true"]
        if normalized == "men":
            return ["gender = 'male'"]
        if normalized == "jackets":
            return [
                "("
                "category_name = 'Куртки' OR "
                "category_name = 'Куртка' OR "
                "category_name = 'Пуховики' OR "
                "category_name = 'Жилеты'"
                ")"
            ]
        return []

    def _build_filter(self, **kwargs) -> Optional[str]:
        """Build a Meilisearch filter string from keyword arguments."""
        clauses: List[str] = []
        if kwargs.get("in_stock") is True:
            clauses.append("in_stock = true")
        if kwargs.get("current_on_site") is True:
            clauses.append("current_on_site = true")
        if "page_tag" in kwargs and kwargs["page_tag"]:
            clauses.extend(self._page_filter_clauses(str(kwargs["page_tag"])))
        if "brand" in kwargs and kwargs["brand"]:
            brand = _escape_filter_value(str(kwargs["brand"]))
            clauses.append(f"brand = '{brand}'")
        if "category" in kwargs and kwargs["category"]:
            category = _escape_filter_value(str(kwargs["category"]))
            clauses.append(f"category_name = '{category}'")
        if "gender" in kwargs and kwargs["gender"]:
            gender = _escape_filter_value(str(kwargs["gender"]))
            clauses.append(f"gender = '{gender}'")
        if "is_new" in kwargs and kwargs["is_new"] is not None:
            val = "true" if kwargs["is_new"] else "false"
            clauses.append(f"is_new = {val}")
        if "is_sale" in kwargs and kwargs["is_sale"] is not None:
            val = "true" if kwargs["is_sale"] else "false"
            clauses.append(f"is_sale = {val}")
        if kwargs.get("min_price") is not None:
            clauses.append(f"price >= {kwargs['min_price']}")
        if kwargs.get("max_price") is not None:
            clauses.append(f"price <= {kwargs['max_price']}")
        return " AND ".join(clauses) if clauses else None

    @staticmethod
    def _resolve_sort(sort_by: str) -> str:
        """Map UI/API sort aliases to Meilisearch sort expressions."""
        mapping = {
            "final_score": "final_score:desc",
            "popularity": "popularity:desc",
            "novelty": "novelty:desc",
            "price_desc": "price:desc",
            "price_asc": "price:asc",
            "price": "price:desc",
        }
        return mapping.get(sort_by, "final_score:desc")

    # -- public query methods ------------------------------------------------

    def health_check(self) -> Dict[str, Any]:
        """Verify connectivity to Meilisearch."""
        start = time.time()
        try:
            # Use requests directly since meilisearch client doesn't have get_health in v0.41
            r = http_requests.get(f"{self.url}/health", timeout=5)
            if r.status_code == 200:
                elapsed_ms = int((time.time() - start) * 1000)
                return {"status": "healthy", "meilisearch_url": self.url, "elapsed_ms": elapsed_ms}
            else:
                raise HTTPException(status_code=503, detail=f"Meilisearch returned status {r.status_code}")
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Meilisearch unhealthy: {exc}") from exc

    def get_trending(self, limit: int = 20, strategy: Optional[str] = None) -> Dict[str, Any]:
        """Top products ranked by final_score (descending)."""
        code, uid = self._strategy_and_uid(strategy)
        index = self._get_index(uid)
        start = time.time()
        results = index.search(
            "",
            opt_params={
                "sort": ["final_score:desc"],
                "limit": limit,
            },
        )
        elapsed_ms = int((time.time() - start) * 1000)
        return self._format_results(
            results, elapsed_ms=elapsed_ms, strategy=code, index_uid=uid
        )

    def get_new_arrivals(self, limit: int = 20, strategy: Optional[str] = None) -> Dict[str, Any]:
        """New arrivals filtered by is_new=true, sorted by novelty desc."""
        code, uid = self._strategy_and_uid(strategy)
        index = self._get_index(uid)
        start = time.time()
        results = index.search(
            "",
            opt_params={
                "filter": "is_new = true AND in_stock = true AND current_on_site = true",
                "sort": ["final_score:desc"],
                "limit": limit,
            },
        )
        elapsed_ms = int((time.time() - start) * 1000)
        return self._format_results(
            results,
            elapsed_ms=elapsed_ms,
            query="is_new=true",
            strategy=code,
            index_uid=uid,
        )

    def get_sale_items(self, limit: int = 20, strategy: Optional[str] = None) -> Dict[str, Any]:
        """Sale items filtered by is_sale=true, sorted by final_score desc."""
        code, uid = self._strategy_and_uid(strategy)
        index = self._get_index(uid)
        start = time.time()
        results = index.search(
            "",
            opt_params={
                "filter": "is_sale = true AND in_stock = true AND current_on_site = true",
                "sort": ["final_score:desc"],
                "limit": limit,
            },
        )
        elapsed_ms = int((time.time() - start) * 1000)
        return self._format_results(
            results,
            elapsed_ms=elapsed_ms,
            query="is_sale=true",
            strategy=code,
            index_uid=uid,
        )

    def get_page(
        self,
        page_tag: str,
        *,
        limit: int = 20,
        sort_by: str = "final_score",
        strategy: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Storefront page response by page tag."""
        code, uid = self._strategy_and_uid(strategy)
        index = self._get_index(uid)
        start = time.time()
        filter_str = self._build_filter(in_stock=True, current_on_site=True, page_tag=page_tag)
        results = index.search(
            "",
            opt_params={
                "filter": filter_str,
                "sort": [self._resolve_sort(sort_by)],
                "limit": limit,
            },
        )
        elapsed_ms = int((time.time() - start) * 1000)
        return self._format_results(
            results,
            elapsed_ms=elapsed_ms,
            query=f"page_tag={page_tag}",
            strategy=code,
            index_uid=uid,
        )

    def search_products(self, request: SearchRequest) -> Dict[str, Any]:
        """Full-text search with optional filters."""
        code, uid = self._strategy_and_uid(request.strategy)
        index = self._get_index(uid)
        start = time.time()

        filter_str = self._build_filter(
            in_stock=True,
            current_on_site=True,
            page_tag=request.page_tag,
            brand=request.brand,
            category=request.category,
            gender=request.gender,
            is_new=request.is_new,
            is_sale=request.is_sale,
            min_price=request.min_price,
            max_price=request.max_price,
        )

        results = index.search(
            request.q,
            opt_params={
                "filter": filter_str,
                "sort": [self._resolve_sort(request.sort_by)],
                "limit": request.limit,
            },
        )
        elapsed_ms = int((time.time() - start) * 1000)
        return self._format_results(
            results,
            elapsed_ms=elapsed_ms,
            query=request.q or filter_str or "(all)",
            strategy=code,
            index_uid=uid,
        )

    # -- response formatting -------------------------------------------------

    @staticmethod
    def _format_results(
        results: dict,
        *,
        elapsed_ms: int = 0,
        query: Optional[str] = None,
        strategy: Optional[str] = None,
        index_uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Convert Meilisearch search result to our standard APIResponse."""
        hits_raw = results.get("hits", [])
        total = results.get("totalHits", len(hits_raw))

        products = []
        for hit in hits_raw:
            try:
                # Convert Pydantic model to dict for JSON serialization
                p = ProductItem(**hit)
                products.append(p.model_dump())
            except Exception as exc:
                logger.warning(f"Skipping malformed product record: {exc}")

        return {
            "success": True,
            "count": len(products),
            "total": total,
            "processing_time_ms": elapsed_ms,
            "query": query,
            "hits": products,
            "strategy": strategy,
            "index_uid": index_uid,
        }

    def get_catalog_metadata(self) -> Dict[str, Any]:
        """Return lightweight metadata for the local UI."""
        categories: List[str] = []
        brands: List[str] = []
        score_window = _resolve_score_window_metadata()
        totals = {
            "products": 0,
            "in_stock": 0,
            "on_sale": 0,
            "new_arrivals": 0,
        }

        catalog_csv = _primary_scored_catalog_path()
        if catalog_csv.exists():
            try:
                df = pd.read_csv(catalog_csv, low_memory=False)
                totals["products"] = int(len(df))
                if "in_stock" in df.columns:
                    totals["in_stock"] = int(df["in_stock"].fillna(False).astype(bool).sum())
                if "is_sale" in df.columns:
                    totals["on_sale"] = int(df["is_sale"].fillna(False).astype(bool).sum())
                if "is_new" in df.columns:
                    totals["new_arrivals"] = int(df["is_new"].fillna(False).astype(bool).sum())
                if "category_name" in df.columns:
                    categories = sorted(
                        {
                            str(value).strip()
                            for value in df["category_name"].dropna().tolist()
                            if str(value).strip()
                        }
                    )
                if "vendor" in df.columns:
                    brands = sorted(
                        {
                            str(value).strip()
                            for value in df["vendor"].dropna().tolist()
                            if str(value).strip()
                        }
                    )
            except Exception as exc:
                logger.warning(f"Could not load catalog metadata from CSV: {exc}")

        labels = strategy_labels()
        strategies_meta = [
            {
                "code": code,
                "label": labels[code],
                "index_uid": resolve_index_uid(code),
            }
            for code in sorted(labels.keys())
        ]
        default_uid = resolve_index_uid(None)

        return {
            "pages": [
                {"key": "new_arrivals", "label": "Новинки"},
                {"key": "sale", "label": "Распродажа"},
                {"key": "men", "label": "Мужское"},
                {"key": "jackets", "label": "Куртки"},
            ],
            "sort_options": [
                {"key": "final_score", "label": "По итоговому баллу"},
                {"key": "popularity", "label": "По популярности"},
                {"key": "novelty", "label": "По новизне"},
                {"key": "price_desc", "label": "По цене вниз"},
                {"key": "price_asc", "label": "По цене вверх"},
            ],
            "strategies": strategies_meta,
            "index_base": get_index_base(),
            "default_strategy": get_default_strategy(),
            "index_uid_default": default_uid,
            "index_override": get_index_override() or None,
            "categories": categories,
            "brands": brands,
            "totals": totals,
            "score_window": score_window,
            "index_name": default_uid,
            "scored_catalog_path": str(catalog_csv) if catalog_csv.exists() else str(SCORED_CATALOG_PATH),
            "pricing_notice": (
                "Цены в мини-сайте показываются по текущей выгрузке и местами расходятся "
                "с живым kixbox.ru. Их лучше воспринимать как технический признак данных, "
                "а не как точную витринную цену."
            ),
        }


# ---------------------------------------------------------------------------
# FastAPI application factory
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown hooks."""
    logger.info("Kixbox Ranking API starting up...")
    yield
    logger.info("Kixbox Ranking API shutting down.")


app = FastAPI(
    title="Kixbox Ranking API",
    description=(
        "RESTful product ranking and search service backed by Meilisearch. "
        "Provides endpoints for trending, new arrivals, sale items, and full-text search."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# Instantiate the core engine (singleton pattern)
engine = RankingAPI()

_ranking_lab_logged = False


def _ranking_lab_html_path() -> Path:
    """Путь к шаблону: env RANKING_LAB_HTML или src/ranking_lab.html рядом с этим файлом."""
    override = (os.environ.get("RANKING_LAB_HTML") or "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parent / "ranking_lab.html"


def _load_ranking_lab_html() -> str:
    """Читать с диска на каждый запрос — не залипает старая строка в памяти."""
    global _ranking_lab_logged
    path = _ranking_lab_html_path()
    if not path.is_file():
        raise RuntimeError(
            f"Missing UI template: {path}. Скопируйте ranking_lab.html из репозитория или задайте RANKING_LAB_HTML."
        )
    html = path.read_text(encoding="utf-8")
    if not _ranking_lab_logged:
        logger.info("Ranking UI: %s (%d bytes)", path.resolve(), len(html.encode("utf-8")))
        _ranking_lab_logged = True
    if 'id="strategy"' not in html:
        logger.error(
            "Файл %s — старая версия без select#strategy. Обновите ranking_lab.html из репозитория.",
            path.resolve(),
        )
    return html


# -- Endpoints ---------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, tags=["UI"])
async def home():
    """Local mini-site for browsing ranking outputs."""
    return HTMLResponse(content=_load_ranking_lab_html(), status_code=200)


@app.get("/debug/ui-info", tags=["UI"])
async def debug_ui_template_info():
    """Проверка: какой файл шаблона читается и есть ли в нём выбор стратегии."""
    path = _ranking_lab_html_path()
    if not path.is_file():
        return JSONResponse(
            status_code=404,
            content={
                "path": str(path.resolve()),
                "exists": False,
                "hint": "Задайте RANKING_LAB_HTML или положите ranking_lab.html рядом с api_service.py",
            },
        )
    text = path.read_text(encoding="utf-8")
    return JSONResponse(
        content={
            "path": str(path.resolve()),
            "exists": True,
            "bytes": len(text.encode("utf-8")),
            "has_strategy_select": 'id="strategy"' in text,
            "has_data_ranking_ui": "data-ranking-ui" in text,
            "expected": "has_strategy_select и has_data_ranking_ui должны быть true",
        }
    )


@app.get("/app/meta", tags=["UI"])
async def app_metadata():
    """Metadata for the local UI."""
    try:
        data = engine.get_catalog_metadata()
        return JSONResponse(content=data, status_code=200)
    except HTTPException as exc:
        raise exc
    except Exception as exc:
        logger.error(f"app metadata endpoint error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

API_BUILD = "2026-05-02-ranking-ui-v3"


@app.get("/health", tags=["System"])
async def health():
    """Health check — verifies Meilisearch connectivity + какой шаблон UI на диске."""
    info = engine.health_check()
    info["api_build"] = API_BUILD
    path = _ranking_lab_html_path()
    ui_diag: Dict[str, Any] = {"path": str(path.resolve()), "exists": path.is_file()}
    if path.is_file():
        try:
            text = path.read_text(encoding="utf-8")
            ui_diag["bytes"] = len(text.encode("utf-8"))
            ui_diag["has_strategy_select"] = 'id="strategy"' in text
            ui_diag["has_data_ranking_ui"] = "data-ranking-ui" in text
        except OSError as exc:
            ui_diag["read_error"] = str(exc)
    info["ranking_lab_html"] = ui_diag
    info["docs"] = "Откройте /docs — там должен быть GET /debug/ui-info"
    return JSONResponse(content=info, status_code=200)


@app.get("/products/trending", response_model=APIResponse, tags=["Products"])
async def trending(
    limit: int = Query(default=20, ge=1, le=100),
    strategy: Optional[str] = Query(
        default=None,
        description="Индекс скоринга: baseline | funnel | robust | commercial",
    ),
):
    """Top products ranked by final_score (overall best sellers)."""
    try:
        data = engine.get_trending(limit=limit, strategy=strategy)
        return JSONResponse(content=data, status_code=200)
    except HTTPException as exc:
        raise exc
    except Exception as exc:
        logger.error(f"trending endpoint error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/products/new", response_model=APIResponse, tags=["Products"])
async def new_arrivals(
    limit: int = Query(default=20, ge=1, le=100),
    strategy: Optional[str] = Query(
        default=None,
        description="Индекс скоринга: baseline | funnel | robust | commercial",
    ),
):
    """New arrivals filtered by is_new=true, sorted by novelty desc."""
    try:
        data = engine.get_new_arrivals(limit=limit, strategy=strategy)
        return JSONResponse(content=data, status_code=200)
    except HTTPException as exc:
        raise exc
    except Exception as exc:
        logger.error(f"new arrivals endpoint error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/products/sale", response_model=APIResponse, tags=["Products"])
async def sale_items(
    limit: int = Query(default=20, ge=1, le=100),
    strategy: Optional[str] = Query(
        default=None,
        description="Индекс скоринга: baseline | funnel | robust | commercial",
    ),
):
    """Sale items filtered by is_sale=true, sorted by final_score desc."""
    try:
        data = engine.get_sale_items(limit=limit, strategy=strategy)
        return JSONResponse(content=data, status_code=200)
    except HTTPException as exc:
        raise exc
    except Exception as exc:
        logger.error(f"sale items endpoint error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/products/page/{page_tag}", response_model=APIResponse, tags=["Products"])
async def storefront_page(
    page_tag: str,
    sort_by: str = Query(default="final_score"),
    limit: int = Query(default=24, ge=1, le=100),
    strategy: Optional[str] = Query(
        default=None,
        description="Индекс скоринга: baseline | funnel | robust | commercial",
    ),
):
    """Storefront page by page tag."""
    try:
        data = engine.get_page(
            page_tag=page_tag, limit=limit, sort_by=sort_by, strategy=strategy
        )
        return JSONResponse(content=data, status_code=200)
    except HTTPException as exc:
        raise exc
    except Exception as exc:
        logger.error(f"storefront page endpoint error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/products/search", response_model=APIResponse, tags=["Products"])
async def search_products(request: SearchRequest):
    """Full-text search with optional filters (category, gender, price range)."""
    try:
        data = engine.search_products(request)
        return JSONResponse(content=data, status_code=200)
    except HTTPException as exc:
        raise exc
    except Exception as exc:
        logger.error(f"search endpoint error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    _api_dir = Path(__file__).resolve().parent
    _project_root = _api_dir.parent
    # Без app_dir reloader при запуске из корня проекта может импортировать не тот модуль — отдаётся старый HTML.
    uvicorn.run(
        "api_service:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        app_dir=str(_api_dir),
        reload_dirs=[str(_api_dir), str(_project_root / "pipeline")],
        log_level="info",
    )
