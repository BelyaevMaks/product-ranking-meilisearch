"""
Generate demo queries and top product lists for the target pages.

The script can work in two modes:
- local mode: reads scored CSV and emulates the same filters/sorts
- Meilisearch mode: sends the generated queries to a running index
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pipeline  # noqa: F401
import meilisearch
import pandas as pd
import requests

from pipeline.search_indexer.indexer import SearchIndexer


SORT_FIELDS = ("final_score", "popularity", "novelty")


@dataclass(frozen=True)
class PageSpec:
    slug: str
    title: str
    filter_expr: str
    require_in_stock: bool = True


PAGE_SPECS = (
    PageSpec("new_arrivals", "Новинки", "is_new = true AND in_stock = true AND current_on_site = true"),
    PageSpec("sale", "Распродажа", "is_sale = true AND in_stock = true AND current_on_site = true"),
    PageSpec("men", "Мужское", "gender = 'male' AND in_stock = true AND current_on_site = true"),
    PageSpec(
        "jackets",
        "Куртки",
        "(category_name = 'Куртки' OR category_name = 'Куртка' OR category_name = 'Пуховики' OR category_name = 'Жилеты') AND in_stock = true AND current_on_site = true",
    ),
)


def _compact_product(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "product_id": doc.get("product_id"),
        "title": doc.get("title"),
        "brand": doc.get("brand"),
        "category_name": doc.get("category_name"),
        "price": doc.get("price"),
        "discount": doc.get("discount"),
        "in_stock": doc.get("in_stock"),
        "is_new": doc.get("is_new"),
        "is_sale": doc.get("is_sale"),
        "gender": doc.get("gender"),
        "popularity": doc.get("popularity"),
        "novelty": doc.get("novelty"),
        "final_score": doc.get("final_score"),
    }


def _query_payload(page: PageSpec, sort_field: str, limit: int) -> Dict[str, Any]:
    return {
        "q": "",
        "filter": page.filter_expr,
        "sort": [f"{sort_field}:desc"],
        "limit": limit,
    }


def _matches_page(doc: Dict[str, Any], slug: str) -> bool:
    if not doc.get("in_stock") or not doc.get("current_on_site"):
        return False
    if slug == "new_arrivals":
        return bool(doc.get("is_new"))
    if slug == "sale":
        return bool(doc.get("is_sale"))
    if slug == "men":
        return str(doc.get("gender", "")).strip().lower() == "male"
    if slug == "jackets":
        return str(doc.get("category_name", "")).strip() in {"Куртки", "Куртка", "Пуховики", "Жилеты"}
    return False


def _load_local_documents(scored_path: Path) -> List[Dict[str, Any]]:
    df = pd.read_csv(scored_path, encoding="utf-8-sig")
    return SearchIndexer(client=None).prepare_documents(df)


def _run_local_query(
    documents: List[Dict[str, Any]],
    *,
    page_slug: str,
    require_in_stock: bool,
    sort_field: str,
    limit: int,
) -> List[Dict[str, Any]]:
    candidates = [
        doc
        for doc in documents
        if _matches_page(doc, page_slug) and (doc.get("in_stock") or not require_in_stock)
    ]
    candidates.sort(key=lambda doc: float(doc.get(sort_field, 0.0)), reverse=True)
    return candidates[:limit]


def _meili_available(url: str) -> bool:
    try:
        response = requests.get(f"{url}/health", timeout=3)
        return response.status_code == 200
    except requests.RequestException:
        return False


def _run_meili_query(
    *,
    url: str,
    api_key: str,
    index_name: str,
    payload: Dict[str, Any],
) -> List[Dict[str, Any]]:
    client = meilisearch.Client(url, api_key)
    index = client.index(index_name)
    results = index.search(
        payload["q"],
        opt_params={
            "filter": payload["filter"],
            "sort": payload["sort"],
            "limit": payload["limit"],
        },
    )
    return results.get("hits", [])


def build_demo(
    *,
    scored_path: Path,
    use_meili: bool,
    meili_url: str,
    meili_key: str,
    index_name: str,
) -> Dict[str, Any]:
    documents = _load_local_documents(scored_path)
    mode = "meilisearch" if use_meili and _meili_available(meili_url) else "local_fallback"

    pages: Dict[str, Any] = {}
    for page in PAGE_SPECS:
        page_docs = _run_local_query(
            documents,
            page_slug=page.slug,
            require_in_stock=page.require_in_stock,
            sort_field="final_score",
            limit=50,
        )
        sort_results: Dict[str, Any] = {}

        for sort_field in SORT_FIELDS:
            payload = _query_payload(page, sort_field, limit=50)
            if mode == "meilisearch":
                hits = _run_meili_query(
                    url=meili_url,
                    api_key=meili_key,
                    index_name=index_name,
                    payload=payload,
                )[:10]
            else:
                hits = _run_local_query(
                    documents,
                    page_slug=page.slug,
                    require_in_stock=page.require_in_stock,
                    sort_field=sort_field,
                    limit=10,
                )

            sort_results[sort_field] = {
                "meilisearch_query": {
                    "endpoint": f"POST /indexes/{index_name}/search",
                    "payload": payload,
                },
                "top_10": [_compact_product(hit) for hit in hits],
            }

        pages[page.slug] = {
            "title": page.title,
            "filter": page.filter_expr,
            "candidate_count_capped_at_50": min(len(page_docs), 50),
            "sample_index_document": page_docs[0] if page_docs else None,
            "sort_examples": sort_results,
        }

    return {
        "mode": mode,
        "scored_path": str(scored_path),
        "index_name": index_name,
        "pages": pages,
    }


def write_markdown(report: Dict[str, Any], output_path: Path) -> None:
    lines: List[str] = [
        "# Демонстрационные результаты Meilisearch",
        "",
        f"Режим: `{report['mode']}`",
        f"Индекс: `{report['index_name']}`",
        "",
    ]

    for page in report["pages"].values():
        lines.extend(
            [
                f"## {page['title']}",
                "",
                "### Пример индексируемого документа",
                "",
                "```json",
                json.dumps(page["sample_index_document"], ensure_ascii=False, indent=2),
                "```",
                "",
            ]
        )

        for sort_field, sort_info in page["sort_examples"].items():
            lines.extend(
                [
                    f"### Сортировка по `{sort_field}`",
                    "",
                    "Запрос:",
                    "",
                    "```json",
                    json.dumps(sort_info["meilisearch_query"], ensure_ascii=False, indent=2),
                    "```",
                    "",
                    "| # | product_id | title | brand | category | final_score | popularity | novelty |",
                    "|---|---:|---|---|---|---:|---:|---:|",
                ]
            )
            for rank, product in enumerate(sort_info["top_10"], start=1):
                lines.append(
                    "| {rank} | {product_id} | {title} | {brand} | {category} | {final_score:.6f} | {popularity:.6f} | {novelty:.4f} |".format(
                        rank=rank,
                        product_id=product.get("product_id") or 0,
                        title=str(product.get("title") or "").replace("|", "\\|"),
                        brand=str(product.get("brand") or "").replace("|", "\\|"),
                        category=str(product.get("category_name") or "").replace("|", "\\|"),
                        final_score=float(product.get("final_score") or 0.0),
                        popularity=float(product.get("popularity") or 0.0),
                        novelty=float(product.get("novelty") or 0.0),
                    )
                )
            lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scored-path", default=str(PROJECT_ROOT / "artifacts" / "scored_catalog.csv"))
    parser.add_argument("--use-meili", action="store_true")
    parser.add_argument("--output-json", default=str(PROJECT_ROOT / "artifacts" / "meilisearch_demo_results.json"))
    parser.add_argument("--output-md", default=str(PROJECT_ROOT / "artifacts" / "meilisearch_demo_results.md"))
    args = parser.parse_args()

    meili_url = os.environ.get("MEILISEARCH_URL", "http://localhost:7700")
    meili_key = os.environ.get(
        "MEILISEARCH_MASTER_KEY",
        "Yu4HfgPAV9gWM5nrutsfJi0b0RsXSFK8tiPw8Zx__Co",
    )
    index_name = os.environ.get("MEILISEARCH_INDEX_NAME", "kixbox_products")

    report = build_demo(
        scored_path=Path(args.scored_path),
        use_meili=args.use_meili,
        meili_url=meili_url,
        meili_key=meili_key,
        index_name=index_name,
    )

    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    output_md = Path(args.output_md)
    write_markdown(report, output_md)

    print(f"[Demo] Mode: {report['mode']}")
    print(f"[Demo] JSON saved: {output_json}")
    print(f"[Demo] Markdown saved: {output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
