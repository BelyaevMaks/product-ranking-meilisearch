"""
Utilities for classifying Mindbox JSON customer actions into intent groups.
"""

from __future__ import annotations

from typing import Dict


SYSTEM_NAME_RULES = {
    "purchase": [
        "SoxranenieZakazaVOperaciiWebsiteCreateOrder",
        "CreateOrder",
    ],
    "cart_add": [
        "DobavlenieProduktaVSpisok",
    ],
    "cart_remove": [
        "UdalenieProduktaIzSpiskaVOperaciiUdalenieIz",
        "UdalenieIzKorziny",
    ],
    "product_view": [
        "ProsmotrProdukta",
    ],
    "category_view": [
        "ProsmotrKategoriiProduktovVOperacii",
    ],
}


TEXT_RULES = {
    "purchase": ["createorder", "website.createorder", "сохранение заказа"],
    "cart_add": ["добавление в корзину", "addtocart", "добавление продукта в список"],
    "cart_remove": ["удаление из корзины", "removefromcart", "удаление продукта из списка"],
    "product_view": ["просмотр товара", "viewproduct", "просмотр продукта"],
    "category_view": ["просмотр категории", "viewcategory"],
}


def classify_event_intent(template_name: str = "", template_system_name: str = "") -> str:
    """Map action template data to a coarse intent group."""
    if template_system_name:
        for intent, patterns in SYSTEM_NAME_RULES.items():
            if any(pattern.lower() in template_system_name.lower() for pattern in patterns):
                return intent

    normalized = (template_name or "").strip().lower()
    if normalized:
        for intent, patterns in TEXT_RULES.items():
            if any(pattern in normalized for pattern in patterns):
                return intent

    return "other"


def summarize_intents_from_templates(template_counts: Dict[str, int]) -> Dict[str, int]:
    """Aggregate template counts into intent counts."""
    summary: Dict[str, int] = {
        "purchase": 0,
        "cart_add": 0,
        "cart_remove": 0,
        "product_view": 0,
        "category_view": 0,
        "other": 0,
    }
    for template, count in template_counts.items():
        intent = classify_event_intent(template_system_name=template, template_name=template)
        summary[intent] = summary.get(intent, 0) + int(count)
    return summary
