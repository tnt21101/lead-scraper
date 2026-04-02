"""Export leads to CSV and Excel formats."""

from io import BytesIO
from typing import Dict, List, Optional

import pandas as pd

# Human-readable column labels
COLUMN_LABELS = {
    "business_name": "Business Name",
    "address": "Address",
    "phone": "Phone",
    "website": "Website",
    "category": "Category",
    "rating": "Rating",
    "reviews_count": "Reviews",
    "google_maps_url": "Google Maps URL",
    "owner_name": "Owner Name",
    "owner_title": "Owner Title",
    "business_email": "Business Email",
    "personal_email": "Personal Email",
    "email_confidence": "Email Confidence",
    "company_linkedin": "Company LinkedIn",
    "company_facebook": "Company Facebook",
    "company_instagram": "Company Instagram",
    "company_twitter": "Company Twitter",
    "company_youtube": "Company YouTube",
    "owner_linkedin": "Owner LinkedIn",
    "owner_facebook": "Owner Facebook",
    "owner_twitter": "Owner Twitter",
    "source_provider": "Source",
    "enriched_by": "Enriched By",
    "scraped_at": "Scraped At",
}

# Default columns for export (most useful first)
DEFAULT_COLUMNS = [
    "business_name",
    "owner_name",
    "personal_email",
    "business_email",
    "phone",
    "website",
    "address",
    "category",
    "company_linkedin",
    "company_facebook",
    "company_instagram",
    "owner_linkedin",
    "rating",
    "reviews_count",
]


def leads_to_dataframe(
    leads_data: List[Dict],
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Convert lead dicts to a DataFrame with clean column names."""
    df = pd.DataFrame(leads_data)

    if columns:
        columns = [c for c in columns if c in df.columns]
        df = df[columns]

    # Rename to human-readable labels
    rename_map = {k: v for k, v in COLUMN_LABELS.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Convert enriched_by list to comma-separated string
    if "Enriched By" in df.columns:
        df["Enriched By"] = df["Enriched By"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else str(x)
        )

    return df


def export_csv(leads_data: List[Dict], columns: Optional[List[str]] = None) -> bytes:
    """Export leads as CSV bytes."""
    df = leads_to_dataframe(leads_data, columns)
    return df.to_csv(index=False).encode("utf-8")


def export_excel(leads_data: List[Dict], columns: Optional[List[str]] = None) -> bytes:
    """Export leads as Excel bytes."""
    df = leads_to_dataframe(leads_data, columns)
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Leads")

        # Auto-adjust column widths
        worksheet = writer.sheets["Leads"]
        for i, col in enumerate(df.columns):
            try:
                max_len = max(
                    df[col].astype(str).apply(len).max(),
                    len(str(col)),
                )
            except Exception:
                max_len = len(str(col))
            col_letter = chr(65 + i) if i < 26 else "A%s" % chr(65 + i - 26)
            worksheet.column_dimensions[col_letter].width = min(max_len + 2, 50)

    return buffer.getvalue()
