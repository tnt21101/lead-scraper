import streamlit as st

from core.export import DEFAULT_COLUMNS, COLUMN_LABELS, export_csv, export_excel

st.title("Export Results")

if "leads" not in st.session_state or not st.session_state["leads"]:
    st.warning("No leads to export. Go to **Scrape Leads** first.")
    st.stop()

leads_data = st.session_state["leads"]
st.info(f"**{len(leads_data)}** leads ready for export")

# --- Column Selection ---
st.subheader("Select Columns")

available_cols = list(COLUMN_LABELS.keys())
# Filter to columns that actually have data
import pandas as pd

df_check = pd.DataFrame(leads_data)
available_cols = [c for c in available_cols if c in df_check.columns and df_check[c].notna().any()]

# Default selection — use DEFAULT_COLUMNS that have data
default_selection = [c for c in DEFAULT_COLUMNS if c in available_cols]

selected_cols = st.multiselect(
    "Choose columns to include in export",
    options=available_cols,
    default=default_selection,
    format_func=lambda x: COLUMN_LABELS.get(x, x),
)

if not selected_cols:
    st.warning("Select at least one column.")
    st.stop()

# --- Preview ---
st.subheader("Preview")
from core.export import leads_to_dataframe

preview_df = leads_to_dataframe(leads_data, selected_cols)
st.dataframe(preview_df.head(10), use_container_width=True, hide_index=True)

if len(leads_data) > 10:
    st.caption(f"Showing 10 of {len(leads_data)} rows")

# --- Export Buttons ---
st.markdown("---")
st.subheader("Download")

col1, col2 = st.columns(2)

query = st.session_state.get("leads_query", "leads")
safe_query = query.replace(" ", "_")[:30]

with col1:
    csv_data = export_csv(leads_data, selected_cols)
    st.download_button(
        label="Download CSV",
        data=csv_data,
        file_name=f"{safe_query}_leads.csv",
        mime="text/csv",
        use_container_width=True,
    )

with col2:
    excel_data = export_excel(leads_data, selected_cols)
    st.download_button(
        label="Download Excel",
        data=excel_data,
        file_name=f"{safe_query}_leads.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
