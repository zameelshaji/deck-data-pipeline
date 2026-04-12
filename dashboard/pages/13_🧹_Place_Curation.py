"""DECK Place Curation — Filter, review, and delete low-quality places."""

import streamlit as st
import pandas as pd
from datetime import datetime
from utils.styling import apply_deck_branding, add_deck_footer
from utils.data_loader import load_places_for_curation, delete_places

st.set_page_config(
    page_title="Place Curation | DECK Analytics",
    page_icon="\U0001f9f9",
    layout="wide",
)

apply_deck_branding()

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
if "deletion_log" not in st.session_state:
    st.session_state.deletion_log = []
if "confirm_delete" not in st.session_state:
    st.session_state.confirm_delete = False
if "pending_ids" not in st.session_state:
    st.session_state.pending_ids = []
if "pending_names" not in st.session_state:
    st.session_state.pending_names = []

# ---------------------------------------------------------------------------
# Title & data load
# ---------------------------------------------------------------------------
st.title("Place Curation")
st.caption("Review and remove low-quality places from the catalog")

df = load_places_for_curation()

if df.empty:
    st.warning("No places found.")
    st.stop()

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Filters")

    max_rating = st.slider("Max Rating", 0.0, 5.0, 5.0, step=0.5)
    max_reviews = st.number_input("Max Reviews", min_value=0, value=1000, step=10)
    max_images = st.number_input("Max Images", min_value=0, value=100, step=1)
    max_impressions = st.number_input("Max Impressions", min_value=0, value=10000, step=10)
    min_dislike_rate = st.slider("Min Dislike Rate %", 0, 100, 0)

    all_categories = sorted(
        {
            cat
            for cats in df["categories"].dropna()
            for cat in (cats if isinstance(cats, list) else [])
        }
    )
    selected_categories = st.multiselect("Categories", all_categories, default=all_categories)

    all_sources = sorted(df["source_type"].dropna().unique().tolist())
    selected_sources = st.multiselect("Source Type", all_sources, default=all_sources)


# ---------------------------------------------------------------------------
# Apply filters (pandas)
# ---------------------------------------------------------------------------
filtered = df.copy()

# Rating — NULLs always included
filtered = filtered[(filtered["rating"].isna()) | (filtered["rating"] <= max_rating)]

# Reviews — NULLs always included
filtered = filtered[
    (filtered["user_ratings_total"].isna()) | (filtered["user_ratings_total"] <= max_reviews)
]

# Media count
filtered = filtered[filtered["media_count"] <= max_images]

# Impressions
filtered = filtered[filtered["total_impressions"] <= max_impressions]

# Dislike rate — only applies to places with impressions
if min_dislike_rate > 0:
    filtered = filtered[
        (filtered["dislike_rate_pct"].notna()) & (filtered["dislike_rate_pct"] >= min_dislike_rate)
    ]

# Categories — array overlap
if selected_categories:
    cat_set = set(selected_categories)

    def _has_category_overlap(cats):
        if not isinstance(cats, list):
            return False
        return bool(set(cats) & cat_set)

    filtered = filtered[filtered["categories"].apply(_has_category_overlap)]

# Source type
if selected_sources:
    filtered = filtered[filtered["source_type"].isin(selected_sources)]


# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Total Places", f"{len(df):,}")
with c2:
    st.metric("Matching Filters", f"{len(filtered):,}")
with c3:
    avg_rating = filtered["rating"].mean()
    st.metric("Avg Rating (Filtered)", f"{avg_rating:.1f}" if pd.notna(avg_rating) else "N/A")
with c4:
    zero_images = int((filtered["media_count"] == 0).sum())
    st.metric("Zero Images", f"{zero_images:,}")

# ---------------------------------------------------------------------------
# Table display
# ---------------------------------------------------------------------------
st.subheader("Places")

SORT_OPTIONS = {
    "Rating (low first)": ("rating", True),
    "Rating (high first)": ("rating", False),
    "Reviews (low first)": ("user_ratings_total", True),
    "Reviews (high first)": ("user_ratings_total", False),
    "Images (low first)": ("media_count", True),
    "Impressions (low first)": ("total_impressions", True),
    "Impressions (high first)": ("total_impressions", False),
    "Dislike % (high first)": ("dislike_rate_pct", False),
    "Save % (low first)": ("save_rate_pct", True),
}
sort_label = st.selectbox("Sort by", list(SORT_OPTIONS.keys()), index=0)
sort_col, sort_asc = SORT_OPTIONS[sort_label]
filtered = filtered.sort_values(sort_col, ascending=sort_asc, na_position="first")

display_df = filtered.copy()
display_df.insert(0, "Select", False)
display_df["categories_str"] = display_df["categories"].apply(
    lambda x: ", ".join(x) if isinstance(x, list) else ""
)

edited_df = st.data_editor(
    display_df[
        [
            "Select",
            "name",
            "neighborhood",
            "categories_str",
            "rating",
            "user_ratings_total",
            "media_count",
            "total_impressions",
            "dislike_rate_pct",
            "save_rate_pct",
            "source_type",
            "is_featured",
            "id",
        ]
    ],
    column_config={
        "Select": st.column_config.CheckboxColumn("Select", default=False),
        "name": st.column_config.TextColumn("Place Name"),
        "neighborhood": st.column_config.TextColumn("Area"),
        "categories_str": st.column_config.TextColumn("Categories"),
        "rating": st.column_config.NumberColumn("Rating", format="%.1f"),
        "user_ratings_total": st.column_config.NumberColumn("Reviews", format="%d"),
        "media_count": st.column_config.NumberColumn("Images", format="%d"),
        "total_impressions": st.column_config.NumberColumn("Impressions", format="%d"),
        "dislike_rate_pct": st.column_config.NumberColumn("Dislike %", format="%.1f%%"),
        "save_rate_pct": st.column_config.NumberColumn("Save %", format="%.1f%%"),
        "source_type": st.column_config.TextColumn("Source"),
        "is_featured": st.column_config.CheckboxColumn("Featured", disabled=True),
        "id": None,
    },
    disabled=[
        "name",
        "neighborhood",
        "categories_str",
        "rating",
        "user_ratings_total",
        "media_count",
        "total_impressions",
        "dislike_rate_pct",
        "save_rate_pct",
        "source_type",
        "is_featured",
    ],
    use_container_width=True,
    hide_index=True,
    key="curation_editor",
)

# ---------------------------------------------------------------------------
# Selection & deletion flow
# ---------------------------------------------------------------------------
selected_rows = edited_df[edited_df["Select"] == True]  # noqa: E712
selected_ids = selected_rows["id"].tolist()
selected_names = selected_rows["name"].tolist()
selected_count = len(selected_ids)

if selected_count > 0:
    st.info(f"**{selected_count}** place{'s' if selected_count != 1 else ''} selected")

    featured_selected = selected_rows[selected_rows["is_featured"] == True]  # noqa: E712
    if not featured_selected.empty:
        st.warning(
            f"**{len(featured_selected)} featured place(s) selected**: "
            f"{', '.join(featured_selected['name'].tolist())}. "
            f"Removing featured places may affect curated sections in the app."
        )

    if st.button("Remove Selected Places", type="secondary"):
        st.session_state.confirm_delete = True
        st.session_state.pending_ids = selected_ids
        st.session_state.pending_names = selected_names
        st.rerun()

if st.session_state.confirm_delete:
    with st.expander("Confirm Deletion", expanded=True):
        st.error("This will **permanently delete** these places from the database. This cannot be undone.")
        for name in st.session_state.pending_names:
            st.write(f"- {name}")

        col_confirm, col_cancel = st.columns(2)
        with col_confirm:
            if st.button("Confirm Delete", type="primary"):
                count = delete_places(st.session_state.pending_ids)
                if count > 0:
                    st.toast(f"Deleted {count} place(s)")
                    st.session_state.deletion_log.append(
                        {
                            "timestamp": datetime.now().strftime("%H:%M:%S"),
                            "count": count,
                            "names": st.session_state.pending_names,
                        }
                    )
                else:
                    st.error("No places were deleted.")
                st.session_state.confirm_delete = False
                st.session_state.pending_ids = []
                st.session_state.pending_names = []
                st.rerun()
        with col_cancel:
            if st.button("Cancel"):
                st.session_state.confirm_delete = False
                st.session_state.pending_ids = []
                st.session_state.pending_names = []
                st.rerun()

# ---------------------------------------------------------------------------
# Session deletion log
# ---------------------------------------------------------------------------
if st.session_state.deletion_log:
    with st.expander("Session Deletion Log"):
        for entry in reversed(st.session_state.deletion_log):
            names_preview = ", ".join(entry["names"][:3])
            suffix = f" +{len(entry['names']) - 3} more" if len(entry["names"]) > 3 else ""
            st.write(
                f"**{entry['timestamp']}** — Deleted {entry['count']} place(s): "
                f"{names_preview}{suffix}"
            )

add_deck_footer()
