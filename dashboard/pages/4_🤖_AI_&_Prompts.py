"""DECK AI & Prompts Dashboard - Dextr/prompt performance analytics"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.filters import render_sidebar_filters
from utils.data_loader import (
    load_prompt_headline_kpis,
    load_prompt_action_funnel,
    load_prompt_intent_performance,
    load_prompt_specificity,
    load_zero_save_trend,
    load_zero_save_prompts_detail,
    load_reprompting_analysis,
    load_pack_performance_top_bottom,
)

st.set_page_config(
    page_title="AI & Prompts | DECK Analytics",
    page_icon="\U0001f916",
    layout="wide"
)

apply_deck_branding()

st.title("AI & Prompts")
st.caption("Dextr prompt performance, zero-save analysis, and pack-level metrics")

# --- Sidebar Filters ---
filters = render_sidebar_filters(
    show_date_range=True,
    show_app_version=True,
    show_activation_cohort=True,
)

start_date = filters['start_date']
end_date = filters['end_date']
app_version = filters['app_version']
activation_week = filters['activation_week']

# ============================================================================
# Section A: AI Headline KPIs
# ============================================================================
st.subheader("Headline KPIs")

try:
    kpi_df = load_prompt_headline_kpis(
        start_date=start_date,
        end_date=end_date,
        app_version=app_version,
        activation_week=activation_week,
    )

    if kpi_df.empty:
        st.info("No data available for the selected filters.")
    else:
        row = kpi_df.iloc[0]
        total_prompts = int(row.get('total_prompts', 0))
        zero_save_pct = float(row.get('zero_save_pct', 0) or 0)
        avg_save_rate = float(row.get('avg_save_rate', 0) or 0)
        avg_cards_generated = float(row.get('avg_cards_generated', 0) or 0)

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                label="Total Prompts",
                value=f"{total_prompts:,}",
                help="Total number of Dextr AI prompts in the selected period"
            )
        with col2:
            st.metric(
                label="Zero-Save %",
                value=f"{zero_save_pct:.1f}%",
                help="Percentage of prompts where the user saved zero cards — indicates poor relevance or engagement"
            )
        with col3:
            st.metric(
                label="Avg Save Rate",
                value=f"{avg_save_rate * 100:.1f}%",
                help="Average fraction of generated cards that were saved across all prompts"
            )
        with col4:
            st.metric(
                label="Avg Cards Generated",
                value=f"{avg_cards_generated:.1f}",
                help="Average number of cards Dextr generated per prompt"
            )
except Exception as e:
    st.error(f"Error loading headline KPIs: {str(e)}")

st.divider()

# ============================================================================
# Section B: Prompt to Action Funnel
# ============================================================================
st.subheader("Prompt to Action Funnel")

try:
    funnel_df = load_prompt_action_funnel(
        start_date=start_date,
        end_date=end_date,
        app_version=app_version,
        activation_week=activation_week,
    )

    if funnel_df.empty:
        st.info("No data available for the selected filters.")
    else:
        row = funnel_df.iloc[0]
        f_prompts = int(row.get('total_prompts', 0))
        f_generated = int(row.get('total_cards_generated', 0))
        f_shown = int(row.get('total_cards_shown', 0))
        f_saved = int(row.get('total_cards_saved', 0))
        f_shared = int(row.get('led_to_share', 0))

        if f_prompts > 0:
            fig = go.Figure(go.Funnel(
                y=['Prompts', 'Cards Generated', 'Cards Shown', 'Cards Saved', 'Led to Share'],
                x=[f_prompts, f_generated, f_shown, f_saved, f_shared],
                textinfo="value+percent initial+percent previous",
                marker=dict(color=[
                    BRAND_COLORS['info'],
                    BRAND_COLORS['accent'],
                    '#E91E8C',
                    BRAND_COLORS['success'],
                    BRAND_COLORS['warning'],
                ])
            ))
            fig.update_layout(
                margin=dict(l=20, r=20, t=20, b=20),
                font=dict(family="Inter, system-ui, sans-serif", size=13),
                plot_bgcolor='white',
                paper_bgcolor='white',
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available for the selected filters.")
except Exception as e:
    st.error(f"Error loading prompt funnel: {str(e)}")

st.divider()

# ============================================================================
# Section C: Prompt Intent Performance
# ============================================================================
st.subheader("Prompt Intent Performance")

try:
    intent_df = load_prompt_intent_performance(
        start_date=start_date,
        end_date=end_date,
        app_version=app_version,
        activation_week=activation_week,
    )

    if intent_df.empty:
        st.info("No data available for the selected filters.")
    else:
        chart_df = intent_df.copy()
        chart_df['save_rate_pct'] = chart_df['avg_save_rate'].astype(float) * 100

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_df['prompt_intent'],
            y=chart_df['save_rate_pct'],
            name='Avg Save Rate (%)',
            marker_color=BRAND_COLORS['accent'],
            text=[f"{v:.1f}%" for v in chart_df['save_rate_pct']],
            textposition='outside',
        ))
        fig.update_layout(
            xaxis_title="Prompt Intent",
            yaxis_title="Avg Save Rate (%)",
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=40, r=20, t=40, b=40),
            yaxis=dict(gridcolor=BRAND_COLORS['border']),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Detail table
        display_df = intent_df.copy()
        display_df['avg_save_rate'] = (display_df['avg_save_rate'].astype(float) * 100).round(1).astype(str) + '%'
        display_df['zero_save_pct'] = display_df['zero_save_pct'].astype(float).round(1).astype(str) + '%'
        display_df.columns = ['Intent', 'Prompt Count', 'Avg Save Rate', 'Avg Cards Generated', 'Zero-Save %', 'Led to Share']
        st.dataframe(display_df, use_container_width=True, hide_index=True)
except Exception as e:
    st.error(f"Error loading intent performance: {str(e)}")

st.divider()

# ============================================================================
# Section D: Prompt Specificity Analysis
# ============================================================================
st.subheader("Prompt Specificity Analysis")

try:
    spec_df = load_prompt_specificity(
        start_date=start_date,
        end_date=end_date,
        app_version=app_version,
        activation_week=activation_week,
    )

    if spec_df.empty:
        st.info("No data available for the selected filters.")
    else:
        chart_df = spec_df.copy()
        chart_df['save_rate_pct'] = chart_df['avg_save_rate'].astype(float) * 100

        # Order specificity levels
        order = ['high', 'medium', 'low']
        chart_df['sort_key'] = chart_df['prompt_specificity'].apply(
            lambda x: order.index(x) if x in order else 99
        )
        chart_df = chart_df.sort_values('sort_key')

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_df['prompt_specificity'],
            y=chart_df['save_rate_pct'],
            name='Avg Save Rate (%)',
            marker_color=BRAND_COLORS['accent'],
            text=[f"{v:.1f}%" for v in chart_df['save_rate_pct']],
            textposition='outside',
        ))
        fig.add_trace(go.Bar(
            x=chart_df['prompt_specificity'],
            y=chart_df['avg_cards_generated'].astype(float),
            name='Avg Cards Generated',
            marker_color=BRAND_COLORS['success'],
            text=[f"{v:.1f}" for v in chart_df['avg_cards_generated'].astype(float)],
            textposition='outside',
        ))
        fig.update_layout(
            barmode='group',
            xaxis_title="Prompt Specificity",
            yaxis_title="Value",
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=40, r=20, t=40, b=40),
            yaxis=dict(gridcolor=BRAND_COLORS['border']),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        )
        st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.error(f"Error loading specificity analysis: {str(e)}")

st.divider()

# ============================================================================
# Section E: Zero-Save Prompts Deep Dive
# ============================================================================
st.subheader("Zero-Save Prompts Deep Dive")

try:
    trend_df = load_zero_save_trend(
        start_date=start_date,
        end_date=end_date,
        app_version=app_version,
    )

    if trend_df.empty:
        st.info("No data available for the selected filters.")
    else:
        trend_df = trend_df.sort_values('prompt_week')

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=trend_df['prompt_week'],
            y=trend_df['zero_save_pct'].astype(float),
            name='Zero-Save %',
            line=dict(color=BRAND_COLORS['error'], width=2.5),
            mode='lines+markers',
            marker=dict(size=6),
        ))
        fig.update_layout(
            xaxis_title="Week",
            yaxis_title="Zero-Save %",
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=40, r=20, t=40, b=40),
            yaxis=dict(gridcolor=BRAND_COLORS['border']),
            xaxis=dict(gridcolor=BRAND_COLORS['border']),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("Worst-performing prompts"):
        detail_df = load_zero_save_prompts_detail(
            start_date=start_date,
            end_date=end_date,
            app_version=app_version,
        )
        if detail_df.empty:
            st.info("No data available for the selected filters.")
        else:
            detail_df.columns = ['Query Text', 'Occurrences', 'Avg Cards Generated', 'Avg Cards Shown']
            st.dataframe(detail_df, use_container_width=True, hide_index=True)
except Exception as e:
    st.error(f"Error loading zero-save analysis: {str(e)}")

st.divider()

# ============================================================================
# Section F: Re-prompting Analysis
# ============================================================================
st.subheader("Re-prompting Analysis")

try:
    reprompt_df = load_reprompting_analysis(
        start_date=start_date,
        end_date=end_date,
        app_version=app_version,
    )

    if reprompt_df.empty:
        st.info("No data available for the selected filters.")
    else:
        total_sessions = reprompt_df['session_count'].astype(int).sum()
        multi_prompt_row = reprompt_df[reprompt_df['session_type'] == '2+ prompts']
        multi_prompt_count = int(multi_prompt_row['session_count'].iloc[0]) if not multi_prompt_row.empty else 0
        multi_prompt_pct = (multi_prompt_count / total_sessions * 100) if total_sessions > 0 else 0

        st.metric(
            label="Sessions with >1 Prompt",
            value=f"{multi_prompt_pct:.1f}%",
            help=f"{multi_prompt_count:,} of {total_sessions:,} sessions had more than one Dextr prompt — indicates users refining their queries"
        )

        # Comparison table
        display_df = reprompt_df.copy()
        display_df['avg_save_rate'] = (display_df['avg_save_rate'].astype(float) * 100).round(1).astype(str) + '%'
        display_df['pct_led_to_save'] = display_df['pct_led_to_save'].astype(float).round(1).astype(str) + '%'
        display_df.columns = ['Session Type', 'Session Count', 'Avg Save Rate', '% Led to Save']
        st.dataframe(display_df, use_container_width=True, hide_index=True)
except Exception as e:
    st.error(f"Error loading re-prompting analysis: {str(e)}")

st.divider()

# ============================================================================
# Section G: Pack Performance
# ============================================================================
with st.expander("Pack-Level Performance"):
    try:
        pack_df = load_pack_performance_top_bottom()

        if pack_df.empty:
            st.info("No data available for the selected filters.")
        else:
            # Determine shortlist column
            has_shortlist_col = 'has_shortlist' in pack_df.columns
            if not has_shortlist_col:
                # Approximate shortlist as packs where cards_saved >= 3
                if 'cards_saved' in pack_df.columns:
                    pack_df['has_shortlist'] = pack_df['cards_saved'] >= 3
                elif 'total_cards_saved' in pack_df.columns:
                    pack_df['has_shortlist'] = pack_df['total_cards_saved'] >= 3
                else:
                    pack_df['has_shortlist'] = False

            shortlist_pct = (pack_df['has_shortlist'].sum() / len(pack_df) * 100) if len(pack_df) > 0 else 0
            st.metric(
                label="Packs Achieving Shortlist",
                value=f"{shortlist_pct:.1f}%",
                help="Percentage of packs where users saved 3+ cards (shortlist threshold)"
            )

            # Top 15
            st.markdown("**Top 15 Packs by Save Rate**")
            top_15 = pack_df.head(15)
            st.dataframe(top_15, use_container_width=True, hide_index=True)

            # Bottom 15
            st.markdown("**Bottom 15 Packs by Save Rate**")
            bottom_15 = pack_df.tail(15).sort_values('save_rate', ascending=True)
            st.dataframe(bottom_15, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Error loading pack performance: {str(e)}")

add_deck_footer()
