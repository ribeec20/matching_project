"""Visualization utilities for monopoly time analyses."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

__all__ = ["plot_monopoly_scatter"]


def _limit_anda_list(anda_list_str: str, max_count: int = 6) -> str:
    """Limit the ANDA list display to first max_count items."""
    if not anda_list_str or anda_list_str == "N/A":
        return "N/A"
    
    anda_items = anda_list_str.split(" | ")
    if len(anda_items) <= max_count:
        return anda_list_str
    
    limited_items = anda_items[:max_count]
    remaining_count = len(anda_items) - max_count
    return " | ".join(limited_items) + f" + {remaining_count} more"


def plot_monopoly_scatter(nda_monopoly_times: pd.DataFrame, show: bool = True) -> None:
    """Generate interactive scatter plot of NDA monopoly times with click details.
    
    Creates an interactive scatter plot showing:
    - X-axis: Granted monopoly period (NDA_MMT_Years)
    - Y-axis: Actual monopoly period (Actual_Monopoly_Years)
    - Click info: NDA details and matched ANDA information
    - Only shows NDAs with calculated monopoly times (matched cases)
    """
    # DEBUG: Print column names and sample data
    print("\n🔍 DEBUG: Checking data before plotting...")
    print(f"Columns in nda_monopoly_times: {nda_monopoly_times.columns.tolist()}")
    
    if 'Actual_Monopoly_Years' in nda_monopoly_times.columns:
        print(f"\nSample Actual_Monopoly_Years values:")
        print(nda_monopoly_times[['NDA_Appl_No', 'Actual_Monopoly_Years', 'NDA_MMT_Years']].head(10))
    
    # Filter to only NDAs with calculated monopoly times (matched cases)
    matched_data = nda_monopoly_times[
        nda_monopoly_times["Actual_Monopoly_Years"].notna() & 
        nda_monopoly_times["NDA_MMT_Years"].notna()
    ].copy()
    
    if matched_data.empty:
        print("No NDAs with calculated monopoly times to plot.")
        return
    
    print(f"\n✓ Filtered to {len(matched_data)} NDAs with monopoly times")
    print(f"  Actual_Monopoly_Years range: {matched_data['Actual_Monopoly_Years'].min():.2f} to {matched_data['Actual_Monopoly_Years'].max():.2f}")
    print(f"  NDA_MMT_Years range: {matched_data['NDA_MMT_Years'].min():.2f} to {matched_data['NDA_MMT_Years'].max():.2f}")
    
    # DEBUG: Check NDA 21513 specifically
    if 21513 in matched_data['NDA_Appl_No'].values:
        nda_21513 = matched_data[matched_data['NDA_Appl_No'] == 21513]
        print(f"\n🔍 DEBUG: NDA 21513 values:")
        print(f"  Actual_Monopoly_Years: {nda_21513['Actual_Monopoly_Years'].values[0]}")
        print(f"  NDA_MMT_Years: {nda_21513['NDA_MMT_Years'].values[0]}")
        if 'Actual_Monopoly_Days' in nda_21513.columns:
            print(f"  Actual_Monopoly_Days: {nda_21513['Actual_Monopoly_Days'].values[0]}")
    
    print()
    
    # Create detailed text with company information (limited to first 6 ANDA matches)
    matched_data["click_text"] = matched_data.apply(
        lambda row: (
            f"<b>NDA {row['NDA_Appl_No']}</b><br>"
            f"Company: {row['NDA_Applicant'] if 'NDA_Applicant' in row.index and pd.notna(row['NDA_Applicant']) else 'N/A'}<br>"
            f"Ingredient: {row['NDA_Ingredient']}<br>"
            f"NDA Approval: {row['NDA_Approval_Date']}<br>"
            f"Earliest ANDA: {row['Earliest_ANDA_Date'].strftime('%Y-%m-%d') if pd.notna(row['Earliest_ANDA_Date']) else 'N/A'}<br>"
            f"Granted Period: {row['NDA_MMT_Years']:.1f} years<br>"
            f"Actual Period: {row['Actual_Monopoly_Years']:.1f} years<br>"
            f"Difference: {row['Actual_Monopoly_Years'] - row['NDA_MMT_Years']:.1f} years<br>"
            f"Matching ANDAs ({row['Num_Matching_ANDAs']}): {_limit_anda_list(row['Matching_ANDA_List'] if 'Matching_ANDA_List' in row.index and pd.notna(row['Matching_ANDA_List']) else 'N/A', 6)}"
        ), 
        axis=1
    )
    
    # Determine color based on whether actual < granted
    matched_data["color_category"] = matched_data.apply(
        lambda row: "Shorter than granted" if row["Actual_Monopoly_Years"] < row["NDA_MMT_Years"] 
        else "Longer than granted", 
        axis=1
    )
    
    # DEBUG: Verify data integrity before plotting
    print("\n🔍 DEBUG: Pre-plot data verification:")
    print(f"  DataFrame shape: {matched_data.shape}")
    print(f"  Columns used for plotting: {['NDA_MMT_Years', 'Actual_Monopoly_Years']}")
    
    # Check for NaN values
    nan_x = matched_data['NDA_MMT_Years'].isna().sum()
    nan_y = matched_data['Actual_Monopoly_Years'].isna().sum()
    print(f"  NaN in X (NDA_MMT_Years): {nan_x}")
    print(f"  NaN in Y (Actual_Monopoly_Years): {nan_y}")
    
    # Sample 5 random rows to verify data
    print(f"\n  Sample of 5 random rows:")
    sample_cols = ['NDA_Appl_No', 'NDA_MMT_Years', 'Actual_Monopoly_Years']
    print(matched_data[sample_cols].sample(min(5, len(matched_data))).to_string(index=False))
    print()
    
    # Create the interactive scatter plot - we'll use hover but with enhanced details
    fig = px.scatter(
        matched_data,
        x="NDA_MMT_Years",
        y="Actual_Monopoly_Years",
        color="color_category",
        color_discrete_map={
            "Shorter than granted": "#1f77b4",  # Blue
            "Longer than granted": "#ff7f0e"    # Orange
        },
        title="NDA Monopoly Times: Granted vs. Actual",
        labels={
            "NDA_MMT_Years": "Granted Monopoly Period (Years)",
            "Actual_Monopoly_Years": "Actual Monopoly Period (Years)"
        },
        width=900,
        height=700
    )
    
    # Update traces with detailed information including company name
    fig.update_traces(
        hovertemplate="%{text}<extra></extra>",
        text=matched_data["click_text"],
        marker=dict(size=10, line=dict(width=1, color='white'))
    )
    
    # Calculate summary statistics
    shorter_count = (matched_data["Actual_Monopoly_Years"] < matched_data["NDA_MMT_Years"]).sum()
    total_count = len(matched_data)
    
    # Customize layout
    fig.update_layout(
        title_font_size=16,
        xaxis_title_font_size=14,
        yaxis_title_font_size=14,
        legend_title_text="Monopoly Duration",
        legend_title_font_size=12,
        showlegend=True,
        plot_bgcolor='white',
        paper_bgcolor='white',
        hovermode='closest',
        annotations=[
            dict(
                x=0.02,
                y=0.98,
                xref="paper",
                yref="paper",
                text=f"NDAs with shorter actual monopoly: {shorter_count}/{total_count} ({100*shorter_count/total_count:.1f}%)",
                showarrow=False,
                bgcolor="white",
                bordercolor="black",
                borderwidth=1,
                font=dict(size=12)
            )
        ]
    )
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    if show:
        # Save as HTML and open in browser instead of using fig.show()
        import webbrowser
        import os
        
        html_file = "nda_monopoly_times_plot.html"
        fig.write_html(html_file)
        
        # Get absolute path
        abs_path = os.path.abspath(html_file)
        
        print(f"📊 Interactive plot saved to: {html_file}")
        print(f"   Opening in browser...")
        
        # Open in default browser
        webbrowser.open('file://' + abs_path)
    
    return fig
