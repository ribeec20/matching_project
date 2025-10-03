"""Visualization utilities for monopoly time analyses."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any

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


def plot_monopoly_scatter(match_objects: Dict[str, Any], show: bool = True) -> None:
    """Generate interactive scatter plot of NDA monopoly times with click details.
    
    Creates an interactive scatter plot showing:
    - X-axis: Granted monopoly period (NDA_MMT_Years)
    - Y-axis: Actual monopoly period (Actual_Monopoly_Years)  
    - Colors: Validation status and monopoly duration comparison
    - Click info: NDA details and matched ANDA information
    - Only shows NDAs with calculated monopoly times (matched cases)
    
    Args:
        match_objects: Dictionary of Match objects from class-based system
        show: Whether to display the plot
    """
    # Convert Match objects to DataFrame for plotting
    plot_data = []
    
    for match_name, match in match_objects.items():
        # Get monopoly summary for this match
        monopoly_summary = match.get_monopoly_summary()
        
        # Only include matches with calculated monopoly times and actual ANDA matches
        if (monopoly_summary.get('monopoly_time_years') is not None and 
            monopoly_summary.get('nda_mmt_years') is not None and
            len(match.get_matches()) > 0):
            
            # Get validation summary
            validation_summary = match.get_validation_summary()
            
            # Create ANDA list string
            anda_numbers = [anda.get_anda_number() for anda in match.get_matches()]
            anda_list_str = " | ".join(anda_numbers) if anda_numbers else "N/A"
            
            # Determine validation status
            validation_method = validation_summary.get('validation_method', 'Unknown')
            if validation_method == 'pdf_validation':
                validation_status = "Validated"
            elif validation_method == 'conservative_validation':
                validation_status = "Conservative"
            else:
                validation_status = "Not Validated"
            
            plot_data.append({
                'NDA_Appl_No': match.nda.get_nda_number(),
                'NDA_Applicant': match.nda.get_applicant(),
                'NDA_Ingredient': match.nda.get_ingredient(),
                'NDA_Approval_Date': match.nda.get_approval_date_str(),
                'NDA_MMT_Years': monopoly_summary.get('nda_mmt_years'),
                'Actual_Monopoly_Years': monopoly_summary.get('monopoly_time_years'),
                'Earliest_ANDA_Date': monopoly_summary.get('earliest_anda_date'),
                'Num_Matching_ANDAs': len(anda_numbers),
                'Matching_ANDA_List': anda_list_str,
                'Validation_Status': validation_status,
                'Match_Name': match_name
            })
    
    if not plot_data:
        print("No NDAs with calculated monopoly times to plot.")
        return
    
    # Convert to DataFrame
    matched_data = pd.DataFrame(plot_data)
    
    # Create detailed text with company information (limited to first 6 ANDA matches)
    matched_data["click_text"] = matched_data.apply(
        lambda row: (
            f"<b>NDA {row['NDA_Appl_No']}</b><br>"
            f"Company: {row.get('NDA_Applicant', 'N/A')}<br>"
            f"Ingredient: {row['NDA_Ingredient']}<br>"
            f"NDA Approval: {row['NDA_Approval_Date']}<br>"
            f"Earliest ANDA: {row['Earliest_ANDA_Date'] if pd.notna(row['Earliest_ANDA_Date']) else 'N/A'}<br>"
            f"Granted Period: {row['NDA_MMT_Years']:.1f} years<br>"
            f"Actual Period: {row['Actual_Monopoly_Years']:.1f} years<br>"
            f"Difference: {row['Actual_Monopoly_Years'] - row['NDA_MMT_Years']:.1f} years<br>"
            f"Validation Status: {row.get('Validation_Status', 'Unknown')}<br>"
            f"Matching ANDAs ({row['Num_Matching_ANDAs']}): {_limit_anda_list(row.get('Matching_ANDA_List', 'N/A'), 6)}"
        ), 
        axis=1
    )
    
    # Determine color based on validation status and monopoly duration
    def get_color_category(row):
        validation_status = row.get('Validation_Status', 'Unknown')
        actual_years = row['Actual_Monopoly_Years']
        granted_years = row['NDA_MMT_Years']
        
        if validation_status == 'Validated':
            return "Validated - Shorter than granted" if actual_years < granted_years else "Validated - Longer than granted"
        elif validation_status == 'Conservative':
            return "Conservative - Shorter than granted" if actual_years < granted_years else "Conservative - Longer than granted"
        else:
            return "Not Validated"
    
    matched_data["color_category"] = matched_data.apply(get_color_category, axis=1)
    
    # Create the interactive scatter plot with validation-aware colors
    fig = px.scatter(
        matched_data,
        x="NDA_MMT_Years",
        y="Actual_Monopoly_Years",
        color="color_category",
        color_discrete_map={
            "Validated - Shorter than granted": "#1f77b4",        # Blue (PDF validated, shorter)
            "Validated - Longer than granted": "#2ca02c",         # Green (PDF validated, longer)
            "Conservative - Shorter than granted": "#ff7f0e",     # Orange (conservative, shorter)
            "Conservative - Longer than granted": "#d62728",      # Red (conservative, longer)
            "Not Validated": "#9467bd"                            # Purple (not validated)
        },
        title="NDA Monopoly Times: Granted vs. Actual (Class-Based with Company Validation)",
        labels={
            "NDA_MMT_Years": "Granted Monopoly Period (Years)",
            "Actual_Monopoly_Years": "Actual Monopoly Period (Years)"
        },
        width=900,
        height=700
    )
    
    # Update traces with detailed information including validation status
    fig.update_traces(
        hovertemplate="%{text}<extra></extra>",
        text=matched_data["click_text"],
        marker=dict(size=10, line=dict(width=1, color='white'))
    )
    
    # Calculate summary statistics
    total_count = len(matched_data)
    validated_count = len(matched_data[matched_data["Validation_Status"] == "Validated"])
    conservative_count = len(matched_data[matched_data["Validation_Status"] == "Conservative"]) 
    not_validated_count = total_count - validated_count - conservative_count
    
    validated_shorter = len(matched_data[
        (matched_data["Validation_Status"] == "Validated") & 
        (matched_data["Actual_Monopoly_Years"] < matched_data["NDA_MMT_Years"])
    ])
    
    conservative_shorter = len(matched_data[
        (matched_data["Validation_Status"] == "Conservative") & 
        (matched_data["Actual_Monopoly_Years"] < matched_data["NDA_MMT_Years"])
    ])
    
    # Customize layout
    fig.update_layout(
        title_font_size=16,
        xaxis_title_font_size=14,
        yaxis_title_font_size=14,
        legend_title_text="Match Validation & Duration",
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
                text=(
                    f"Total NDAs: {total_count}<br>"
                    f"PDF validated: {validated_count} ({100*validated_count/total_count:.1f}%)<br>"
                    f"Conservative: {conservative_count} ({100*conservative_count/total_count:.1f}%)<br>"
                    f"Not validated: {not_validated_count} ({100*not_validated_count/total_count:.1f}%)<br>"
                    f"PDF shorter than granted: {validated_shorter}/{validated_count}<br>"
                    f"Conservative shorter: {conservative_shorter}/{conservative_count}"
                ),
                showarrow=False,
                bgcolor="white",
                bordercolor="black",
                borderwidth=1,
                font=dict(size=11),
                align="left"
            )
        ]
    )
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    if show:
        print("📊 Interactive plot opening in browser...")
        print(f"   Company validation results: {validated_count} PDF validated, {conservative_count} conservative, {not_validated_count} not validated")
        fig.show()
    
    return fig
