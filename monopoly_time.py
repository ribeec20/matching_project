"""Visualization utilities for monopoly time analyses.

This module can be used in two ways:
1. As part of the dosage pipeline (receives pre-computed DataFrame)
2. Standalone by reading final_nda_anda_matches.txt and computing monopoly times

Usage:
    # Standalone (reads txt file and computes monopoly times)
    python monopoly_time.py final_nda_anda_matches.txt
    
    # Or in Python code
    from monopoly_time import create_monopoly_plot_from_file
    create_monopoly_plot_from_file("final_nda_anda_matches.txt")
"""

from __future__ import annotations

import sys
import re
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

__all__ = ["plot_monopoly_scatter", "create_monopoly_plot_from_file", "parse_matches_file", "load_submissions_data", "calculate_monopoly_times_from_matches"]


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
    """Generate interactive histogram with scatter points of actual monopoly times.
    
    Creates a visualization showing:
    - Histogram bins: Distribution of actual monopoly periods
    - Scatter points: Individual NDAs to show matches
    - X-axis: Actual monopoly time bins (years)
    - Y-axis: Count of NDAs in each bin
    - Click info: NDA details and matched ANDA information
    """
    # DEBUG: Print column names and sample data
    print("\n🔍 DEBUG: Checking data before plotting...")
    print(f"Columns in nda_monopoly_times: {nda_monopoly_times.columns.tolist()}")
    
    if 'Actual_Monopoly_Years' in nda_monopoly_times.columns:
        print(f"\nSample Actual_Monopoly_Years values:")
        print(nda_monopoly_times[['NDA_Appl_No', 'Actual_Monopoly_Years']].head(10))
    
    # Filter to only NDAs with calculated monopoly times
    matched_data = nda_monopoly_times[
        nda_monopoly_times["Actual_Monopoly_Years"].notna()
    ].copy()
    
    if matched_data.empty:
        print("No NDAs with calculated monopoly times to plot.")
        return
    
    print(f"\n✓ Found {len(matched_data)} NDAs with calculated monopoly times")
    print(f"  Actual_Monopoly_Years range: {matched_data['Actual_Monopoly_Years'].min():.2f} to {matched_data['Actual_Monopoly_Years'].max():.2f}")
    print(f"  Mean: {matched_data['Actual_Monopoly_Years'].mean():.2f}, Median: {matched_data['Actual_Monopoly_Years'].median():.2f}")
    print()
    
    # Create bins for the histogram (1-year intervals)
    bin_size = 1.0  # 1 year bins
    max_years = np.ceil(matched_data['Actual_Monopoly_Years'].max())
    bins = np.arange(0, max_years + bin_size, bin_size)
    
    # Assign each NDA to a bin
    matched_data['Bin'] = pd.cut(matched_data['Actual_Monopoly_Years'], bins=bins, include_lowest=True)
    matched_data['Bin_Center'] = matched_data['Bin'].apply(lambda x: x.mid if pd.notna(x) else np.nan)
    
    # Count NDAs in each bin
    bin_counts = matched_data.groupby('Bin_Center').size().reset_index(name='Count')
    
    # Create detailed hover text for each point
    matched_data["hover_text"] = matched_data.apply(
        lambda row: (
            f"<b>NDA {row['NDA_Appl_No']}</b><br>"
            f"Sponsor: {row['NDA_Sponsor'] if 'NDA_Sponsor' in row.index and pd.notna(row['NDA_Sponsor']) else 'N/A'}<br>"
            f"Drug: {row['NDA_DrugName'] if 'NDA_DrugName' in row.index and pd.notna(row['NDA_DrugName']) else 'N/A'}<br>"
            f"Ingredient: {row['NDA_Ingredient']}<br>"
            f"NDA Approval: {row['NDA_Approval_Date']}<br>"
            f"Earliest ANDA: {row['Earliest_ANDA_Date'].strftime('%Y-%m-%d') if pd.notna(row['Earliest_ANDA_Date']) else 'N/A'}<br>"
            f"Actual Monopoly: {row['Actual_Monopoly_Years']:.2f} years<br>"
            f"Matching ANDAs: {row['Num_Matching_ANDAs']}<br>"
            f"Top ANDAs: {_limit_anda_list(row['Matching_ANDA_List'] if 'Matching_ANDA_List' in row.index and pd.notna(row['Matching_ANDA_List']) else 'N/A', 3)}"
        ), 
        axis=1
    )
    
    # For each bin, calculate the y-position for each NDA (stacked vertically)
    matched_data['Y_Position'] = 0
    for bin_center in bin_counts['Bin_Center']:
        mask = matched_data['Bin_Center'] == bin_center
        count = mask.sum()
        if count > 0:
            # Stack points vertically from 0.5 to count + 0.5
            matched_data.loc[mask, 'Y_Position'] = np.arange(1, count + 1)
    
    # Create figure with both histogram and scatter
    fig = go.Figure()
    
    # Add histogram bars
    fig.add_trace(go.Bar(
        x=bin_counts['Bin_Center'],
        y=bin_counts['Count'],
        name='Distribution',
        marker_color='lightblue',
        marker_line_color='darkblue',
        marker_line_width=1,
        opacity=0.6,
        width=bin_size * 0.8,
        hovertemplate='Monopoly Time: %{x:.1f} years<br>Count: %{y}<extra></extra>'
    ))
    
    # Add scatter points on top
    fig.add_trace(go.Scatter(
        x=matched_data['Bin_Center'],
        y=matched_data['Y_Position'],
        mode='markers',
        name='Individual NDAs',
        marker=dict(
            size=8,
            color='darkblue',
            line=dict(width=1, color='white'),
            opacity=0.7
        ),
        text=matched_data['hover_text'],
        hovertemplate='%{text}<extra></extra>'
    ))
    
    total_count = len(matched_data)
    
    # Customize layout
    fig.update_layout(
        title=dict(
            text=f"Distribution of Actual NDA Monopoly Times<br><sub>Total: {total_count} NDAs with matched ANDAs</sub>",
            font=dict(size=18)
        ),
        xaxis_title="Actual Monopoly Time (Years)",
        yaxis_title="Number of NDAs",
        xaxis_title_font_size=14,
        yaxis_title_font_size=14,
        showlegend=True,
        legend=dict(x=0.7, y=0.95),
        plot_bgcolor='white',
        paper_bgcolor='white',
        hovermode='closest',
        width=1000,
        height=600,
        annotations=[
            dict(
                x=0.02,
                y=0.98,
                xref="paper",
                yref="paper",
                text=f"Mean: {matched_data['Actual_Monopoly_Years'].mean():.1f} years<br>Median: {matched_data['Actual_Monopoly_Years'].median():.1f} years<br>Range: {matched_data['Actual_Monopoly_Years'].min():.1f} - {matched_data['Actual_Monopoly_Years'].max():.1f} years",
                showarrow=False,
                bgcolor="white",
                bordercolor="black",
                borderwidth=1,
                font=dict(size=11),
                align='left'
            )
        ]
    )
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray', range=[-0.5, max_years + 0.5])
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


def parse_matches_file(filepath: str) -> Dict[str, List[str]]:
    """Parse final_nda_anda_matches.txt file to extract NDA-ANDA mappings.
    
    Args:
        filepath: Path to final_nda_anda_matches.txt file
        
    Returns:
        Dictionary mapping NDA number (str) to list of ANDA numbers (list of str)
        
    Example:
        {'019955': ['200653', '201831', '207880', '076470', '077122', '077414'], ...}
    """
    nda_anda_map = {}
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            
            # Skip header lines, empty lines, and separator lines
            if not line or line.startswith('=') or line.startswith('-') or line.startswith('Total') or line.startswith('Generated'):
                continue
            
            # Match lines like: NDA19955: 200653, 200653, 201831, ...
            match = re.match(r'NDA(\d+):\s*(.+)', line)
            if match:
                nda_num = match.group(1).zfill(6)  # Pad to 6 digits
                anda_list_str = match.group(2)
                
                # Split ANDAs and clean up
                anda_numbers = [anda.strip().zfill(6) for anda in anda_list_str.split(',') if anda.strip()]
                
                # Remove duplicates while preserving order
                unique_andas = []
                seen = set()
                for anda in anda_numbers:
                    if anda not in seen:
                        unique_andas.append(anda)
                        seen.add(anda)
                
                nda_anda_map[nda_num] = unique_andas
    
    return nda_anda_map


def load_submissions_data(submissions_path: str = "txts/OB txts/Submissions.txt") -> pd.DataFrame:
    """Load Submissions.txt to get approval dates for NDAs and ANDAs.
    
    Args:
        submissions_path: Path to Submissions.txt file
        
    Returns:
        DataFrame with columns: ApplNo, SubmissionType, SubmissionStatusDate
    """
    print(f"Loading submissions data from {submissions_path}...")
    
    # Load with latin-1 encoding (required for Submissions.txt)
    submissions = pd.read_csv(submissions_path, sep='\t', encoding='latin-1', dtype={'ApplNo': str})
    
    # Convert submission dates to datetime
    submissions['SubmissionStatusDate'] = pd.to_datetime(submissions['SubmissionStatusDate'], errors='coerce')
    
    # Pad ApplNo to 6 digits
    submissions['ApplNo'] = submissions['ApplNo'].str.zfill(6)
    
    print(f"  Loaded {len(submissions)} submission records")
    return submissions


def calculate_monopoly_times_from_matches(
    nda_anda_map: Dict[str, List[str]],
    submissions_df: pd.DataFrame,
    applications_path: str = "txts/OB txts/Applications.txt",
    products_path: str = "txts/OB txts/Products.txt"
) -> pd.DataFrame:
    """Calculate monopoly times from NDA-ANDA matches using Submissions.txt and Orange Book data.
    
    Args:
        nda_anda_map: Dictionary mapping NDA numbers to lists of ANDA numbers
        submissions_df: DataFrame from Submissions.txt with approval dates
        applications_path: Path to Applications.txt (for sponsor info)
        products_path: Path to Products.txt (for drug/ingredient info)
        
    Returns:
        DataFrame with monopoly time calculations ready for plotting
    """
    print(f"\nCalculating monopoly times for {len(nda_anda_map)} NDAs...")
    
    # Load Orange Book data for NDA information
    try:
        print(f"  Loading Orange Book data...")
        applications_df = pd.read_csv(applications_path, sep='\t', encoding='latin-1', dtype={'ApplNo': str})
        applications_df['ApplNo'] = applications_df['ApplNo'].str.zfill(6)
        
        products_df = pd.read_csv(products_path, sep='\t', encoding='latin-1', dtype={'ApplNo': str})
        products_df['ApplNo'] = products_df['ApplNo'].str.zfill(6)
        
        print(f"  Loaded {len(applications_df)} applications and {len(products_df)} products from Orange Book")
    except Exception as e:
        print(f"  Warning: Could not load Orange Book data: {e}")
        applications_df = None
        products_df = None
    
    # Filter submissions to only ORIG approvals (original approvals with submission #1)
    orig_approvals = submissions_df[
        (submissions_df['SubmissionType'] == 'ORIG') &
        (submissions_df['SubmissionNo'] == 1) &
        (submissions_df['SubmissionStatus'] == 'AP')
    ].copy()
    
    # Get earliest approval date per application
    app_approvals = orig_approvals.groupby('ApplNo')['SubmissionStatusDate'].min().reset_index()
    app_approvals.columns = ['ApplNo', 'Approval_Date']
    
    print(f"  Found approval dates for {len(app_approvals)} applications")
    
    # Calculate monopoly times for each NDA
    monopoly_records = []
    
    for nda_num, anda_list in nda_anda_map.items():
        # Get NDA approval date
        nda_approval = app_approvals[app_approvals['ApplNo'] == nda_num]
        
        if nda_approval.empty:
            print(f"  Warning: No approval date found for NDA {nda_num}")
            continue
        
        nda_approval_date = nda_approval.iloc[0]['Approval_Date']
        
        if pd.isna(nda_approval_date):
            continue
        
        # Get ANDA approval dates
        anda_approvals = app_approvals[app_approvals['ApplNo'].isin(anda_list)].copy()
        
        # Filter ANDAs approved after NDA
        anda_approvals = anda_approvals[anda_approvals['Approval_Date'] > nda_approval_date]
        
        if anda_approvals.empty:
            # No ANDAs approved after this NDA
            continue
        
        # Find earliest ANDA approval
        earliest_anda_date = anda_approvals['Approval_Date'].min()
        earliest_anda_num = anda_approvals[anda_approvals['Approval_Date'] == earliest_anda_date].iloc[0]['ApplNo']
        
        # Calculate monopoly time
        monopoly_days = (earliest_anda_date - nda_approval_date).days
        monopoly_years = monopoly_days / 365.25
        
        # Get NDA info from Orange Book
        nda_sponsor = 'Unknown'
        nda_drug_name = 'Unknown'
        nda_ingredient = 'Unknown'
        
        if applications_df is not None:
            app_row = applications_df[applications_df['ApplNo'] == nda_num]
            if not app_row.empty:
                nda_sponsor = app_row.iloc[0].get('SponsorName', 'Unknown')
        
        if products_df is not None:
            # Get the first product for this NDA (multiple products may exist)
            prod_row = products_df[products_df['ApplNo'] == nda_num]
            if not prod_row.empty:
                nda_drug_name = prod_row.iloc[0].get('DrugName', 'Unknown')
                nda_ingredient = prod_row.iloc[0].get('ActiveIngredient', 'Unknown')
        
        record = {
            'NDA_Appl_No': nda_num,
            'NDA_Approval_Date': nda_approval_date.strftime('%Y-%m-%d'),
            'NDA_Approval_Date_Date': nda_approval_date,
            'NDA_Sponsor': nda_sponsor,
            'NDA_DrugName': nda_drug_name,
            'NDA_Ingredient': nda_ingredient,
            'Earliest_ANDA_Date': earliest_anda_date,
            'Earliest_ANDA_Number': earliest_anda_num,
            'Actual_Monopoly_Days': monopoly_days,
            'Actual_Monopoly_Years': monopoly_years,
            'Num_Matching_ANDAs': len(anda_list),
            'Matching_ANDA_List': ' | '.join(anda_list)
        }
        
        monopoly_records.append(record)
    
    monopoly_df = pd.DataFrame(monopoly_records)
    
    print(f"  Calculated monopoly times for {len(monopoly_df)} NDAs")
    if not monopoly_df.empty:
        print(f"  Monopoly time range: {monopoly_df['Actual_Monopoly_Years'].min():.2f} to {monopoly_df['Actual_Monopoly_Years'].max():.2f} years")
    
    return monopoly_df


def create_monopoly_plot_from_file(
    matches_file: str,
    submissions_path: str = "txts/OB txts/Submissions.txt",
    applications_path: str = "txts/OB txts/Applications.txt",
    products_path: str = "txts/OB txts/Products.txt",
    show: bool = True
) -> Tuple[pd.DataFrame, go.Figure]:
    """Create monopoly time plot from final_nda_anda_matches.txt file.
    
    This is the main function for standalone usage. It:
    1. Parses the matches file
    2. Loads Submissions.txt for approval dates
    3. Loads Orange Book data for NDA information
    4. Calculates monopoly times
    5. Creates the interactive histogram with scatter points
    
    Args:
        matches_file: Path to final_nda_anda_matches.txt or final_nda_anda_matches_2025.txt
        submissions_path: Path to Submissions.txt file
        applications_path: Path to Applications.txt file (Orange Book)
        products_path: Path to Products.txt file (Orange Book)
        show: Whether to display the plot in browser
        
    Returns:
        Tuple of (monopoly_times_df, plotly_figure)
        
    Example:
        >>> df, fig = create_monopoly_plot_from_file("final_nda_anda_matches_2025.txt")
        >>> # Plot is automatically shown in browser if show=True
    """
    print(f"\n{'='*70}")
    print(f"Creating Monopoly Time Plot from: {matches_file}")
    print(f"{'='*70}\n")
    
    # Step 1: Parse matches file
    nda_anda_map = parse_matches_file(matches_file)
    print(f"✓ Parsed {len(nda_anda_map)} NDA-ANDA match groups from file")
    
    # Step 2: Load submissions data
    submissions_df = load_submissions_data(submissions_path)
    
    # Step 3: Calculate monopoly times
    monopoly_times_df = calculate_monopoly_times_from_matches(
        nda_anda_map,
        submissions_df,
        applications_path,
        products_path
    )
    
    # Step 4: Create plot
    print(f"\n{'='*70}")
    print("Generating Interactive Plot...")
    print(f"{'='*70}\n")
    
    fig = plot_monopoly_scatter(monopoly_times_df, show=show)
    
    print(f"\n{'='*70}")
    print("✓ Complete!")
    print(f"{'='*70}\n")
    
    return monopoly_times_df, fig


if __name__ == "__main__":
    """Standalone execution: python monopoly_time.py [matches_file]"""
    
    if len(sys.argv) < 2:
        print("Usage: python monopoly_time.py <final_nda_anda_matches.txt>")
        print("\nExample:")
        print("  python monopoly_time.py final_nda_anda_matches.txt")
        print("  python monopoly_time.py final_nda_anda_matches_2025.txt")
        sys.exit(1)
    
    matches_file = sys.argv[1]
    
    if not Path(matches_file).exists():
        print(f"Error: File not found: {matches_file}")
        sys.exit(1)
    
    # Run the standalone analysis
    df, fig = create_monopoly_plot_from_file(matches_file)
    
    # Save the data to test_results folder
    output_filename = Path(matches_file).stem + '_monopoly_times.csv'
    output_csv = Path('test_results') / output_filename
    output_csv.parent.mkdir(exist_ok=True)
    df.to_csv(output_csv, index=False)
    print(f"✓ Monopoly times saved to: {output_csv}")
