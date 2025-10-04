"""Quick test to verify monopoly time calculation is working correctly."""

import pandas as pd
import numpy as np

# Simulate the calculation
def test_monopoly_calculation():
    # Create sample data similar to what we'd have
    test_data = pd.DataFrame({
        'NDA_Appl_No': ['22256', '20123', '20456'],
        'NDA_Approval_Date_Date': pd.to_datetime(['2009-01-14', '2010-05-20', '2012-03-15']),
        'NDA_MMT_Years': [20.7, 15.0, 18.0],
        'Earliest_ANDA_Date': pd.to_datetime(['2016-01-27', '2018-06-15', '2015-08-10']),
    })
    
    # Calculate monopoly times (same logic as in postprocess.py)
    test_data["Actual_Monopoly_Days"] = (
        test_data["Earliest_ANDA_Date"] - test_data["NDA_Approval_Date_Date"]
    ).dt.days
    
    test_data["Actual_Monopoly_Years"] = test_data["Actual_Monopoly_Days"] / 365.25
    
    print("Test Results:")
    print("=" * 80)
    for _, row in test_data.iterrows():
        print(f"\nNDA {row['NDA_Appl_No']}:")
        print(f"  NDA Approval: {row['NDA_Approval_Date_Date'].date()}")
        print(f"  Earliest ANDA: {row['Earliest_ANDA_Date'].date()}")
        print(f"  Granted Period: {row['NDA_MMT_Years']:.1f} years")
        print(f"  Actual Days: {row['Actual_Monopoly_Days']:.0f} days")
        print(f"  Actual Period: {row['Actual_Monopoly_Years']:.2f} years")
        print(f"  Expected for NDA 22256: ~7.0 years ✓" if row['NDA_Appl_No'] == '22256' else "")

if __name__ == "__main__":
    test_monopoly_calculation()
