

import pandas as pd
import numpy as np
import os


def load_data(filepath):
    """Load patient data from Excel file."""
    print(f"📂 Loading data from: {filepath}")
    df = pd.read_excel(filepath, sheet_name="Patient Records", engine="openpyxl")
    print(f"   ✅ Loaded {len(df)} records with {len(df.columns)} columns")
    return df


def clean_data(df):
    """
    Apply comprehensive data cleaning:
    1. Handle missing values
    2. Fix data types
    3. Remove duplicates
    4. Create derived columns
    5. Validate ranges
    """
    print("🧹 Cleaning data...")
    initial_rows = len(df)
    
    # ── 1. Handle Missing Values ──
    df["Name"].fillna("Unknown Patient", inplace=True)
    df["Age"].fillna(df["Age"].median(), inplace=True)
    df["Treatment_Cost_INR"].fillna(df["Treatment_Cost_INR"].median(), inplace=True)
    df.dropna(subset=["Patient_ID", "Department", "Admission_Date"], inplace=True)
    
    # ── 2. Fix Data Types ──
    df["Admission_Date"] = pd.to_datetime(df["Admission_Date"])
    df["Discharge_Date"] = pd.to_datetime(df["Discharge_Date"])
    df["Age"] = df["Age"].astype(int)
    df["Length_of_Stay"] = df["Length_of_Stay"].astype(int)
    df["Treatment_Cost_INR"] = df["Treatment_Cost_INR"].astype(int)
    
    # ── 3. Remove Duplicates ──
    duplicates = df.duplicated(subset=["Patient_ID"]).sum()
    df.drop_duplicates(subset=["Patient_ID"], keep="first", inplace=True)
    if duplicates > 0:
        print(f"   ⚠️  Removed {duplicates} duplicate records")
    
    # ── 4. Create Derived Columns ──
    df["Admission_Month"] = df["Admission_Date"].dt.to_period("M").astype(str)
    df["Admission_Quarter"] = df["Admission_Date"].dt.to_period("Q").astype(str)
    df["Admission_Year"] = df["Admission_Date"].dt.year
    df["Admission_DayOfWeek"] = df["Admission_Date"].dt.day_name()
    df["Is_Weekend_Admission"] = df["Admission_Date"].dt.dayofweek >= 5
    
    # Age groups
    bins = [0, 12, 17, 30, 45, 60, 75, 100]
    labels = ["Child (0-12)", "Teen (13-17)", "Young Adult (18-30)",
              "Adult (31-45)", "Middle Aged (46-60)", "Senior (61-75)", "Elderly (76+)"]
    df["Age_Group"] = pd.cut(df["Age"], bins=bins, labels=labels, right=True)
    
    # Cost category
    cost_bins = [0, 25000, 75000, 150000, 300000, float("inf")]
    cost_labels = ["Low (<25K)", "Medium (25K-75K)", "High (75K-1.5L)", 
                   "Very High (1.5L-3L)", "Critical (>3L)"]
    df["Cost_Category"] = pd.cut(df["Treatment_Cost_INR"], bins=cost_bins, labels=cost_labels)
    
    # Stay category
    stay_bins = [0, 2, 5, 10, 20, float("inf")]
    stay_labels = ["Short (1-2d)", "Medium (3-5d)", "Long (6-10d)", 
                   "Extended (11-20d)", "Prolonged (>20d)"]
    df["Stay_Category"] = pd.cut(df["Length_of_Stay"], bins=stay_bins, labels=stay_labels)
    
    # ── 5. Validate Ranges ──
    df = df[(df["Age"] >= 0) & (df["Age"] <= 120)]
    df = df[df["Length_of_Stay"] > 0]
    df = df[df["Treatment_Cost_INR"] > 0]
    df = df[df["Discharge_Date"] >= df["Admission_Date"]]
    
    final_rows = len(df)
    print(f"   ✅ Cleaning complete: {initial_rows} → {final_rows} records "
          f"({initial_rows - final_rows} removed)")
    
    return df


def get_data_quality_report(df):
    """Generate a data quality report."""
    print("\n📊 DATA QUALITY REPORT")
    print("=" * 50)
    print(f"Total Records:      {len(df)}")
    print(f"Total Columns:      {len(df.columns)}")
    print(f"Missing Values:     {df.isnull().sum().sum()}")
    print(f"Duplicate IDs:      {df.duplicated(subset=['Patient_ID']).sum()}")
    print(f"Date Range:         {df['Admission_Date'].min().date()} → "
          f"{df['Admission_Date'].max().date()}")
    print(f"Age Range:          {df['Age'].min()} - {df['Age'].max()}")
    print(f"Cost Range:         ₹{df['Treatment_Cost_INR'].min():,} - "
          f"₹{df['Treatment_Cost_INR'].max():,}")
    print(f"Departments:        {df['Department'].nunique()}")
    print(f"Unique Diagnoses:   {df['Diagnosis'].nunique()}")
    print("=" * 50)


# ─── Main ────────────────────────────────────────────────────────
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    excel_path = os.path.join(base_dir, "data", "hospital_data.xlsx")
    
    df = load_data(excel_path)
    df = clean_data(df)
    get_data_quality_report(df)
    
    # Save cleaned data
    cleaned_path = os.path.join(base_dir, "data", "hospital_data_cleaned.xlsx")
    df.to_excel(cleaned_path, index=False, engine="openpyxl")
    print(f"\n📁 Cleaned data saved to: {cleaned_path}")
