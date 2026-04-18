"""
📈 Hospital Data Analysis Module
Core analysis functions for patient flow, bed occupancy, and hospital efficiency.

Demonstrates 10 Essential Pandas Techniques:
1. DataFrame creation & loading
2. Data cleaning & missing values
3. Filtering & selection
4. Groupby & aggregation
5. Merge & join
6. Pivot tables
7. String operations
8. DateTime operations
9. Statistical analysis
10. Data visualization prep
"""

import pandas as pd
import numpy as np
import os
import sys

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data_cleaning import load_data, clean_data


# ═══════════════════════════════════════════════════════════════
# 1. PATIENT FLOW ANALYSIS
# ═══════════════════════════════════════════════════════════════

def analyze_patient_flow(df):
    """
    Analyze patient admission and discharge patterns.
    Uses: Groupby, DateTime operations, Pivot tables
    """
    print("\n📊 PATIENT FLOW ANALYSIS")
    print("=" * 60)
    
    # Monthly admissions trend
    monthly_admissions = df.groupby("Admission_Month").agg(
        Admissions=("Patient_ID", "count"),
        Avg_Stay=("Length_of_Stay", "mean"),
        Avg_Cost=("Treatment_Cost_INR", "mean"),
    ).round(2)
    
    print("\n📅 Monthly Admissions Trend (Latest 6 months):")
    print(monthly_admissions.tail(6).to_string())
    
    # Day-of-week pattern
    dow_pattern = df.groupby("Admission_DayOfWeek")["Patient_ID"].count()
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dow_pattern = dow_pattern.reindex(day_order)
    
    print("\n📆 Admissions by Day of Week:")
    for day, count in dow_pattern.items():
        bar = "█" * (count // 5)
        print(f"   {day:<12} {count:>4} {bar}")
    
    # Weekend vs Weekday
    weekend = df[df["Is_Weekend_Admission"]]["Patient_ID"].count()
    weekday = df[~df["Is_Weekend_Admission"]]["Patient_ID"].count()
    print(f"\n   Weekday Admissions: {weekday} ({weekday/len(df)*100:.1f}%)")
    print(f"   Weekend Admissions: {weekend} ({weekend/len(df)*100:.1f}%)")
    
    # Pivot: Department × Quarter
    pivot = pd.pivot_table(
        df, values="Patient_ID", index="Department",
        columns="Admission_Quarter", aggfunc="count", fill_value=0
    )
    print("\n📊 Department × Quarter Admissions Pivot:")
    print(pivot.to_string())
    
    return monthly_admissions


# ═══════════════════════════════════════════════════════════════
# 2. BED OCCUPANCY ANALYSIS
# ═══════════════════════════════════════════════════════════════

DEPT_BEDS = {
    "Cardiology": 40, "Orthopedics": 35, "Neurology": 30,
    "Pediatrics": 25, "General Medicine": 50, "Oncology": 30,
    "Gynecology": 30, "Emergency": 40, "Pulmonology": 20,
}

def analyze_bed_occupancy(df):
    """
    Calculate bed occupancy rates by department and time period.
    Uses: Groupby, Aggregation, Calculated columns
    """
    print("\n🛏️  BED OCCUPANCY ANALYSIS")
    print("=" * 60)
    
    # Monthly occupancy estimation
    monthly_dept = df.groupby(["Admission_Month", "Department"]).agg(
        Patient_Count=("Patient_ID", "count"),
        Total_Patient_Days=("Length_of_Stay", "sum"),
    ).reset_index()
    
    monthly_dept["Total_Beds"] = monthly_dept["Department"].map(DEPT_BEDS)
    monthly_dept["Bed_Days_Available"] = monthly_dept["Total_Beds"] * 30
    monthly_dept["Occupancy_Rate_%"] = (
        monthly_dept["Total_Patient_Days"] / monthly_dept["Bed_Days_Available"] * 100
    ).round(2)
    
    # Overall department occupancy
    dept_occupancy = monthly_dept.groupby("Department").agg(
        Avg_Occupancy=("Occupancy_Rate_%", "mean"),
        Peak_Occupancy=("Occupancy_Rate_%", "max"),
        Min_Occupancy=("Occupancy_Rate_%", "min"),
    ).round(2).sort_values("Avg_Occupancy", ascending=False)
    
    print("\n🏢 Department Bed Occupancy Rates:")
    print(f"   {'Department':<20} {'Avg %':>8} {'Peak %':>8} {'Min %':>8}  Status")
    print("   " + "-" * 62)
    for dept, row in dept_occupancy.iterrows():
        status = "🔴 Over" if row["Avg_Occupancy"] > 85 else (
            "🟡 High" if row["Avg_Occupancy"] > 70 else "🟢 Normal"
        )
        print(f"   {dept:<20} {row['Avg_Occupancy']:>7.1f}% {row['Peak_Occupancy']:>7.1f}% "
              f"{row['Min_Occupancy']:>7.1f}%  {status}")
    
    # Overall hospital occupancy
    total_beds = sum(DEPT_BEDS.values())
    total_patient_days = df["Length_of_Stay"].sum()
    date_range_days = (df["Admission_Date"].max() - df["Admission_Date"].min()).days + 1
    overall_occupancy = (total_patient_days / (total_beds * date_range_days)) * 100
    
    print(f"\n   🏥 Overall Hospital Occupancy: {overall_occupancy:.1f}%")
    print(f"   📊 Total Beds: {total_beds} | Total Patient-Days: {total_patient_days:,}")
    
    return dept_occupancy


# ═══════════════════════════════════════════════════════════════
# 3. DEPARTMENT PERFORMANCE
# ═══════════════════════════════════════════════════════════════

def analyze_departments(df):
    """
    Compare department performance metrics.
    Uses: Groupby, Multiple aggregations, Lambda functions
    """
    print("\n🏢 DEPARTMENT PERFORMANCE ANALYSIS")
    print("=" * 60)
    
    dept_perf = df.groupby("Department").agg(
        Total_Patients=("Patient_ID", "count"),
        Avg_Age=("Age", "mean"),
        Avg_Stay=("Length_of_Stay", "mean"),
        Median_Stay=("Length_of_Stay", "median"),
        Total_Revenue=("Treatment_Cost_INR", "sum"),
        Avg_Cost=("Treatment_Cost_INR", "mean"),
        Recovery_Rate=("Outcome", lambda x: (x == "Recovered").sum() / len(x) * 100),
        Mortality_Rate=("Outcome", lambda x: (x == "Deceased").sum() / len(x) * 100),
        Readmission_Rate=("Readmitted_Within_30_Days", lambda x: (x == "Yes").sum() / len(x) * 100),
        Insurance_Rate=("Insurance_Covered", lambda x: (x == "Yes").sum() / len(x) * 100),
    ).round(2)
    
    # Revenue share
    dept_perf["Revenue_Share_%"] = (
        dept_perf["Total_Revenue"] / dept_perf["Total_Revenue"].sum() * 100
    ).round(2)
    
    print("\n📋 Department Performance Summary:")
    print(dept_perf[["Total_Patients", "Avg_Stay", "Recovery_Rate", 
                     "Total_Revenue", "Revenue_Share_%"]].to_string())
    
    # Top & Bottom performers
    print(f"\n   🏆 Highest Recovery Rate: {dept_perf['Recovery_Rate'].idxmax()} "
          f"({dept_perf['Recovery_Rate'].max():.1f}%)")
    print(f"   💰 Highest Revenue:       {dept_perf['Total_Revenue'].idxmax()} "
          f"(₹{dept_perf['Total_Revenue'].max():,.0f})")
    print(f"   ⚡ Shortest Avg Stay:     {dept_perf['Avg_Stay'].idxmin()} "
          f"({dept_perf['Avg_Stay'].min():.1f} days)")
    
    return dept_perf


# ═══════════════════════════════════════════════════════════════
# 4. OUTCOME ANALYSIS
# ═══════════════════════════════════════════════════════════════

def analyze_outcomes(df):
    """
    Analyze treatment outcomes and readmission patterns.
    Uses: Filtering, Cross-tabulation, Statistical analysis
    """
    print("\n🎯 OUTCOME & READMISSION ANALYSIS")
    print("=" * 60)
    
    # Overall outcome distribution
    outcome_dist = df["Outcome"].value_counts()
    total = len(df)
    
    print("\n📊 Overall Outcome Distribution:")
    for outcome, count in outcome_dist.items():
        pct = count / total * 100
        bar = "█" * int(pct)
        print(f"   {outcome:<12} {count:>5} ({pct:>5.1f}%) {bar}")
    
    # Cross-tabulation: Department × Outcome
    cross_tab = pd.crosstab(df["Department"], df["Outcome"], normalize="index") * 100
    print("\n📊 Outcome Distribution by Department (%):")
    print(cross_tab.round(1).to_string())
    
    # Readmission analysis
    readmitted = df[df["Readmitted_Within_30_Days"] == "Yes"]
    not_readmitted = df[df["Readmitted_Within_30_Days"] == "No"]
    
    print(f"\n🔄 Readmission Analysis:")
    print(f"   Total Readmissions:    {len(readmitted)} ({len(readmitted)/total*100:.1f}%)")
    print(f"   Avg Stay (Readmitted): {readmitted['Length_of_Stay'].mean():.1f} days")
    print(f"   Avg Stay (Not):        {not_readmitted['Length_of_Stay'].mean():.1f} days")
    print(f"   Avg Cost (Readmitted): ₹{readmitted['Treatment_Cost_INR'].mean():,.0f}")
    print(f"   Avg Cost (Not):        ₹{not_readmitted['Treatment_Cost_INR'].mean():,.0f}")
    
    # Age group outcomes
    age_outcomes = pd.crosstab(df["Age_Group"], df["Outcome"], normalize="index") * 100
    print("\n📊 Outcome by Age Group (%):")
    print(age_outcomes.round(1).to_string())
    
    return outcome_dist


# ═══════════════════════════════════════════════════════════════
# 5. FINANCIAL ANALYSIS
# ═══════════════════════════════════════════════════════════════

def analyze_financials(df):
    """
    Revenue and cost analysis.
    Uses: Statistical methods, Groupby, Filtering
    """
    print("\n💰 FINANCIAL ANALYSIS")
    print("=" * 60)
    
    print(f"\n   Total Revenue:     ₹{df['Treatment_Cost_INR'].sum():,.0f}")
    print(f"   Average Cost:      ₹{df['Treatment_Cost_INR'].mean():,.0f}")
    print(f"   Median Cost:       ₹{df['Treatment_Cost_INR'].median():,.0f}")
    print(f"   Std Deviation:     ₹{df['Treatment_Cost_INR'].std():,.0f}")
    
    # Insurance analysis
    insured = df[df["Insurance_Covered"] == "Yes"]
    uninsured = df[df["Insurance_Covered"] == "No"]
    print(f"\n   💳 Insurance Coverage:")
    print(f"      Insured Patients:   {len(insured)} ({len(insured)/len(df)*100:.1f}%)")
    print(f"      Avg Cost (Insured): ₹{insured['Treatment_Cost_INR'].mean():,.0f}")
    print(f"      Avg Cost (Uninsured): ₹{uninsured['Treatment_Cost_INR'].mean():,.0f}")
    
    # Monthly revenue trend
    monthly_rev = df.groupby("Admission_Month")["Treatment_Cost_INR"].sum()
    print(f"\n   📈 Monthly Revenue Trend (Latest 6):")
    for month, rev in monthly_rev.tail(6).items():
        bar = "█" * (rev // 1000000)
        print(f"      {month}: ₹{rev:>12,.0f} {bar}")
    
    # Cost by diagnosis (Top 10)
    diag_cost = df.groupby("Diagnosis").agg(
        Count=("Patient_ID", "count"),
        Avg_Cost=("Treatment_Cost_INR", "mean"),
        Total_Cost=("Treatment_Cost_INR", "sum"),
    ).round(0).sort_values("Total_Cost", ascending=False).head(10)
    
    print(f"\n   💊 Top 10 Costliest Diagnoses:")
    print(diag_cost.to_string())
    
    return monthly_rev


# ═══════════════════════════════════════════════════════════════
# 6. COMPREHENSIVE SUMMARY
# ═══════════════════════════════════════════════════════════════

def generate_summary(df):
    """Generate a comprehensive hospital performance summary."""
    print("\n" + "═" * 60)
    print("🏥 HOSPITAL PERFORMANCE DASHBOARD SUMMARY")
    print("═" * 60)
    
    print(f"""
    📋 OVERVIEW
    ─────────────────────────────────────
    Total Patients:        {len(df):,}
    Date Range:            {df['Admission_Date'].min().date()} → {df['Admission_Date'].max().date()}
    Departments:           {df['Department'].nunique()}
    Unique Diagnoses:      {df['Diagnosis'].nunique()}
    
    🛏️  OPERATIONS
    ─────────────────────────────────────
    Avg Length of Stay:    {df['Length_of_Stay'].mean():.1f} days
    Recovery Rate:         {(df['Outcome'] == 'Recovered').sum() / len(df) * 100:.1f}%
    Readmission Rate:      {(df['Readmitted_Within_30_Days'] == 'Yes').sum() / len(df) * 100:.1f}%
    Weekend Admissions:    {df['Is_Weekend_Admission'].sum() / len(df) * 100:.1f}%
    
    💰 FINANCIALS
    ─────────────────────────────────────
    Total Revenue:         ₹{df['Treatment_Cost_INR'].sum():,.0f}
    Avg Treatment Cost:    ₹{df['Treatment_Cost_INR'].mean():,.0f}
    Insurance Coverage:    {(df['Insurance_Covered'] == 'Yes').sum() / len(df) * 100:.1f}%
    
    👥 DEMOGRAPHICS
    ─────────────────────────────────────
    Avg Patient Age:       {df['Age'].mean():.1f} years
    Male/Female Ratio:     {(df['Gender'] == 'Male').sum()}:{(df['Gender'] == 'Female').sum()}
    Most Common Dept:      {df['Department'].mode().values[0]}
    Most Common Diagnosis: {df['Diagnosis'].mode().values[0]}
    """)


# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    excel_path = os.path.join(base_dir, "data", "hospital_data.xlsx")
    
    # Load & clean
    df = load_data(excel_path)
    df = clean_data(df)
    
    # Run all analyses
    generate_summary(df)
    analyze_patient_flow(df)
    analyze_bed_occupancy(df)
    analyze_departments(df)
    analyze_outcomes(df)
    analyze_financials(df)
    
    print("\n✅ Analysis complete!")
