"""
Hospital Data Analysis - Pandas 10 Essential Techniques
A Mini Project demonstrating data handling, cleaning, and analysis using Pandas.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

print("Starting Pandas Mini-Project: Hospital Data Analysis\n")

# Setup paths
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
raw_data_path = os.path.join(base_dir, "data", "hospital_data.xlsx")
exports_dir = os.path.join(base_dir, "reports", "pandas_tutorial_output")
os.makedirs(exports_dir, exist_ok=True)

# ==========================================
# 1. Data Loading
# Reading data from files like CSV, Excel, SQL
# ==========================================
print("[1] Data Loading ------------------------------------------")
# read_excel()
df = pd.read_excel(raw_data_path, sheet_name="Patient Records", engine="openpyxl")
print(f"Loaded {len(df)} patient records into a DataFrame.")

# ==========================================
# 2. Data Inspection
# Understanding the dataset structure
# ==========================================
print("\n[2] Data Inspection ---------------------------------------")
print("-> head() - First 3 rows:")
print(df.head(3)[["Patient_ID", "Name", "Age", "Department"]])

print("\n-> tail() - Last 3 rows:")
print(df.tail(3)[["Patient_ID", "Name", "Age", "Department"]])

print("\n-> info() - Data types:")
df.info()

print("\n-> describe() - Summary statistics:")
print(df.describe())

# ==========================================
# 3. Data Cleaning
# Handling missing or incorrect data
# ==========================================
print("\n[3] Data Cleaning -----------------------------------------")
# Creating some artificial missing data for demonstration
df.loc[0:5, 'Name'] = np.nan
df.loc[5:10, 'Age'] = np.nan

# fillna() -> replace missing values
df['Name'] = df['Name'].fillna("Unknown Patient")
df['Age'] = df['Age'].fillna(df['Age'].median())

# dropna() -> remove missing values (if any remain)
df = df.dropna(subset=['Patient_ID', 'Department'])

# rename() -> change column names
df = df.rename(columns={
    "Patient_ID": "ID",
    "Treatment_Cost_INR": "Cost_INR"
})
print(f"Cleaned missing data and renamed columns: {list(df.columns[:5])}...")


# ==========================================
# 4. Data Selection & Filtering
# Selecting specific rows/columns
# ==========================================
print("\n[4] Data Selection & Filtering ----------------------------")
# Conditional filtering
high_cost_patients = df[df['Cost_INR'] > df['Cost_INR'].mean()]
print(f"Found {len(high_cost_patients)} patients with above-average treatment costs.")

# loc[] -> label/condition-based selection (Extracting Name and Cost for specific patients)
pediatric_cases = df.loc[df['Department'] == "Pediatrics", ["Name", "Department", "Cost_INR"]]
print("\n-> loc[] - Pediatrics Patients (Sample):")
print(pediatric_cases.head(3))

# iloc[] -> index-based selection (Rows 10 to 14)
print("\n-> iloc[] - Rows 10 to 12:")
print(df.iloc[10:13, :4])


# ==========================================
# 5. Data Manipulation
# Modifying data
# ==========================================
print("\n[5] Data Manipulation -------------------------------------")
# Adding new columns
# Assuming 1 USD = 83 INR
df['Cost_USD'] = (df['Cost_INR'] / 83).round(2)

# Updating values
# Giving a 10% discount to all Senior Citizens (Age > 65)
df.loc[df['Age'] > 65, 'Cost_INR'] = df.loc[df['Age'] > 65, 'Cost_INR'] * 0.9

# sort_values()
# Sorting patients by highest cost first
df = df.sort_values(by="Cost_INR", ascending=False)
print("Added 'Cost_USD', updated discounts, and sorted by 'Cost_INR':")
print(df[["ID", "Age", "Cost_INR", "Cost_USD"]].head(3))


# ==========================================
# 6. Grouping & Aggregation
# Grouping data for analysis
# ==========================================
print("\n[6] Grouping & Aggregation --------------------------------")
# groupby()
# Example: Average Cost and Avg Stay by Department
dept_analysis = df.groupby('Department')[['Cost_INR', 'Length_of_Stay']].mean().round(2)
print("Average Cost and Stay by Department:")
print(dept_analysis.head())


# ==========================================
# 7. Merging & Joining
# Combining multiple datasets
# ==========================================
print("\n[7] Merging & Joining -------------------------------------")
# Creating a dummy DataFrame for Department Heads
dept_heads_df = pd.DataFrame({
    'Department': ['Cardiology', 'Emergency', 'Neurology', 'Pediatrics'],
    'Head_Doctor': ['Dr. Sharma', 'Dr. Tiwari', 'Dr. Rao', 'Dr. Kapoor']
})

# merge() - Joining the main DataFrame with department heads
merged_df = pd.merge(df, dept_heads_df, on="Department", how="left")

# concat() - Concatenating two smaller subsets
df_part1 = df.iloc[0:5]
df_part2 = df.iloc[100:105]
concat_df = pd.concat([df_part1, df_part2])

print(f"merge() results in new columns: {list(merged_df.columns[-3:])}")
print(f"concat() combines 5+5 rows. Total logic length: {len(concat_df)}")


# ==========================================
# 8. Data Transformation
# Changing data format
# ==========================================
print("\n[8] Data Transformation -----------------------------------")
# map() -> mapping specific values
df['Gender_Code'] = df['Gender'].map({'Male': 1, 'Female': 0})

# apply() -> apply function
# Categorizing stay duration
df['Stay_Category'] = df['Length_of_Stay'].apply(lambda x: "Short" if x < 3 else ("Medium" if x <= 7 else "Long"))

# astype() -> change data type
df['Age'] = df['Age'].astype(float) # Converting integer age to float

print("Transformed Data (Sample):")
print(df[["Gender", "Gender_Code", "Length_of_Stay", "Stay_Category"]].head(3))


# ==========================================
# 9. Data Visualization
# Creating charts and graphs
# ==========================================
print("\n[9] Data Visualization ------------------------------------")
print(f"Generating visual charts and saving to {exports_dir}")

plt.style.use("seaborn-v0_8-darkgrid")

# Matplotlib: Bar chart of Patients by Department
plt.figure(figsize=(10, 5))
df['Department'].value_counts().plot(kind='bar', color='skyblue', edgecolor='black')
plt.title("Number of Patients per Department")
plt.xlabel("Department")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(exports_dir, "department_counts.png"))
plt.close()

# Seaborn: Distribution of Treatment Costs
plt.figure(figsize=(10, 5))
sns.histplot(df['Cost_INR'], bins=30, kde=True, color='purple')
plt.title("Distribution of Treatment Costs (INR)")
plt.xlabel("Cost (INR)")
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig(os.path.join(exports_dir, "cost_distribution.png"))
plt.close()


# ==========================================
# 10. Exporting Data
# Saving processed data
# ==========================================
print("\n[10] Exporting Data ---------------------------------------")
out_csv = os.path.join(exports_dir, "processed_hospital_data.csv")
out_excel = os.path.join(exports_dir, "processed_hospital_summary.xlsx")

# to_csv()
df.to_csv(out_csv, index=False)

# to_excel()
dept_analysis.to_excel(out_excel, sheet_name="Dept_Analysis")

print(f"Processed data exported successfully!")
print(f"   CSV: {out_csv}")
print(f"   Excel: {out_excel}")

print("\nPandas Mini-Project Execution Completed Successfully!")
