"""
Hospital Data Analysis - SQL 10 Essential Techniques
A Mini Project demonstrating data handling, cleaning, and analysis using SQL (SQLite) as an alternative to Pandas.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

print("Starting SQL Mini-Project: Hospital Data Analysis\n")

# Setup paths
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
raw_data_path = os.path.join(base_dir, "data", "hospital_data.xlsx")
exports_dir = os.path.join(base_dir, "reports", "sql_tutorial_output")
db_path = os.path.join(exports_dir, "hospital_database.db")
os.makedirs(exports_dir, exist_ok=True)

# Helper function to print SQL results nicely
def print_query(conn, query, limit=5):
    try:
        df_result = pd.read_sql_query(query, conn)
        if len(df_result) > limit:
            print(df_result.head(limit).to_string(index=False))
            print(f"... ({len(df_result)} rows total)")
        else:
            print(df_result.to_string(index=False))
        return df_result
    except Exception as e:
        print(f"Error executing query: {e}")

# ==========================================
# 0. Preparation: Load raw Excel into SQLite
# ==========================================
# We use pandas just to populate the SQLite database initially
print("[0] Database Preparation  ------------------------------------")
df_raw = pd.read_excel(raw_data_path, sheet_name="Patient Records", engine="openpyxl")
# Create connection
conn = sqlite3.connect(db_path)
# Save data to SQLite table named 'patients'
df_raw.to_sql("patients", conn, if_exists="replace", index=False)
print(f"Created SQLite database '{db_path}' and loaded {len(df_raw)} records into 'patients' table.")


# ==========================================
# 1. Data Loading (Selecting Data)
# Equivalent to read_csv() / read_excel()
# ==========================================
print("\n[1] Data Loading (SQL SELECT) -----------------------------")
query_loading = "SELECT * FROM patients;"
print("Query: SELECT * FROM patients;")
print_query(conn, query_loading, limit=3)


# ==========================================
# 2. Data Inspection
# Equivalent to .head(), .tail(), .info(), .describe()
# ==========================================
print("\n[2] Data Inspection ---------------------------------------")
print("-> PRAGMA table_info (Like .info()):")
print_query(conn, "PRAGMA table_info(patients);")

print("\n-> SQL LIMIT (Like .head(3)):")
print_query(conn, "SELECT Patient_ID, Name, Age, Department FROM patients LIMIT 3;")

print("\n-> SQL AGGREGATES (Like .describe()):")
query_describe = """
SELECT 
    COUNT(Age) as count_age, 
    AVG(Age) as avg_age, 
    MIN(Age) as min_age, 
    MAX(Age) as max_age,
    AVG(Treatment_Cost_INR) as avg_cost
FROM patients;
"""
print_query(conn, query_describe)


# ==========================================
# 3. Data Cleaning
# Equivalent to .fillna(), .dropna(), .rename()
# ==========================================
print("\n[3] Data Cleaning -----------------------------------------")
cur = conn.cursor()

# First we simulate some NULLs
cur.execute("UPDATE patients SET Name = NULL WHERE Patient_ID IN ('HOS-00001', 'HOS-00002')")
cur.execute("UPDATE patients SET Age = NULL WHERE Patient_ID = 'HOS-00003'")
conn.commit()

# SQL fillna() -> UPDATE ... SET ... WHERE ... IS NULL
cur.execute("UPDATE patients SET Name = 'Unknown Patient' WHERE Name IS NULL")
# SQL dropna() -> DELETE FROM ... WHERE ... IS NULL
cur.execute("DELETE FROM patients WHERE Age IS NULL")
# SQL rename column -> ALTER TABLE ... RENAME COLUMN (Supported in modern SQLite)
cur.execute("ALTER TABLE patients RENAME COLUMN Treatment_Cost_INR TO Cost_INR")
conn.commit()
print("Cleaned NULLs and renamed 'Treatment_Cost_INR' to 'Cost_INR'.")


# ==========================================
# 4. Data Selection & Filtering
# Equivalent to .loc[], conditional filtering
# ==========================================
print("\n[4] Data Selection & Filtering ----------------------------")
query_filtering = """
SELECT Patient_ID, Name, Department, Cost_INR 
FROM patients 
WHERE Department = 'Pediatrics' AND Cost_INR > 50000
LIMIT 3;
"""
print("Querying high cost pediatric patients:")
print_query(conn, query_filtering)


# ==========================================
# 5. Data Manipulation
# Equivalent to adding columns, updating values, sort_values()
# ==========================================
print("\n[5] Data Manipulation -------------------------------------")
# Adding a column
try:
    cur.execute("ALTER TABLE patients ADD COLUMN Cost_USD REAL;")
except sqlite3.OperationalError:
    pass # Column might already exist if re-running
# Updating column values
cur.execute("UPDATE patients SET Cost_USD = ROUND(Cost_INR / 83.0, 2);")
conn.commit()

# Sorting using ORDER BY
query_sorting = """
SELECT Patient_ID, Age, Cost_INR, Cost_USD
FROM patients
ORDER BY Cost_INR DESC
LIMIT 3;
"""
print("Added 'Cost_USD' and ordered by highest cost:")
print_query(conn, query_sorting)


# ==========================================
# 6. Grouping & Aggregation
# Equivalent to .groupby()
# ==========================================
print("\n[6] Grouping & Aggregation --------------------------------")
query_groupby = """
SELECT 
    Department, 
    ROUND(AVG(Cost_INR), 2) as Avg_Cost, 
    ROUND(AVG(Length_of_Stay), 2) as Avg_Stay,
    COUNT(*) as Patient_Count
FROM patients
GROUP BY Department
ORDER BY Avg_Cost DESC;
"""
print("Average Cost and Stay by Department:")
print_query(conn, query_groupby)


# ==========================================
# 7. Merging & Joining
# Equivalent to pd.merge()
# ==========================================
print("\n[7] Merging & Joining -------------------------------------")
# Create a second table for Department Heads
cur.execute("""
CREATE TABLE IF NOT EXISTS department_heads (
    Department TEXT PRIMARY KEY,
    Head_Doctor TEXT
);
""")
cur.executemany("""
INSERT OR IGNORE INTO department_heads (Department, Head_Doctor) VALUES (?, ?)
""", [
    ('Cardiology', 'Dr. Sharma'),
    ('Emergency', 'Dr. Tiwari'),
    ('Neurology', 'Dr. Rao'),
    ('Pediatrics', 'Dr. Kapoor')
])
conn.commit()

# SQL LEFT JOIN
query_join = """
SELECT p.Patient_ID, p.Department, d.Head_Doctor
FROM patients p
LEFT JOIN department_heads d ON p.Department = d.Department
WHERE d.Head_Doctor IS NOT NULL
LIMIT 5;
"""
print("Joined patients table with department_heads table:")
print_query(conn, query_join)


# ==========================================
# 8. Data Transformation
# Equivalent to .apply(), .map(), .astype()
# ==========================================
print("\n[8] Data Transformation -----------------------------------")
# SQL CASE Statement (Alternative to mapping/apply)
query_transform = """
SELECT 
    Gender,
    CASE 
        WHEN Gender = 'Male' THEN 1 
        WHEN Gender = 'Female' THEN 0 
        ELSE NULL 
    END as Gender_Code,
    Length_of_Stay,
    CASE 
        WHEN Length_of_Stay < 3 THEN 'Short'
        WHEN Length_of_Stay <= 7 THEN 'Medium'
        ELSE 'Long'
    END as Stay_Category,
    CAST(Age AS REAL) as Casted_Age 
FROM patients
LIMIT 3;
"""
print("Transformed data using SQL CASE WHEN and CAST():")
print_query(conn, query_transform)


# ==========================================
# 9. Data Visualization
# Creating charts from SQL Result Sets
# ==========================================
print("\n[9] Data Visualization ------------------------------------")
print(f"Generating chart from SQL Query and saving to {exports_dir}")

# Fetch summarized data directly into pandas for plotting
df_chart = pd.read_sql_query("""
    SELECT Department, COUNT(Patient_ID) as patient_count 
    FROM patients 
    GROUP BY Department;
""", conn)

plt.style.use("seaborn-v0_8-darkgrid")
plt.figure(figsize=(10, 5))
plt.bar(df_chart['Department'], df_chart['patient_count'], color='teal', edgecolor='black')
plt.title("Patients per Department (Generated via SQL)")
plt.xlabel("Department")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(exports_dir, "sql_department_counts.png"))
plt.close()


# ==========================================
# 10. Exporting Data
# Save query results to a CSV File
# ==========================================
print("\n[10] Exporting Data ---------------------------------------")
out_csv = os.path.join(exports_dir, "sql_query_output.csv")

# Extract the groupby query from technique 6 and save
df_export = pd.read_sql_query(query_groupby, conn)
df_export.to_csv(out_csv, index=False)

print(f"Saved GroupBy SQL query results to CSV: {out_csv}")

conn.close()
print("\nSQL Mini-Project Execution Completed Successfully!")
