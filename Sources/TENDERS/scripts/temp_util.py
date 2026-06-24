import pandas as pd
import os

# Load the full CSV
df = pd.read_csv(r"D:\CDL\Bihar Scraper\tenders_full_data_2026.csv", parse_dates=["Publish Date"], dayfirst=False, encoding="utf-8")

# Create output folder if not exists
output_dir = r"D:\CDL\flood-data-ecosystem-Bihar\Sources\TENDERS\data\monthly_tenders"
os.makedirs(output_dir, exist_ok=True)

# Drop rows with missing Publish Date
df = df.dropna(subset=["Publish Date"])

# Group by Year-Month and write separate CSVs
df["Publish Date"] = pd.to_datetime(df["Publish Date"])
for (year, month), group in df.groupby([df["Publish Date"].dt.year, df["Publish Date"].dt.month]):
    file_name = f"{year}_{str(month).zfill(2)}_tenders.csv"
    file_path = os.path.join(output_dir, file_name)
    group.to_csv(file_path, index=False)


