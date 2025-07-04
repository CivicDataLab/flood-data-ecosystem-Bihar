import pandas as pd
import os

# Load the full CSV
df = pd.read_csv("/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/TENDERS/scripts/tender_data_csv/all_tenders_bihar.csv", parse_dates=["Publish Date"], dayfirst=True, encoding="utf-8")

# Create output folder if not exists
output_dir = "/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/TENDERS/monthly_tenders"
os.makedirs(output_dir, exist_ok=True)

# Drop rows with missing Publish Date
df = df.dropna(subset=["Publish Date"])

# Group by Year-Month and write separate CSVs
for (year, month), group in df.groupby([df["Publish Date"].dt.year, df["Publish Date"].dt.month]):
    file_name = f"{year}_{str(month).zfill(2)}_tenders.csv"
    file_path = os.path.join(output_dir, file_name)
    group.to_csv(file_path, index=False)


