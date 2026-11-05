from pathlib import Path
import pandas as pd

# Define project directories
PROJECT_ROOT = Path(__file__).resolve().parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"



def inspect_csv(file_path):

    df = pd.read_csv(file_path)

    report = {
        "File Name": file_path.name,
        "Rows": df.shape[0],
        "Columns": df.shape[1],
        "Missing Values": df.isnull().sum().sum(),
        "Duplicate Rows": df.duplicated().sum()
    }

    return report


def generate_report():

    report_list = []

    csv_files = RAW_DATA_DIR.glob("*.csv")

    for file_path in csv_files:

        print(f"Checking: {file_path.name}")

        try:
            report = inspect_csv(file_path)
            report_list.append(report)

        except Exception as e:
            print(f"Error reading {file_path.name}: {e}")

    report_df = pd.DataFrame(report_list)

    return report_df

def generate_missing_report():

    missing_list = []

    csv_files = RAW_DATA_DIR.glob("*.csv")

    for file_path in csv_files:

        print(f"Checking missing values: {file_path.name}")

        try:
            df = pd.read_csv(file_path)

            missing_counts = df.isnull().sum()

            for column, count in missing_counts.items():

                if count > 0:
                    missing_list.append({
                        "File Name": file_path.name,
                        "Column Name": column,
                        "Missing Values": count
                    })

        except Exception as e:
            print(f"Error reading {file_path.name}: {e}")

    return pd.DataFrame(missing_list)


if __name__ == "__main__":

    report_df = generate_report()

    print("\nData Quality Report:")
    print(report_df)

    report_df.to_csv(
        PROJECT_ROOT / "data_quality_report.csv",
        index=False
    )

    missing_df = generate_missing_report()

    missing_df.to_csv(
    PROJECT_ROOT / "missing_values_report.csv",
    index=False
    )

    print("\nMissing Values Report:")
    print(missing_df)

    print("\nBoth reports saved successfully!")