import pandas as pd

def remove_column_and_rows(csv_file_path, column_name, output_file_path):
    """
    Removes a specified column and its corresponding row values from a CSV file.

    Args:
        csv_file_path (str): The path to the input CSV file.
        column_name (str): The name of the column to remove.
        output_file_path (str): The path to save the modified CSV file.
    """
    try:
        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(csv_file_path)

        # Check if the column exists
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in the CSV file.")

        # Remove the specified column
        df = df.drop(column_name, axis=1)

        # Remove rows where all values are NaN (empty) after dropping the column.
        # This handles the case where the entire row becomes empty.
        df = df.dropna(how='all')


        # Save the modified DataFrame to a new CSV file
        df.to_csv(output_file_path, index=False)

        print(f"Column '{column_name}' and its corresponding rows removed successfully.")
        print(f"Modified CSV saved to: {output_file_path}")

    except FileNotFoundError:
        print(f"Error: File not found at {csv_file_path}")
    except ValueError as e:
        print(f"ValueError: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# Example usage:
csv_file = "/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/ANTYODAYA/data/variables/antyodaya_variables.csv"  # Replace with your CSV file name
column_to_remove = "geometry"  # Replace with the column name you want to remove
output_file = "/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/ANTYODAYA/data/variables/antyodaya_variables_1.csv"  # Replace with your desired output file name

remove_column_and_rows(csv_file, column_to_remove, output_file)