import os
import pandas as pd

def merge_and_filter_csv_files(input_dir, output_file):
    all_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    df_list = []
    
    for file in all_files:
        file_path = os.path.join(input_dir, file)
        print(f"Reading {file_path}")
        df = pd.read_csv(file_path, header=None, delimiter=',')
        df_list.append(df)
    
    combined_df = pd.concat(df_list, ignore_index=True)
    print(f"Combined {len(all_files)} files into a single dataframe")

    duplicate_df = combined_df[combined_df[0].duplicated()]
    duplicate_df.to_csv('duplicates.csv', index=False, header=False, sep='^')
    print(f"Duplicate rows saved to duplicates.csv")
    
    filtered_df = combined_df.drop_duplicates()
    print(f"Removed {len(combined_df) - len(filtered_df)} duplicates")
    
    filtered_df.sort_values(by=0, inplace=True)
    
    print(f"Filtered dataframe has {len(filtered_df)} rows")
    filtered_df.to_csv(output_file, sep='^', index=False, header=False)
    print(f"Merged file saved to {output_file}")

input_directory = '../output/financial_data'
output_csv = '../output_merged/financial_data.csv'

merge_and_filter_csv_files(input_directory, output_csv)
