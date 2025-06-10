import os
import json

def merge_json_files(directory):
    merged_data = []

    # Iterate through all files in the specified directory
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                if isinstance(data, list):
                    # Filter out elements where AvailableYears is null
                    filtered_data = [item for item in data if item.get('AvailableYears') is not None]
                    merged_data.extend(filtered_data)
                else:
                    print(f"Warning: {file_path} does not contain a JSON array")

    # Specify the output file path
    output_file = os.path.join(directory, 'merged_output_full.json')
    # Filter out duplicates from merged_data
    merged_data = list({json.dumps(item): item for item in merged_data}.values())
    print(f"Found {len(merged_data)} unique items")
    with open(output_file, 'w') as outfile:
        json.dump(merged_data, outfile)
        

    print(f"Successfully merged files. The output is saved to {output_file}")

directory = '../stable_data/'  
merge_json_files(directory)
