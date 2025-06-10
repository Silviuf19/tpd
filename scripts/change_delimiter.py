import pandas as pd
import sys
args = sys.argv

# Check if the file path is provided as an argument
if len(args) < 2:
    print("Please provide the file path as an argument.")
    sys.exit(1)

# Get the file path from the arguments
file_path = args[1]

# Load the CSV file
df = pd.read_csv(file_path)

# Save the file with a caret (^) delimiter
df.to_csv(file_path, sep='^', index=False, header=False)
