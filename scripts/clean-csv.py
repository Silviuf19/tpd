import time

input_csv_file = '../input/firme_neradiate_cu_sediu.csv'
cleaned_csv_file = '../output_merged/firme_neradiate_cu_sediu_cleaned.csv'

start = time.time()
text = ''
with open(input_csv_file, 'r', encoding='utf-8', errors='replace') as f:
    text = f.read()
print(f"Time taken to read the CSV file: {time.time() - start:.2f} seconds.")
start = time.time()

with open(cleaned_csv_file, 'w') as f:
    f.write(text)
print(f"Time taken to write the CSV file: {time.time() - start:.2f} seconds.")