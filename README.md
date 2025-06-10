0. Get a list of CUIs and put them in the input folder
1. Run data_extractor.go, it will populate the output/data and ouput/vars
2. Run financial_data_extractor.go, it will use a vars file and will populate output/financial_data
3. Run the merge_csvs.py for merging the financial and data
4. Run final-converter.go to merge all data in output/finalJson