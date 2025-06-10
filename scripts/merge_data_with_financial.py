import pandas as pd

# Load the first CSV file
first_csv_path = '../output_merged/data.csv'
df1 = pd.read_csv(first_csv_path, header=None, delimiter='^', names=[
    'CUI', 'ActAutorizare', 'CodPostal', 'Telefon', 'Fax', 'StareSocietate',
    'DataUltimeiDeclaratii', 'DataUltimeiPrelucrari', 'ImpozitProfit', 'ImpozitMicroint',
    'Accize', 'Tva', 'ContributiiAsigSoc', 'ContributiaAsigMunca', 'ContributiiAsigSan',
    'TaxaJocuriNoroc', 'ImpozitSalarii', 'ImpozitConstructii', 'ImpozitTiteiGaz',
    'RedeventeMiniere', 'RedeventePetroliere'])

# Load the second CSV file
second_csv_path = '../output_merged/financial_data.csv'
df2 = pd.read_csv(second_csv_path, delimiter='^', header=None, names=[
    'CUI', 'An', 'ActiveImobilizate', 'ActiveCirculante', 'Stocuri', 'Creante',
    'CasaSiConturiLaBanci', 'CheltuieliInAvans', 'Datorii', 'VenituriInAvans',
    'Provizioane', 'CapitaluriTotal', 'CapitalSubscrisVarsat', 'PatrimoniulRegiei',
    'CifraDeAfaceriNeta', 'VenituriTotale', 'CheltuieliTotale', 'ProfitBrut','PierdereBrut',
    'ProfitNet', 'PierdereNet', 'NumarMediuDeSalariati', 'TipulDeActivitate'])
mapping_csv_path = '../input/caen_map.csv'
mapping_df = pd.read_csv(mapping_csv_path, delimiter='^', header=None, names=['CAEN', 'Description'])

# Create a dictionary for mapping descriptions to codes
mapping_dict = dict(zip(mapping_df['Description'], mapping_df['CAEN']))
# Convert TipulDeActivitate descriptions to CAEN codes
df2['TipulDeActivitate'] = df2['TipulDeActivitate'].map(mapping_dict)

# Reshape the second DataFrame to have one row per CUI, with columns for each year
df2_wide = df2.pivot_table(index='CUI', columns='An', values=[
    'ActiveImobilizate', 'ActiveCirculante', 'Stocuri', 'Creante',
    'CasaSiConturiLaBanci', 'CheltuieliInAvans', 'Datorii', 'VenituriInAvans',
    'Provizioane', 'CapitaluriTotal', 'CapitalSubscrisVarsat', 'PatrimoniulRegiei',
    'CifraDeAfaceriNeta', 'VenituriTotale', 'CheltuieliTotale', 'ProfitBrut', 'PierdereBrut',
    'ProfitNet', 'PierdereNet', 'NumarMediuDeSalariati', 'TipulDeActivitate'], aggfunc='first')
df2_wide.columns = [f'{col[0]}_{col[1]}' for col in df2_wide.columns]
df2_wide.reset_index(inplace=True)

# Merge the two DataFrames on CUI
merged_df = pd.merge(df1, df2_wide, on='CUI', how='left')

# Save the merged DataFrame to a new CSV file
output_csv_path = '../output_merged/merged_data_financial.csv'
merged_df.to_csv(output_csv_path, index=False, sep='^')

print(f'Results saved to {output_csv_path}')
