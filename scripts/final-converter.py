import datetime
import re
import pandas as pd
import time
import ujson as json

cleaned_csv_file = '../output_merged/firme_neradiate_cu_sediu_cleaned.csv'
financials_file = '../output_merged/merged_data_financial.csv'
final_output_json_file = '../output_merged/finalJson.json'

all_columns_csv1 = [
    'DENUMIRE', 'CUI', 'COD_INMATRICULARE', 'EUID', 'STARE_FIRMA', 'ADRESA_COMPLETA',
    'ADR_TARA', 'ADR_LOCALITATE', 'ADR_JUDET', 'ADR_DEN_STRADA', 'ADR_DEN_NR_STRADA',
    'ADR_BLOC', 'ADR_SCARA', 'ADR_ETAJ', 'ADR_APARTAMENT', 'ADR_COD_POSTAL',
    'ADR_SECTOR', 'ADR_COMPLETARE', 'BROKEN_DATA'
]

interest_columns_csv1 = [
    'DENUMIRE', 'CUI', 'COD_INMATRICULARE', 'EUID', 'STARE_FIRMA',
    'ADR_TARA', 'ADR_LOCALITATE', 'ADR_JUDET', 'ADR_DEN_STRADA', 'ADR_DEN_NR_STRADA',
    'ADR_BLOC', 'ADR_SCARA', 'ADR_ETAJ', 'ADR_APARTAMENT', 'ADR_COD_POSTAL',
    'ADR_SECTOR', 'ADR_COMPLETARE'
]

columns_csv2 = [
    'CUI', 'ActAutorizare', 'CodPostal', 'Telefon2', 'Fax', 'StareSocietate',
    'DataUltimeiDeclaratii', 'DataUltimeiPrelucrari', 'ImpozitProfit',
    'ImpozitMicroint', 'Accize', 'Tva', 'ContributiiAsigSoc',
    'ContributiaAsigMunca', 'ContributiiAsigSan', 'TaxaJocuriNoroc',
    'ImpozitSalarii', 'ImpozitConstructii', 'ImpozitTiteiGaz',
    'RedeventeMiniere', 'RedeventePetroliere',
    'ActiveCirculante_2018', 'ActiveCirculante_2019', 'ActiveCirculante_2020', 'ActiveCirculante_2021', 'ActiveCirculante_2022', 'ActiveCirculante_2023',
    'ActiveImobilizate_2018', 'ActiveImobilizate_2019', 'ActiveImobilizate_2020', 'ActiveImobilizate_2021', 'ActiveImobilizate_2022', 'ActiveImobilizate_2023',
    'CapitalSubscrisVarsat_2018', 'CapitalSubscrisVarsat_2019', 'CapitalSubscrisVarsat_2020', 'CapitalSubscrisVarsat_2021', 'CapitalSubscrisVarsat_2022', 'CapitalSubscrisVarsat_2023',
    'CapitaluriTotal_2018', 'CapitaluriTotal_2019', 'CapitaluriTotal_2020', 'CapitaluriTotal_2021', 'CapitaluriTotal_2022', 'CapitaluriTotal_2023',
    'CasaSiConturiLaBanci_2018', 'CasaSiConturiLaBanci_2019', 'CasaSiConturiLaBanci_2020', 'CasaSiConturiLaBanci_2021', 'CasaSiConturiLaBanci_2022', 'CasaSiConturiLaBanci_2023',
    'CheltuieliInAvans_2018', 'CheltuieliInAvans_2019', 'CheltuieliInAvans_2020', 'CheltuieliInAvans_2021', 'CheltuieliInAvans_2022', 'CheltuieliInAvans_2023',
    'CheltuieliTotale_2018', 'CheltuieliTotale_2019', 'CheltuieliTotale_2020', 'CheltuieliTotale_2021', 'CheltuieliTotale_2022', 'CheltuieliTotale_2023',
    'CifraDeAfaceriNeta_2018', 'CifraDeAfaceriNeta_2019', 'CifraDeAfaceriNeta_2020', 'CifraDeAfaceriNeta_2021', 'CifraDeAfaceriNeta_2022', 'CifraDeAfaceriNeta_2023',
    'Creante_2018', 'Creante_2019', 'Creante_2020', 'Creante_2021', 'Creante_2022', 'Creante_2023',
    'Datorii_2018', 'Datorii_2019', 'Datorii_2020', 'Datorii_2021', 'Datorii_2022', 'Datorii_2023',
    'NumarMediuDeSalariati_2018', 'NumarMediuDeSalariati_2019', 'NumarMediuDeSalariati_2020', 'NumarMediuDeSalariati_2021', 'NumarMediuDeSalariati_2022', 'NumarMediuDeSalariati_2023',
    'PatrimoniulRegiei_2018', 'PatrimoniulRegiei_2019', 'PatrimoniulRegiei_2020', 'PatrimoniulRegiei_2021', 'PatrimoniulRegiei_2022', 'PatrimoniulRegiei_2023',
    'PierdereBrut_2018', 'PierdereBrut_2019', 'PierdereBrut_2020', 'PierdereBrut_2021', 'PierdereBrut_2022', 'PierdereBrut_2023',
    'PierdereNet_2018', 'PierdereNet_2019', 'PierdereNet_2020', 'PierdereNet_2021', 'PierdereNet_2022', 'PierdereNet_2023',
    'ProfitBrut_2018', 'ProfitBrut_2019', 'ProfitBrut_2020', 'ProfitBrut_2021', 'ProfitBrut_2022', 'ProfitBrut_2023',
    'ProfitNet_2018', 'ProfitNet_2019', 'ProfitNet_2020', 'ProfitNet_2021', 'ProfitNet_2022', 'ProfitNet_2023',
    'Provizioane_2018', 'Provizioane_2019', 'Provizioane_2020', 'Provizioane_2021', 'Provizioane_2022', 'Provizioane_2023',
    'Stocuri_2018', 'Stocuri_2019', 'Stocuri_2020', 'Stocuri_2021', 'Stocuri_2022', 'Stocuri_2023',
    'TipulDeActivitate_2018', 'TipulDeActivitate_2019', 'TipulDeActivitate_2020', 'TipulDeActivitate_2021', 'TipulDeActivitate_2022', 'TipulDeActivitate_2023',
    'VenituriInAvans_2018', 'VenituriInAvans_2019', 'VenituriInAvans_2020', 'VenituriInAvans_2021', 'VenituriInAvans_2022', 'VenituriInAvans_2023',
    'VenituriTotale_2018', 'VenituriTotale_2019', 'VenituriTotale_2020', 'VenituriTotale_2021', 'VenituriTotale_2022', 'VenituriTotale_2023',
]

interest_columns_csv2 = [
    'CUI', 'ActAutorizare', 'Telefon2', 'Fax', 'StareSocietate',
    'DataUltimeiDeclaratii', 'DataUltimeiPrelucrari', 'ImpozitProfit',
    'ImpozitMicroint', 'Accize', 'Tva', 'ContributiiAsigSoc',
    'ContributiaAsigMunca', 'ContributiiAsigSan', 'TaxaJocuriNoroc',
    'ImpozitSalarii', 'ImpozitConstructii', 'ImpozitTiteiGaz',
    'RedeventeMiniere', 'RedeventePetroliere',
    'ActiveCirculante_2018', 'ActiveCirculante_2019', 'ActiveCirculante_2020', 'ActiveCirculante_2021', 'ActiveCirculante_2022', 'ActiveCirculante_2023',
    'ActiveImobilizate_2018', 'ActiveImobilizate_2019', 'ActiveImobilizate_2020', 'ActiveImobilizate_2021', 'ActiveImobilizate_2022', 'ActiveImobilizate_2023',
    'CapitalSubscrisVarsat_2018', 'CapitalSubscrisVarsat_2019', 'CapitalSubscrisVarsat_2020', 'CapitalSubscrisVarsat_2021', 'CapitalSubscrisVarsat_2022', 'CapitalSubscrisVarsat_2023',
    'CapitaluriTotal_2018', 'CapitaluriTotal_2019', 'CapitaluriTotal_2020', 'CapitaluriTotal_2021', 'CapitaluriTotal_2022', 'CapitaluriTotal_2023',
    'CasaSiConturiLaBanci_2018', 'CasaSiConturiLaBanci_2019', 'CasaSiConturiLaBanci_2020', 'CasaSiConturiLaBanci_2021', 'CasaSiConturiLaBanci_2022', 'CasaSiConturiLaBanci_2023',
    'CheltuieliInAvans_2018', 'CheltuieliInAvans_2019', 'CheltuieliInAvans_2020', 'CheltuieliInAvans_2021', 'CheltuieliInAvans_2022', 'CheltuieliInAvans_2023',
    'CheltuieliTotale_2018', 'CheltuieliTotale_2019', 'CheltuieliTotale_2020', 'CheltuieliTotale_2021', 'CheltuieliTotale_2022', 'CheltuieliTotale_2023',
    'CifraDeAfaceriNeta_2018', 'CifraDeAfaceriNeta_2019', 'CifraDeAfaceriNeta_2020', 'CifraDeAfaceriNeta_2021', 'CifraDeAfaceriNeta_2022', 'CifraDeAfaceriNeta_2023',
    'Creante_2018', 'Creante_2019', 'Creante_2020', 'Creante_2021', 'Creante_2022', 'Creante_2023',
    'Datorii_2018', 'Datorii_2019', 'Datorii_2020', 'Datorii_2021', 'Datorii_2022', 'Datorii_2023',
    'NumarMediuDeSalariati_2018', 'NumarMediuDeSalariati_2019', 'NumarMediuDeSalariati_2020', 'NumarMediuDeSalariati_2021', 'NumarMediuDeSalariati_2022', 'NumarMediuDeSalariati_2023',
    'PatrimoniulRegiei_2018', 'PatrimoniulRegiei_2019', 'PatrimoniulRegiei_2020', 'PatrimoniulRegiei_2021', 'PatrimoniulRegiei_2022', 'PatrimoniulRegiei_2023',
    'PierdereBrut_2018', 'PierdereBrut_2019', 'PierdereBrut_2020', 'PierdereBrut_2021', 'PierdereBrut_2022', 'PierdereBrut_2023',
    'PierdereNet_2018', 'PierdereNet_2019', 'PierdereNet_2020', 'PierdereNet_2021', 'PierdereNet_2022', 'PierdereNet_2023',
    'ProfitBrut_2018', 'ProfitBrut_2019', 'ProfitBrut_2020', 'ProfitBrut_2021', 'ProfitBrut_2022', 'ProfitBrut_2023',
    'ProfitNet_2018', 'ProfitNet_2019', 'ProfitNet_2020', 'ProfitNet_2021', 'ProfitNet_2022', 'ProfitNet_2023',
    'Provizioane_2018', 'Provizioane_2019', 'Provizioane_2020', 'Provizioane_2021', 'Provizioane_2022', 'Provizioane_2023',
    'Stocuri_2018', 'Stocuri_2019', 'Stocuri_2020', 'Stocuri_2021', 'Stocuri_2022', 'Stocuri_2023',
    'TipulDeActivitate_2018', 'TipulDeActivitate_2019', 'TipulDeActivitate_2020', 'TipulDeActivitate_2021', 'TipulDeActivitate_2022', 'TipulDeActivitate_2023',
    'VenituriInAvans_2018', 'VenituriInAvans_2019', 'VenituriInAvans_2020', 'VenituriInAvans_2021', 'VenituriInAvans_2022', 'VenituriInAvans_2023',
    'VenituriTotale_2018', 'VenituriTotale_2019', 'VenituriTotale_2020', 'VenituriTotale_2021', 'VenituriTotale_2022', 'VenituriTotale_2023',
]

column_mapping = {
    'CUI': 'cui',
    'DENUMIRE': 'nume',
    'COD_INMATRICULARE': 'nrReg',
    'EUID': 'euid',
    'STARE_FIRMA': 'codStare',
    'ActAutorizare': 'actAutorizare',
    'Telefon': 'tel',
    'Fax': 'fax',
    'StareSocietate': 'stare',
    'DataUltimeiDeclaratii': 'dataDecl',
    'DataUltimeiPrelucrari': 'dataPrel',
    'ImpozitProfit': 'impozitProfit',
    'ImpozitMicroint': 'impozitMicroint',
    'Accize': 'accize',
    'Tva': 'tva',
    'ContributiiAsigSoc': 'contributiiAsigSoc',
    'ContributiaAsigMunca': 'contributiiAsigMunca',
    'ContributiiAsigSan': 'contributiiAsigSan',
    'TaxaJocuriNoroc': 'taxaJocuriNoroc',
    'ImpozitSalarii': 'impozitSalarii',
    'ImpozitConstructii': 'impozitConstructii',
    'ImpozitTiteiGaz': 'impozitTiteiGaz',
    'RedeventeMiniere': 'redevMiniere',
    'RedeventePetroliere': 'redevPetroliere',

    'ADR_TARA': 'tara',
    'ADR_LOCALITATE': 'localitate',
    'ADR_JUDET': 'judet',
    'ADR_DEN_STRADA': 'strada',
    'ADR_DEN_NR_STRADA': 'nrStr',
    'ADR_BLOC': 'bloc',
    'ADR_SCARA': 'scara',
    'ADR_ETAJ': 'etaj',
    'ADR_APARTAMENT': 'apartament',
    'ADR_COD_POSTAL': 'codPostal',
    'ADR_SECTOR': 'sector',
    'ADR_COMPLETARE': 'comp',


    'ActiveCirculante': 'activeCirc',
    'ActiveImobilizate': 'activeImob',
    'CapitalSubscrisVarsat': 'capitalSubsVars',
    'CapitaluriTotal': 'capitalTotal',
    'CasaSiConturiLaBanci': 'casaConturi',
    'CheltuieliInAvans': 'cheltuieliAvans',
    'CheltuieliTotale': 'cheltuieliTot',
    'CifraDeAfaceriNeta': 'cifraAfaceri',
    'Creante': 'creante',
    'Datorii': 'datorii',
    'NumarMediuDeSalariati': 'salariati',
    'PatrimoniulRegiei': 'patrimRegie',
    'PierdereBrut': 'pierdereBrut',
    'PierdereNet': 'pierdereNet',
    'ProfitBrut': 'profitBrut',
    'ProfitNet': 'profitNet',
    'Provizioane': 'provizioane',
    'Stocuri': 'stocuri',
    'TipulDeActivitate': 'caen',
    'VenituriInAvans': 'venituriAvans',
    'VenituriTotale': 'venituriTot',
}


def extract_and_format_data(text):
    # Define regular expression patterns for status and date
    status_pattern = r"(INREGISTRAT|TRANSFER\(SOSIRE\)|SUSPENDARE ACTIVITATE|RELUARE ACTIVITATE|DIZOLVARE FARA LICHIDARE\(FUZIUNE\)|DIZOLVARE FARA LICHIDARE\(DIVIZARE\)|DIZOLVARE CU LICHIDARE\(INCEPUT LICHIDARE\)|LICHIDAT|)"
    date_pattern = r"din data (\d+) ([a-zA-Z]+) (\d+)"

    # Search for status and date
    status_match = re.search(status_pattern, text)
    date_match = re.search(date_pattern, text)

    # Check if both status and date are found
    if status_match and date_match:
        status = status_match.group(1)
        day = int(date_match.group(1))
        month_name = date_match.group(2)

        # Convert month name to number (January = 1, December = 12)
        months = {"Ianuarie": 1, "Februarie": 2, "Martie": 3, "Aprilie": 4, "Mai": 5, "Iunie": 6,
                  "Iulie": 7, "August": 8, "Septembrie": 9, "Octombrie": 10, "Noiembrie": 11, "Decembrie": 12}
        month = months[month_name]
        year = int(date_match.group(3))

        # Create datetime object
        extracted_date = datetime.date(year, month, day)

        # Format data as JSON
        data = {
            "status": status,
            "data": extracted_date.isoformat()
        }
        return data
    else:
        return text


def getCompanyType(name: str):
    if 'SRL' in name or 'S.R.L' in name or 'S.R.L.' in name or 'SOCIETATE CU RĂSPUNDERE LIMITATĂ' in name or 'SOCIETATE CU RASPUNDERE LIMITATĂ' in name:
        return 'SRL'
    elif 'SRL-D' in name or 'S.R.L-D' in name or 'S.R.L.-D' in name or 'S.R.L.-D.' in name:
        return 'SRL-D'
    elif 'SA' in name or 'S.A.' in name or 'S.A' in name or 'SOCIETATE PE ACŢIUNI' in name:
        return 'SA'
    elif 'PERSOANĂ FIZICĂ AUTORIZATĂ' in name or 'PFA' in name or 'P.F.A.' in name or 'P.F.A' in name or 'PERSOANA FIZICA AUTORIZATA' in name or 'FIZICĂ AUTORIZATĂ' in name or 'FIZICĂ AUTORIZATA' in name or 'PERSOANĂ FIZICĂ' in name or 'PERSOANÃ FIZICÃ AUTORIZATÃ' in name:
        return 'PFA'
    elif 'ÎNTREPRINDERE FAMILIALĂ' in name or 'Î.F.' in name or 'I.F.' in name or 'I.F' in name or name.endswith('IF') or 'ÎNTREPRINDERE FAMILIALA' in name or 'iNTREPRINDERE FAMILIAlĂ' in name or 'INTREPRINDERE FAMILIALĂ' in name or 'INTREPRINDERE FAMILIALA' in name or 'INTEPRINDERE FAMILIALA' in name:
        return 'IF'
    elif 'INTREPRINDERE INDIVIDUALA' in name or 'ÎNREPRINDERE INDIVIDUALĂ' in name or 'ÎNTREPRIDERE INDIVIDUALĂ' in name or 'ÎNTREPRINDERE INDIVIDUALĂ' in name or 'Î.I.' in name or 'I.I.' in name or 'I.I' in name or 'ÎI' in name or 'Î.I' in name or 'INTREPRINDERE INDIVIDUALĂ' in name or ' ÎNTREPRINDRE INDIVIDUALĂ' in name or name.endswith(' II') or 'ÎNTREPRINERE INDIVIDUALĂ' in name or 'ÎNTREPRINDERE INDIVUDUALĂ' in name or 'INTREPRINDEREA INDIVIDUALĂ' in name or 'ÎNTREPRINDERE INDIVIDUALA' in name or 'INTEPRINDERE INDIVIDUALA' in name:
        return 'II'
    elif 'SOCIETATE COOPERATIVĂ' in name or 'SOCIETATE COOPERATIVA' in name or 'SOCIETATEA COOPERATIVA' in name or 'SOCIETATEA COOPERATIVĂ' in name or 'COOPERATIVA' in name or 'COOPERATISTA' in name or 'COOPERATISTĂ' in name or 'COOPERATIVĂ' in name:
        return 'CA'
    elif 'SUCURSALĂ' in name or 'SUCURSALA' in name or 'sucursala' in name:
        return 'SUC'
    elif 'SNC' in name or 'SOCIETATE ÎN NUME COLECTIV' in name or 'SOCIETATE IN NUME COLECTIV' in name:
        return 'SNC'
    elif 'SCS' in name or 'SOCIETATE ÎN COMANDITĂ SIMPLĂ' in name or 'SOCIETATE IN COMANDITA SIMPLA' in name or 'SOCIETATE IN COMANDITĂ SIMPLĂ' in name or 'SOCIETATE ÎN COMANDITA SIMPLA' in name or 'SOCIETATE ÎN COMANDITĂ' in name:
        return 'SCS'
    elif 'SCM' in name or 'SOCIETATE ÎN COMANDITĂ PE ACŢIUNI' in name:
        return 'SCM'
    elif ' RA ' in name or 'REGIA AUTONOMĂ' in name or 'REGIE AUTONOMĂ' in name or name.endswith(' RA') or name.startswith('RA ') or 'R.A.' in name:
        return 'RA'
    elif 'INSTITUTUL NATIONAL DE CERCETARE' in name or 'INSTITUTUL NAȚIONAL DE CERCETARE' in name:
        return 'INC'
    else:
        return None


def processRow(row):
    bilant = {}
    adresa = {}
    firma_json = {}
    firma_json['cui'] = row['CUI']

    for key, value in row.items():
        if pd.notna(value):  # Only include non-null values
            if key.endswith(('_2018', '_2019', '_2020', '_2021', '_2022', '_2023')):
                year = key[-4:]
                new_key = key[:-5]
                new_key = column_mapping.get(new_key, new_key)
                if year not in bilant:
                    bilant[year] = {}
                bilant[year][new_key] = value
            elif key.startswith('ADR_'):
                new_key = column_mapping.get(key, key)
                if new_key not in adresa:
                    adresa[new_key] = value
            elif key == 'DENUMIRE':
                firma_json['nume'] = value
                type = getCompanyType(value)
                if type is not None:
                    firma_json['tip'] = type
            elif key.startswith('Telefon'):
                if 'tel' not in firma_json:
                    firma_json['tel'] = value
            elif key == 'STARE_FIRMA':
                if 'codStare' not in firma_json:
                    firma_json['codStare'] = []
                firma_json['codStare'] = value.split(',')
            elif key == 'StareSocietate':
                data = extract_and_format_data(value)
                if isinstance(data, str):
                    firma_json['stare'] = value
                    print(value)
                else:
                    firma_json['stare'] = data
            elif key == 'COD_INMATRICULARE':
                year = value[-4:]
                firma_json['anInfiintare'] = year
                firma_json['nrReg'] = value
            else:
                new_key = column_mapping.get(key, key)
                firma_json[new_key] = value
            
    if bilant:
        firma_json['bilant'] = bilant
    if adresa:
        firma_json['adresa'] = adresa
    return firma_json


start = time.time()

firme_full_df = pd.read_csv(cleaned_csv_file, delimiter='^', names=all_columns_csv1,
                            skiprows=1, low_memory=False, on_bad_lines='warn', encoding='utf-8')
print(
    f"Red the CSV1 file into a DataFrame: {time.time() - start:.2f} seconds.")
start = time.time()

filtered_firme_full_df = firme_full_df.drop_duplicates()
print('Removed duplicates from the first DataFrame', time.time() - start)
start = time.time()

filtered_firme_full_df = firme_full_df[interest_columns_csv1]
print(
    f"Filtered the columns of interest CSV1: {time.time() - start:.2f} seconds.")
start = time.time()

date_dirme_df = pd.read_csv(financials_file, delimiter='^', names=columns_csv2, skiprows=1,
                            low_memory=False, on_bad_lines='warn', encoding='utf-8')
print(
    f"Red the CSV2 file into a DataFrame: {time.time() - start:.2f} seconds.")
start = time.time()

# Filter the columns of interest
filtered_date_dirme_df = date_dirme_df[interest_columns_csv2]
print(
    f"Filtered the columns of interest CSV2: {time.time() - start:.2f} seconds.")
start = time.time()
filtered_cui_set = set(filtered_date_dirme_df['CUI'].values)

# Merge the two DataFrames
firme_merged = filtered_firme_full_df.merge(
    filtered_date_dirme_df, how='left', on='CUI')
print(f"Merged the two DataFrames: {time.time() - start:.2f} seconds.")
start = time.time()

formatted_json_content = []

for _, row in firme_merged.iterrows():
    processed_row = processRow(row)
    if processed_row:
        formatted_json_content.append(processed_row)

print(
    f"DataFrame converted to formatted JSON in: {time.time() - start:.2f} seconds.")
start = time.time()


with open(final_output_json_file, 'w', encoding='utf-8') as f:
    f.write(json.dumps(formatted_json_content, ensure_ascii=False, indent=0, sort_keys=True).replace(
        '\\/', '/').replace('"sector":0.0,', '').replace(',"sector":0.0}', '}'))


print(f"Final JSON file saved in: {time.time() - start:.2f} seconds.")
