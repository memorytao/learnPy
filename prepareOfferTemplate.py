import pandas as pd
import os
import csv

# Read the Excel file
excel_file = pd.read_excel(os.getcwd()+'/PP_Offer_202306161609.xlsx' )

# Define the desired delimiter
delimiter = '|'  # Change this to your desired delimiter

# Convert Excel data to CSV format without newline characters
csv_data = excel_file.to_csv(index=False, sep=delimiter)


csv_data = csv_data.split('\n')



# Save the CSV data without the header to a file
with open('offer_load.csv', 'w', encoding='utf-8') as file:
    file.write(''.join(csv_data[1:]))
