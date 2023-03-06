import openpyxl
import csv
import os
import pandas as pd
# input excel file path

def convert():
    with os.scandir('excel_data/') as files:
        print(files)
        for file in files:
            inputExcelFile = file.name
            print(inputExcelFile)
            df = pd.read_excel("excel_data/"+inputExcelFile)
           
            print("loaded")
            def write_string(s):
                special = '"!#$%\'()*+,-./:;<=>?@[\]^_`{|}~±™⁀–©≈°Ø•'
                return ''.join(i for i in s if i not in special)
            
            df['Company_Name'] = df['Company_Name'].str.replace('"',"")
            df['Company_Name'] = df['Company_Name'].str.replace(',',"")
            df['Headquarters'] = df['Headquarters'].str.replace('"',"")
            df["Description"] = df["Description"].str.replace('"', "")
            df["Industry"] = df["Industry"].str.replace('"', "")

            filename = inputExcelFile.split('.')[0]
            df.to_csv(f"data/{filename}.csv", index=False)

    print("Converted")

convert()      
