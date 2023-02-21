import openpyxl
import csv

# input excel file path

def convert():
    inputExcelFile = 'ZI Data Sample.xlsx'

    # creating or loading an excel workbook
    newWorkbook = openpyxl.load_workbook(inputExcelFile)

    # getting the active workbook sheet(Bydefault-->Sheet1)
    firstWorksheet = newWorkbook.active

    # Opening a output csv file in write mode
    OutputCsvFile = csv.writer(open("data/ZI Data Sample.csv", 'w'), delimiter=",")

    # Traversing in each row of the worshsheet
    for eachrow in firstWorksheet.rows:

        # Writing data of the excel file into the result csv file row-by-row
        OutputCsvFile.writerow([cell.value for cell in eachrow])

    print("Converted")
        
