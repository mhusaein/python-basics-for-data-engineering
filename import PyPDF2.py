from datetime import datetime
from sre_constants import RANGE
import pandas as pd
import glob
from PyPDF2 import PdfReader as pr

# Extraction steps
# Make a list from all the pdf files in a subfolder
list_pdf = glob.glob(pathname='C:\Hussein\Ops Data\Titan\pdfs\*.pdf')
print(len(list_pdf))

# read pdf and put output into a list
def read_from_pdf(pdf_to_convert):
    reader = pr(pdf_to_convert)
    page = reader.pages[0]
    page_text = page.extract_text()

    # Make a list for every line from the page_text
    text_list = list(page_text.split("\n"))

    # locate machine id position in the list
    eq_loc = text_list.index('MACHINE ID:')+1

    # locate estimated volume position in the list
    if 'ESTIMATED VOLUME'  in text_list: 
        vol_loc = text_list.index('ESTIMATED VOLUME')+1
    else: vol_loc = 0 

    # locate Tonnes moved position in the list
    if 'TONNES MOVED'  in text_list: 
        tonnes_loc = text_list.index('TONNES MOVED')+1
    else: tonnes_loc = 0 

    # Put all the found values into a list
    final_list = [text_list[eq_loc],text_list[1],text_list[vol_loc].split(' ')[0],text_list[tonnes_loc]]
    for i in range(len(final_list)): 
        if 'SHIFT' in final_list[i] :
            final_list[i] = 0
    return final_list

def extract():
    # Define a dataframe with the same number of columns as the final list to populate the values from it
    extracted_data = pd.DataFrame(columns=['equipment','shift','volume','tonnes'])
    
    # Create a loop to extract a list with the required values and put it in a seperate row in the dataframe
    for pdffile in list_pdf:
        extracted_data.loc[len(extracted_data)] = read_from_pdf(pdffile)
    return extracted_data

# Load data
def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile, index=False) 
targetfile ="C:\Hussein\Ops Data\Titan\Titan_data.csv"    

# Logging
def log(message):
    timestamp_format = '%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("C:\Hussein\Ops Data\Titan\logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

log("ETL Job Started")
log("Extract phase started")
extracted_data = extract()
log("Extract phase ended")
log("Load phase started")
load(targetfile,extracted_data)
log("Load phase ended")
log("ETL phase ended")