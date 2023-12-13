
#takes in link as an argument and returns a pandas DataFrame
import pandas as pd
import tabula

pdf = pd.DataFrame(tabula.read_pdf('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', pages='all', multiple_tables=False)[0])
print(pdf.nunique())