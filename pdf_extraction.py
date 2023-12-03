
#takes in link as an argument and returns a pandas DataFrame
import pandas as pd
import tabula

pdf_df = pd.DataFrame(tabula.read_pdf('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', pages='all', multiple_tables=False)[0])
print(pdf_df.nunique())