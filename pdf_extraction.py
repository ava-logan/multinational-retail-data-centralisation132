
#takes in link as an argument and returns a pandas DataFrame
import pandas as pd
import tabula
from database_utils import DataCleaning

class pdf_cleaning:
    def retrieve_and_clean():
        pdf = pd.DataFrame(tabula.read_pdf('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', pages='all', multiple_tables=False)[0])
        valid_card_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit', 'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover', 'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
        pdf = pdf[pdf['card_provider'].isin(valid_card_providers)]
        return pdf
    
    def upload_pdf():
        data = pdf_cleaning.retrieve_and_clean()
        DataCleaning.upload_pdf_to_local(data, 'dim_card_details')


pdf_cleaning.upload_pdf()
