## Introduction ##

This “Multinational Retail Data Centralisation” Project is designed to practice querying large sets of data stores in RDMSMs. This database was created and stored locally using pgadmin4 (VERSION).  Before the data could be used, it had to first be extracted from multiple sources (AWS RDS database, PDF website, API endpoint, s3 bucket). This required using newly taught methods and our initiative (Google), which together tested our practical understanding and research ability.  Once each set of data was ready and clean, they could all be uploaded to the local database using a SQLAlchemy engine. After finishing touches were given, the Database was queried using pgAdmin 4 (management tool for PostgreSQL). 

## 1. Topics ##

To carry out the project, the user will require a resonable understanding of the following topics:
- Reading and converting files types (JSON, YAML, CSV)
- Using Pandas DataFrames
- API requests and the GET method
- Amazon s3 Buckets
- SQL including querying the data and SQLAlchemy engines

## 2. Environment ##

To successfully use all the programmes required, these must first be installed:
- AWS Command Line Interface (AWS CLI)
- boto3
- JPype1
- pandas
- PyYAML
- requests
- SQLAlchemy
- tabula-py
Except for AWS CLI, all can be installed using pip <name> as given in this list. To install AWS CLI see documentation at https://docs.aws.amazon.com/cli/

## 3. Cleaning using Pandas ## 

The first steps of cleaning the data was to remove any erroneous values. Using .dropna() would remove rows with any NULL values. This would be useful in a questionnaire where for example any null values might skew the total results. However, this had to be used with caution as the online WEB store had NULL values for longitude, latitude, and locality which are correct values. 

- <pd_name>.info() gives the column names and types
- <pd_name>[‘<column_name>’].unique() to return a list of the unique values in the column

Unique values are useful for columns where the entries are definitive and fewer, such as 'Countries', as opposed to 'Store_code' which should have more possible inputs and hence less unique values. This technique also shows any mistyped data clearly which can be fixed by creating a dictionary of fixes and using the .replace() method.

Rows containing non-valid entries could be removed using a mask to create a copy of the DataFrame eliminating rows not containing defined data in particular columns. For example, if the only valid countries are 'Germany', 'US' and 'UK', and this query considered any row invlaid if one input is invalid, then the mask eliminate all entries not containing the listed countries. (using .notin())

- <pd_name>.drop(columns=['<column_name>'], inplace=True)

Irrelevant or empty columns can be removed using .drop(). As this doesnt strongly interfer with querying the data, this step is not necessary except for cleaniless and readability 

## 4. Cleaning in SQL ##

### Miscellaneous values ###

Occasional values arose that did not fit the predicted format, despite being valid entries. To catch these values, the programme employed an 'else' block in the Python code to keep the values but flag them for later processing. 

<img width="937" alt="changing weights" src="https://github.com/ava-logan/multinational-retail-data-centralisation132/assets/148722602/f7c4ef3f-f220-425f-b550-dd95d39338e6">

After running the first code, it was clear three values were incorrect and could be removed, while the final one could be manually fixed with a quick google conversion. 

### Data Type ###

Data types could be assigned using 'SET' and 'USING' to convert to the specified type. If any values did not fit the new data type they would be flagged. In the case of 'staff_numbers' some of these had letters whilst being converted to numeric. During the first attempt these had been removed because it was unclear if it was a typo or an invalid input. After errors with the primary and foreign key arose, it was evident that these values were correct and the alphabetic characters needed to be removed. This was done manually using the same method as the miscellaneous values. 

## 5. Querying the data ##

### Task 4: Case ### 
Used 'CASE' to regroup the data into 'Online' and 'Not Online'/offline

<img width="556" alt="howmanysalesoffline" src="https://github.com/ava-logan/multinational-retail-data-centralisation132/assets/148722602/fb312a25-2c23-405d-8172-dff2bd6fc504">

### Task 6: Double join ###
Used two joins to use data about the product and the datetimes, linked by the orders table.

<img width="655" alt="actual task 6" src="https://github.com/ava-logan/multinational-retail-data-centralisation132/assets/148722602/7e57baaf-a9ec-4cba-8008-6521d68fd9aa">


