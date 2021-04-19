from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import math
import calendar
import warnings
warnings.filterwarnings('ignore')
import datetime as dt

#Get Entered Data
url = "https://www.commcarehq.org/enter export link"
r = requests.get(url, auth=('username', 'password'))
soup = BeautifulSoup(r.content)
n_columns = 0
n_rows=0
column_names = []

# Find number of rows and columns
# we also find the column titles if we can
for row in soup.find_all('tr'):

    # Determine the number of rows in the table
    td_tags = row.find_all('td')
    if len(td_tags) > 0:
        n_rows+=1
        if n_columns == 0:
            # Set the number of columns for our table
            n_columns = len(td_tags)

    # Handle column names if we find them
    th_tags = row.find_all('th') 
    if len(th_tags) > 0 and len(column_names) == 0:
        for th in th_tags:
            column_names.append(th.get_text())

# Safeguard on Column Titles
if len(column_names) > 0 and len(column_names) != n_columns:
    raise Exception("Column titles do not match the number of columns")

columns = column_names if len(column_names) > 0 else range(0,n_columns)
entered_data = pd.DataFrame(columns = columns,index= range(0,n_rows))

row_marker = 0
for row in soup.find_all('tr'):
    column_marker = 0
    columns = row.find_all('td')
    for column in columns:
        entered_data.iat[row_marker,column_marker] = column.get_text()
        column_marker += 1
    if len(columns) > 0:
        row_marker += 1
        
#Get Edited Data
url = "https://www.commcarehq.org/enter export link"
r = requests.get(url, auth=('username', 'password'))
soup = BeautifulSoup(r.content)
n_columns = 0
n_rows=0
column_names = []

# Find number of rows and columns
# we also find the column titles if we can
for row in soup.find_all('tr'):

    # Determine the number of rows in the table
    td_tags = row.find_all('td')
    if len(td_tags) > 0:
        n_rows+=1
        if n_columns == 0:
            # Set the number of columns for our table
            n_columns = len(td_tags)

    # Handle column names if we find them
    th_tags = row.find_all('th') 
    if len(th_tags) > 0 and len(column_names) == 0:
        for th in th_tags:
            column_names.append(th.get_text())

# Safeguard on Column Titles
if len(column_names) > 0 and len(column_names) != n_columns:
    raise Exception("Column titles do not match the number of columns")

columns = column_names if len(column_names) > 0 else range(0,n_columns)
edited_data = pd.DataFrame(columns = columns,index= range(0,n_rows))

row_marker = 0
for row in soup.find_all('tr'):
    column_marker = 0
    columns = row.find_all('td')
    for column in columns:
        edited_data.iat[row_marker,column_marker] = column.get_text()
        column_marker += 1
    if len(columns) > 0:
        row_marker += 1

#check for result case ids that have been edited
edited_caseids = set(entered_data['Results caseid']).intersection(set(edited_data['Results caseid']))

#extract edited entries from the entered data and delete them from the entered data in prep for new data
edited_entries = entered_data[entered_data['Results caseid'].isin(edited_caseids)]

#prep new data by deleting edited entries from the entered data.
new_data = entered_data[~entered_data['Results caseid'].isin(edited_caseids)]

#Add indicator caseid to each of the rows of the edited data
edited_data = pd.merge(entered_data[['Indicators caseid','Results caseid']], edited_data, on=['Results caseid'])

#for edited entries, compare and keep the latest edit by adding it to the new dataset
for result_caseid in edited_caseids:
    edits = edited_entries[edited_entries['Results caseid'] == result_caseid]
    latest_data = edits[edits['Last update'] == edits['Last update'].max()]
    
    #append row to new_data
    new_data = pd.concat([new_data, latest_data])
    
#define columns in final output file

output_data = pd.DataFrame(columns = ['Disaggregation type', 'Disaggregation option', 'Achieved numerator',
       'Achieved denominator', 'Achieved result', 'Indicators caseid',
       'Result caseid', 'GMS Code', 'Indicator code', 'Reporting period',
       'Result date', 'Last update'])

def clean(data):
    data = data.split(' ')
    
    return data[0]
columns = ['Indicators caseid','Results caseid','GMS Code','Indicator code','Reporting Period (year and month)','Result date','Disaggregation type','Disaggregation option','Achieved numerator','Achieved denominator','Achieved result','Last update']

for result_caseid in list(set(new_data['Results caseid'])):
    
    data = new_data[new_data['Results caseid'] == result_caseid]
    
    ## Sort out disaggregations 
    keep = []
    for disaggregation_number in range(1,7):
        col = []
        disaggregation_type = data['Disaggregation type' + str(disaggregation_number)].item()

        for disaggregation_option in range(1,11):
            dis = []
            dis.append(disaggregation_type)
            disaggregation_opt = data['Type' + str(disaggregation_number) + '- Disaggregation option' + str(disaggregation_option)].item()
            numerator = data['type' + str(disaggregation_number) + '-level' + str(disaggregation_option) + ' Achieved numerator'].item()
            denominator = data['type_' + str(disaggregation_number) +'.level_' + str(disaggregation_option) + '_Achieved denominator'].item()
            result = data['type' + str(disaggregation_number) + '_level_' + str(disaggregation_option) + ' Achieved result'].item()
            dis.append(disaggregation_opt)
            dis.append(numerator)
            dis.append(denominator)
            dis.append(result)
            col.append(dis)

        keep.append(col)
    
    
    #sort out the other values in the dataset
    collection = []
    collection.append(data['Indicators caseid'].item())
    collection.append(data['Results caseid'].item())
    collection.append(data['GMS Code'].item())
    collection.append(data['Indicator code'].item())
    
    
    #get result date from reporting period
    data['Reporting period (year and month)']= pd.to_datetime(data['Reporting period (year and month)'])
    reporting_date = list(data['Reporting period (year and month)'])[0]
       
    collection.append(str(reporting_date).split(' ')[0])
    
    result_date = calendar.monthrange(reporting_date.year, reporting_date.month)[1]
    result_date = str(result_date) + '/' + str(reporting_date.month) + '/' + str(reporting_date.year)
    
    collection.append(result_date)
    
    update = str(list(data['Last update'])[0]).split(' ')
    
    collection.append(update[0] + ' '+ update[1])
    
    
    ##Put the new columns to the aggregations
    x = pd.DataFrame(np.array(keep).reshape(60,5), columns = ['Disaggregation type', 'Disaggregation option','Achieved numerator','Achieved denominator','Achieved result'])
    
    x['Indicators caseid'] = collection[0]
    x['Result caseid'] = collection[1]
    x['GMS Code'] = collection[2]
    x['Indicator code'] = collection[3]
    x['Reporting period']  = collection[4]
    x['Result date'] = collection[5]
    x['Last update'] = collection[6]
    
    
    def fix_gms(x, y):
        if str(y) == 'nan':
            return x.split('-')[1]
        if str(y) == '':
            return x.split('-')[1]
        else :
            return y
    
    ##Disaggregation type
    #drop rows with missing values
    x = x.dropna(subset=['Disaggregation type'])

    #drop rows with ---
    x = x[x['Disaggregation type'] != '---']

    #drop rows with total
    x = x[x['Disaggregation type'] != 'total']
    
    #drop rows with missing values
    x = x[x['Disaggregation type'] != '']


    ##Disaggregation option
    #drop rows with missing values
    x.dropna(subset=['Disaggregation option'])
    
    #drop rows with missing values
    x = x[x['Disaggregation option'] != '']


    ##Achieved result
    #replace missing numbers with 0
    x['Achieved result'] = x['Achieved result'].fillna(0)
    x['Achieved result'] = x['Achieved result'].replace([''],0)


    ##GMS code
    x['GMS Code'] = x[['Indicator code','GMS Code']].apply(lambda x: fix_gms(*x), axis=1)
    
    #Export to Excel file
    output_data = pd.concat([output_data,x])
    output_data.to_excel('Output.xlsx', index = False)