#-------------------------------------------------------------------------------
# Name:        LaborZendeskRefreshTicketForms
# Purpose:     Perform refresh of Zendesk Ticket Forms from API to data warehouse
# Author:      Chad Rue
# Created:     01/30/2021
#-------------------------------------------------------------------------------

#import built-in and custom packages
import requests
import csv
import json
import pandas as pd
import urllib
import pyodbc
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
from dateutil import tz
import dateutil.parser


def RefreshZenDeskTicketForms():

    #Parameters for Zendesk API connection need to be removed from script and placed into an ini file
    url = 'https://lwdsupport.tn.gov/api/v2/ticket_forms.json'
    user = 'Joe.W.Denton@tn.gov' + '/token'
    pwd = 'zjRDNwnE8odwtsOq0tZevpxgJGMVacYFvyIQzjMP'

    #Perform HTTP request/API call by passing credentials to access data
    response = requests.get(url, auth=(user, pwd))

    #Check for HTTP codes other than 200 and exit program if true
    if response.status_code != 200:
            print('Status:', response.status_code, 'Problem with the request. Exiting.')
            exit()

    #Create database connection.
    conn = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=CG01NDCWB00007;DATABASE=CG_UI_DataWH;Trusted_Connection=yes;")
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn))

    #Delete data from staging table before loading API data into staging table
    engine.execute(text('''EXEC dbo.usp_DeleteZDTicketFormsTable''').execution_options(autocommit=True))

    #Delete data from staging table before loading API data into staging table
    engine.execute(text('''EXEC dbo.usp_DeleteZDTicketFormsFieldsStageTable''').execution_options(autocommit=True))

    #Begin paginating through API data per Zendesk Support API documentation
    while url:
         response = requests.get(url, auth=(user, pwd))
         data = response.json()

         #Insert API data into pandas dataframe (all columns) and normalize json
         dfallcols = pd.json_normalize(data['ticket_forms'])

         #Create dataframe with only columns that will be inserted into staging table
         dfRequiredCols = dfallcols[['id','name','display_name','end_user_visible','position','active','default','in_all_brands','created_at','updated_at']]

         #Convert date string from UTC format to datetime data type
         dfRequiredCols['created_at'] = pd.to_datetime(dfRequiredCols['created_at'])
         dfRequiredCols['updated_at'] = pd.to_datetime(dfRequiredCols['updated_at'])

         #Add data warehouse audit timestamp fields to dataframe to audit to staging table
         dfRequiredCols['DwhInsert'] = datetime.now()
         dfRequiredCols['DwhUpdate'] = datetime.now()

         #Rename columns in dataframe to match database column names in preparation for staging table insert
         dfRequiredCols = dfRequiredCols.rename(columns = {'id':'TicketFormID',
                                                           'name':'FormName',
                                                           'display_name':'DisplayName',
                                                           'end_user_visible':'EndUserVisible',
                                                           'position':'Position',
                                                           'active':'Active',
                                                           'default':'FormDefault',
                                                           'in_all_brands':'InAllBrands',
                                                           'created_at':'CreatedAt',
                                                           'updated_at':'UpdatedAt',
                                                           'DwhInsert':'DwhInsert',
                                                           'DwhUpdate':'DwhUpdate'})

         #Create dataframe for form fields
         dfTicketFormFields = pd.json_normalize(data=data['ticket_forms'],
                                    record_path='ticket_field_ids',
                                    meta=['id'])

         # Name columns something more meaningful
         dfTicketFormFields.columns = ['TicketFieldID','TicketFormID']

         #Add data warehouse audit timestamp fields to dataframe to audit to staging table
         dfTicketFormFields['DwhInsert'] = datetime.now()
         dfTicketFormFields['DwhUpdate'] = datetime.now()

         #Insert dataframe rows into data warehouse staging table
         dfRequiredCols.to_sql('StageZDTicketForms', schema='dbo', con = engine, if_exists = 'append', chunksize = 100, index=False)

         #Insert dataframe rows into data warehouse staging table
         dfTicketFormFields.to_sql('StageZDTicketFormsFields', schema='dbo', con = engine, if_exists = 'append', chunksize = 100, index=False)

         #Print on screen for development purposes
         #print(dfRequiredCols.head(5).to_markdown())

         #Continue paginating through Zendesk incremental API call while next_page value is not null per Zendesk API documentation
         url = data['next_page']

         #Stop paginating through Zendesk incremental API call when next_page is null, all API pages have been returned
         if data['next_page'] is None:
            break

    #Zendesk API does not return deleted/inactived records in some cases.  We do not want to truncate the reporting/production/operational table and reload,
    #but want to retain any deleted/inactivated records by only updating previously loaded records and inserting any new records.

    #Update existing records on reporting table with most recent information matching data from staging table on key field.
    #Note:  Update must be performed first before insert of new records.  We don't want to insert new users first and
    #then immediately turn around and update the newly inserted records that don't need updating.
    engine.execute(text('''EXEC dbo.usp_UpdateZDTicketFormsTable''').execution_options(autocommit=True))

    #Insert new records into reporting table
    engine.execute(text('''EXEC dbo.usp_LoadZDTicketFormsTable''').execution_options(autocommit=True))

    #Delete data from forms fields table before load
    engine.execute(text('''EXEC dbo.usp_DeleteZDTicketFormsFieldsTable''').execution_options(autocommit=True))

    #Insert new records from staging table into reporting table
    engine.execute(text('''EXEC dbo.usp_LoadZDTicketFormsFieldsTable''').execution_options(autocommit=True))

    #Delete data from staging table
    engine.execute(text('''EXEC dbo.usp_DeleteZDTicketFormsTable''').execution_options(autocommit=True))

    #Delete data from staging table
    engine.execute(text('''EXEC dbo.usp_DeleteZDTicketFormsFieldsStageTable''').execution_options(autocommit=True))

def main():
    RefreshZenDeskTicketForms()

if __name__ == '__main__':
    main()
