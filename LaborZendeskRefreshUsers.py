#-------------------------------------------------------------------------------
# Name:        LaborZendeskRefreshUsers
# Purpose:     Perform refresh of Zendesk Users from API to data warehouse
# Author:      Chad Rue
# Created:     01/30/2021
#-------------------------------------------------------------------------------

#import built-in and custom packages
import requests
import csv
import json
import pandas
import urllib
import pyodbc
from sqlalchemy import create_engine,text
from datetime import datetime, timezone
from dateutil import tz
import dateutil.parser


def RefreshZendeskUsers():

    #Parameters for Zendesk API connection need to be removed from script and placed into an ini file
    url = 'https://lwdsupport.tn.gov/api/v2/users.json?role[]=admin&role[]=agent'
    user = 'Joe.W.Denton@tn.gov' + '/token'
    pwd = 'zjRDNwnE8odwtsOq0tZevpxgJGMVacYFvyIQzjMP'

    #Perform HTTP request/API call by passing credentials to access data
    response = requests.get(url, auth=(user, pwd))

    #Check for HTTP codes other than 200 and exit program if true
    if response.status_code != 200:
            print('Status:', response.status_code, 'Problem with the request. Exiting.')
            exit()

    #Create database connection
    conn = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=CG01NDCWB00007;DATABASE=CG_UI_DataWH;Trusted_Connection=yes;")
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn))

    #Delete data from staging table before loading API data into staging table
    engine.execute(text('''EXEC dbo.usp_DeleteZDUsersTable''').execution_options(autocommit=True))

    #Begin paginating through API data per Zendesk Support API documentation
    while url:
         response = requests.get(url, auth=(user, pwd))
         data = response.json()

         #Insert API data into pandas dataframe (all columns) and normalize json
         dfallcols = pandas.json_normalize(data['users'])

         #Create dataframe with only columns that will be inserted into staging table
         dfRequiredCols = dfallcols[['id','name','alias','email','phone','time_zone','role','role_type','default_group_id','organization_id','active','suspended','ticket_restriction','chat_only','last_login_at','created_at','updated_at']]

         #Convert date string from UTC format to datetime data type
         dfRequiredCols['last_login_at'] = pandas.to_datetime(dfRequiredCols['last_login_at'])
         dfRequiredCols['created_at'] = pandas.to_datetime(dfRequiredCols['created_at'])
         dfRequiredCols['updated_at'] = pandas.to_datetime(dfRequiredCols['updated_at'])

         #Add data warehouse audit timestamp fields to dataframe to audit to staging table
         dfRequiredCols['DwhInsert'] = datetime.now()
         dfRequiredCols['DwhUpdate'] = datetime.now()

         #Rename columns in dataframe to match database column names in preparation for staging table insert
         dfRequiredCols = dfRequiredCols.rename(columns = {'id':'UserID',
                                                            'name':'DisplayName',
                                                            'alias':'UserAlias',
                                                            'email':'UserEmail',
                                                            'phone':'PhoneNumber',
                                                            'time_zone':'UserTimezone',
                                                            'role':'UserRole',
                                                            'role_type':'RoleType',
                                                            'default_group_id':'DefaultGroupID',
                                                            'organization_id':'OrganizationID',
                                                            'active':'Active',
                                                            'suspended':'Suspended',
                                                            'ticket_restriction':'TicketRestriction',
                                                            'chat_only':'ChatOnly',
                                                            'last_login_at':'LastLoginAt',
                                                            'created_at':'CreatedAt',
                                                            'updated_at':'UpdatedAt',
                                                            'DwhInsert':'DwhInsert',
                                                            'DwhUpdate':'DwhUpdate'})

         #Insert dataframe rows into data warehouse staging table
         dfRequiredCols.to_sql('StageZDUsers', schema='dbo', con = engine, if_exists = 'append', chunksize = 100, index=False)

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
    #then turn around and update them immediately.
    engine.execute(text('''EXEC dbo.usp_UpdateZDUsersTable''').execution_options(autocommit=True))

    #Insert new records into reporting table
    engine.execute(text('''EXEC dbo.usp_LoadZDUsersTable''').execution_options(autocommit=True))

    #Delete data from staging table
    engine.execute(text('''EXEC dbo.usp_DeleteZDUsersTable''').execution_options(autocommit=True))

def main():
    RefreshZendeskUsers()

if __name__ == '__main__':
    main()