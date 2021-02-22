#-------------------------------------------------------------------------------
# Name:        LaborZendeskRefreshBrands
# Purpose:     Perform refresh of Zendesk Brands from API to data warehouse
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

# define variables for self and default values
myNameIs = os.path.basename(sys.argv[0])
pidNum = str(os.getpid())

# define variables for related file names
thisMonth = datetime.now().strftime("%Y%m")
baseDirName = "." if os.path.dirname(sys.argv[0]) == "" else os.path.dirname(sys.argv[0])
baseFileName = baseDirName + os.path.sep + myNameIs.split(".")[0] 
parmFileName = baseFileName + ".ini"
logFileName = baseFileName + "." + thisMonth + ".log"

class invalidLogLevel(Exception): pass
class invalidAPIResponse(Exception): pass

def logmsg(msgtxt, msgtype):
    #
    # DEBUG is used for helping a developer know what's happening during execution of the code.  This could even include logging variable values.
    # INFO is used for normal execution information: This step started.  This step completed. This file was successfully loaded.  etc.
    # WARNING is used for errors that the program knows how to handle.
    # ERROR is used for errors that the program does not expect and will cause the program to halt.
    #
    switcher = {
        0: "DEBUG: ",
        1: "INFO: ",
        2: "WARNING: ",
        3: "ERROR: "
    }       
    if msgtype >= logLevel:
        logdate=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        frmtdMsgTxt=logdate + " - [" + myNameIs + ":" + pidNum + "] " + switcher.get(msgtype) + msgtxt
        if screenIt: print(frmtdMsgTxt)
        if logIt: logfile.write(frmtdMsgTxt + os.linesep)

def getParm(fileSpec,parmSectionName,parmName):
    configp = configparser.ConfigParser()
    configp.read(fileSpec)
    parmValue = configp.get(parmSectionName,parmName)
    return parmValue

try:

    screenIt = True
    logIt = True

    logLevel = int(getParm(parmFileName,'standardScriptParms','logLevel'))

    #Parameters for Zendesk API connection need to be removed from script and placed into an ini file
    url = 'https://lwdsupport.tn.gov/api/v2/brands.json'
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
    engine.execute(text('''EXEC dbo.usp_DeleteZDBrandsTable''').execution_options(autocommit=True))

    #Begin paginating through API data per Zendesk Support API documentation
    while url:
         response = requests.get(url, auth=(user, pwd))
         data = response.json()

         #Insert API data into pandas dataframe (all columns) and normalize json
         dfallcols = pandas.json_normalize(data['brands'])

         #Create dataframe with only columns that will be inserted into staging table
         dfRequiredCols = dfallcols[['id','name','brand_url','subdomain','has_help_center','help_center_state','active','default','is_deleted','host_mapping','created_at','updated_at']]

         #Convert date string from UTC format to datetime data type
         dfRequiredCols['created_at'] = pandas.to_datetime(dfRequiredCols['created_at'])
         dfRequiredCols['updated_at'] = pandas.to_datetime(dfRequiredCols['updated_at'])

         #Add data warehouse audit timestamp fields to dataframe to audit to staging table
         dfRequiredCols['DwhInsert'] = datetime.now()
         dfRequiredCols['DwhUpdate'] = datetime.now()

         #Rename columns in dataframe to match database column names in preparation for staging table insert
         dfRequiredCols = dfRequiredCols.rename(columns = {'id':'BrandID',
                                                            'name':'BrandName',
                                                            'brand_url':'BrandURL',
                                                            'subdomain':'Subdomain',
                                                            'has_help_center':'HasHelpCenter',
                                                            'help_center_state':'HelpCenterState',
                                                            'active':'Active',
                                                            'default':'DefaultBrand',
                                                            'is_deleted':'IsDeleted',
                                                            'host_mapping':'HostMapping',
                                                            'created_at':'CreatedAt',
                                                            'updated_at':'UpdatedAt',
                                                            'DwhInsert':'DwhInsert',
                                                            'DwhUpdate':'DwhUpdate'})

         #Insert dataframe rows into data warehouse staging table
         dfRequiredCols.to_sql('StageZDBrands', schema='dbo', con = engine, if_exists = 'append', chunksize = 100, index=False)

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
    engine.execute(text('''EXEC dbo.usp_UpdateZDBrandsTable''').execution_options(autocommit=True))

    #Insert new records into reporting table
    engine.execute(text('''EXEC dbo.usp_LoadZDBrandsTable''').execution_options(autocommit=True))

    #Delete data from staging table
    engine.execute(text('''EXEC dbo.usp_DeleteZDBrandsTable''').execution_options(autocommit=True))

except invalidLogLevel:
    logLevel = 0
    logmsg ("Invalid log level set.  Resetting to 0.",0)
    return

finally:
    logfile.close
    conn.close()
    del conn