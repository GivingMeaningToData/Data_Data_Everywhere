#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datetime
import xlsxwriter
from pandas import DataFrame
import numpy as np
from pymongo import MongoClient
from pprint import pprint
import json
import csv
import pandas as pd
import pandas.io.sql as psql
import mysql.connector
from pandas import DataFrame

# client = MongoClient('mongodb://pristynRoot:pristyn321Root@10.0.25.154:27017/bitnami_parse') old one
client = MongoClient('mongodb://viewUserDec2023:wqxEJeoIcPaK@10.0.25.154:27017/bitnami_parse')
db=client.bitnami_parse
#date_now = datetime.datetime.now()
# delta = datetime.timedelta(days= 32, hours=0, minutes=0, seconds=0)
# date_now = datetime.datetime.now()
# d= date_now-delta

mydb = mysql.connector.connect(
  host="marketing-db.cmwukub0eama.ap-south-1.rds.amazonaws.com",
  user="admin",
  password="marketing",
  database="marketingDashboard"
)
mycursor = mydb.cursor()
conn = mycursor.execute


Enquiry = pd.read_sql_query(f"SELECT enquiry_id,leadId,tenDayUnique,thirty_day_unique_flag,mobile_number from marketingDashboard.enquiry_table where Created_at_IST > SYSDATE() - interval 39 day",mydb)
#x = Enquiry.dropna()
x = Enquiry.dropna(subset=['mobile_number'])
mobile_number =  tuple(x['mobile_number'].unique())

df_Enq = pd.read_sql_query(f"SELECT leadId,lead_source,form_name,created_at_ist_day,created_at_ist_year,sr_number,call_status,full_url,url,city,disease,source,enquiry_id,created_at_ist,created_at_ist_date,created_at_ist_month,created_at_ist_hour,disease_marketing,city_marketing,mobile_number,OperationalStatus,team_final,category_final from enquiry_table  where mobile_number in {mobile_number} ; ",mydb)
df_Enq['Team_final'] = np.where(df_Enq['team_final']=='Metro',df_Enq['category_final'],df_Enq['team_final'])

data = df_Enq.loc[(df_Enq["Team_final"]!="MD")&
                        (df_Enq["Team_final"]!="PXBD")&
                        (df_Enq["Team_final"]!="instituionalSales")&
                        (df_Enq["Team_final"]!="Pharma")&
                        (df_Enq["Team_final"]!="TVC")&
                        (df_Enq["Team_final"]!="App Install")&
                        (df_Enq["Team_final"]!="Covid-19")&
                        (df_Enq["Team_final"]!="General")&
                        (df_Enq["Team_final"]!="Diagnostic")&
                        (df_Enq["Team_final"]!="Central")&
                        (df_Enq["Team_final"]!="test")&
                        (df_Enq["Team_final"]!="COVID")&
                        (df_Enq["Team_final"]!="CentalBD")&
                        (df_Enq["Team_final"]!="Offline_Business")&
                        (df_Enq["Team_final"]!="satya_test")&
                        (df_Enq["Team_final"]!="CentralBD")&
                        (df_Enq["Team_final"]!="Other")&
                        (df_Enq["Team_final"]!="instituionalSales")&
                        (df_Enq["Team_final"]!="Offline_Business")&
                        (df_Enq["Team_final"]!="Diagnostic")&
                        (df_Enq["Team_final"]!="instituionalSales_1")&
                        (df_Enq["Team_final"]!="Covid")&
                        (df_Enq["Team_final"]!="General Physician")&
                        (df_Enq["Team_final"]!="Ayurveda")&
                        (df_Enq["Team_final"].notnull())]


# data = data[~data['Mobile'].str.contains(r'\b(\d)(?=\1{9})\d{9}\b')]

data['created_at_ist'] =  pd.to_datetime(data['created_at_ist'])
data = data.sort_values(['mobile_number','created_at_ist'],ignore_index=True)
# data['Date'] = data['Created_at_IST'].dt.date
data

data['diff_days'] = data.groupby('mobile_number')['created_at_ist'].diff().dt.days

data['sevenDay'] = False
data.loc[data.groupby('mobile_number').head(1).index, 'sevenDay'] = True
data.loc[data['diff_days'] >= 7, 'sevenDay'] = True

delta = datetime.timedelta(days= 39, hours=0, minutes=0, seconds=0)
date_now = datetime.datetime.now()
d= date_now-delta

df_Enq = data[(data['created_at_ist']>=d)&(data['sevenDay']==True)]
df_Enq

##########1
df_Enq['Check'] = np.where(df_Enq['city']=="Others",0,
                           np.where(df_Enq['city']=="Other",0,
                                    np.where(df_Enq['city'].isnull(),0,
                                             np.where(df_Enq['city']=="NA",0,
                                                      np.where(df_Enq['city']=="other",0,
                                                               np.where(df_Enq['city']=="others",0,
                                                                        np.where(df_Enq['city']=="none",0,
                                                                                 np.where(df_Enq['city']=="None",0,1))))))))

df_Leads_LeadID = df_Enq['enquiry_id']
x = df_Leads_LeadID.to_string(header=False, index=False).split('\n')
                  
vals = [','.join(ele.split()) for ele in x]
vals

# vals = list(df_Enq['enquiry_id'].unique())

# Extracting Knwlarity calls data
col = db["EnquiriesView"]
cursor = col.aggregate([
    {'$match' : { '_id' : { '$in' : vals} }},
    {
            "$lookup": {
                "from": "LeadsView",
                "localField": "leadId",
                "foreignField": "_id",
                "as": "LeadDetails"
            }
        },
        {
            "$unwind": {
                "path": "$LeadDetails",
                 "preserveNullAndEmptyArrays": True
            }
        },
    {
       "$project":
         {
 "Enq_id":"$_id" ,           
# "leadId":"$leadId",
"category_current":"$LeadDetails.Disease",
"concernedTeam":"$LeadDetails.concernedTeam",
"BDAssigned" : "$LeadDetails.BDAssigned",
"City_Leads" :"$LeadDetails.City",
"Disease_Leads" : "$LeadDetails.SurgeryType",
"leadStatus" : "$LeadDetails.leadStatus",
"leadSubStatus":"$LeadDetails.leadSubStatus",
"InsStatus" : "$LeadDetails.InsStatus",
"subLeadSource":"$subLeadSource",
#Change_Here_10_Jul
"isResurfaced":"$isResurfaced.resurfaced",
         }}])

Enquiries = list(cursor)
df_Enquiries=DataFrame(Enquiries)
df_Enquiries.dtypes
df_Enquiries

df_Enq_Col_BD = pd.merge(df_Enq,df_Enquiries,left_on=['enquiry_id'], right_on = ['Enq_id'], how = 'left')
df_Enq_Col_BD
#Change_Here_10_Jul_First_Line
df_Enq_Col_BD["lead_source1"] =         np.where(df_Enq_Col_BD['isResurfaced'] == True,"ReSurfaced",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "GP - Pristyn"),"DocRef_GP",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "GP-Pristyn"),"DocRef_GP", 
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "GP"),"DocRef_GP",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "GP - Lybrate"),"DocRef_GP",          
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "Partner"),"Referral_PilesLaser",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "FT"),"DocRef_FT",
                                        np.where((df_Enq_Col_BD['lead_source'] == "IS_Referral")&(df_Enq_Col_BD['subLeadSource']== "GP"),"DocRef_GP",
                                        np.where((df_Enq_Col_BD['lead_source'] == "IS_Partnerships")&(df_Enq_Col_BD['subLeadSource']== "Medibuddy"),"Medibuddy",
                                        np.where((df_Enq_Col_BD['lead_source'] == "insurancecore")&(df_Enq_Col_BD['subLeadSource']== "futurisk"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral_AS")&(df_Enq_Col_BD['subLeadSource'].isnull()),"DocRef_Others",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Partner_2")&(df_Enq_Col_BD['subLeadSource']== "Meradoc"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource'].isnull()),"DocRef_Others",
                                        np.where((df_Enq_Col_BD['lead_source'] == "IS_Partnerships")&(df_Enq_Col_BD['subLeadSource']== "Ekin care"),"Referral_EkinCare",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "NCR - Field Team"),"DocRef_Others",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Truworth")&(df_Enq_Col_BD['subLeadSource'].isnull()),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "insurancecore")&(df_Enq_Col_BD['subLeadSource']== "SPA Insurance"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "Ex- VC"),"DocRef_PCVC_VC",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Medibuddy")&(df_Enq_Col_BD['subLeadSource'].isnull()),"Medibuddy",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Partner_2")&(df_Enq_Col_BD['subLeadSource']== "visit"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Partner_2")&(df_Enq_Col_BD['subLeadSource']== "GP"),"DocRef_GP",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "VC"),"DocRef_PCVC_VC",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "Patient referral"),"DocRef_Others",
                                        np.where((df_Enq_Col_BD['lead_source'] == "IS_Partnerships")&(df_Enq_Col_BD['subLeadSource']== "pc-partner-pazcare"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Partner_2")&(df_Enq_Col_BD['subLeadSource'].isnull()),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "IS_Partnerships")&(df_Enq_Col_BD['subLeadSource']== "phonepe_brand_cpm"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Partner_2")&(df_Enq_Col_BD['subLeadSource']== "Non-Referral"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "GP- FT"),"DocRef_FT",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "GP - Pristyn 1"),"DocRef_GP",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "GP - Pristyn 2"),"DocRef_GP",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "Field Team"),"DocRef_Others",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "Patners"),"Referral_PilesLaser",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Medibuddy")&(df_Enq_Col_BD['subLeadSource']== "Medibuddy"),"Medibuddy",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "Internal"),"DocRef_Others",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "GP-FT"),"DocRef_FT",
                                        np.where((df_Enq_Col_BD['lead_source'] == "insurancecore")&(df_Enq_Col_BD['subLeadSource']== "wns"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "Ex-VC"),"DocRef_PCVC_VC",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Partner_2")&(df_Enq_Col_BD['subLeadSource']== "Referral"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "IS_Referral")&(df_Enq_Col_BD['subLeadSource']== "HEALWELL24"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "IS_Referral")&(df_Enq_Col_BD['subLeadSource']== "HEALTH-ASSURE"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "insurancecore")&(df_Enq_Col_BD['subLeadSource']== "HealthieU"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Medibuddy_Offline")&(df_Enq_Col_BD['subLeadSource'].isnull()),"Medibuddy",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Partner_2")&(df_Enq_Col_BD['subLeadSource']== "palm-broker"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Partner_2")&(df_Enq_Col_BD['subLeadSource']== "team-com"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "Patient"),"DocRef_Others",
                                        np.where((df_Enq_Col_BD['lead_source'] == "IS_Partnerships")&(df_Enq_Col_BD['subLeadSource']== "SAFETREE INSURANCE"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "IS_Referral")&(df_Enq_Col_BD['subLeadSource']== "SAFETREE%20INSURANCE"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "insurancecore")&(df_Enq_Col_BD['subLeadSource']== "ZoomBrokers"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "insurancecore")&(df_Enq_Col_BD['subLeadSource'].isnull()),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "insurancecore")&(df_Enq_Col_BD['subLeadSource']== "arpr"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Medibuddy")&(df_Enq_Col_BD['subLeadSource']== "MediBuddy/"),"Medibuddy",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Partner_2")&(df_Enq_Col_BD['subLeadSource']== "Prisha"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Partner_2")&(df_Enq_Col_BD['subLeadSource']== "r4u"),"Referral_OtherPartners",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Doctor's Referral")&(df_Enq_Col_BD['subLeadSource']== "Partners"),"Referral_PilesLaser",
                                        np.where(df_Enq_Col_BD['lead_source'] == "Patient's Referral - PX","Patient_Referral_Px",
                                        np.where(df_Enq_Col_BD['lead_source'] == "Doctor's Referral_AS","DocRef_Others", 
                                        np.where(df_Enq_Col_BD['lead_source'] == "YT_Channel","Referral_OtherPartners",                  
                                        np.where(df_Enq_Col_BD['lead_source'] == "Patient's Referral","Patient_Referral_Cat",
                                        np.where((df_Enq_Col_BD['lead_source'] == "Brand")&(df_Enq_Col_BD['subLeadSource']== "My Gate"),"MyGate",
                                                 df_Enq_Col_BD['source'])))))))))))))))))))))))))))))))))))))))))))))))))))))))))))


bd = db["BDList"]
BDLIST = bd.aggregate([
     {
            "$lookup": {
                "from": "_User",
                "localField": "BDObjectId",
                "foreignField": "_id",
                "as": "UserDetails"
            }
        },
        {
            "$unwind": {
                "path": "$UserDetails",
                 "preserveNullAndEmptyArrays": True
            }
        },
    {
       "$project":
         {
             "BDName_Current":"$UserDetails.email",
             "BDObjectId":"$BDObjectId",
             "BD_Team_Current" :"$Team",
         } 
    }
])
BD = list(BDLIST)
df_BD=DataFrame(BD)
df_Enq_Col_BD = pd.merge(df_Enq_Col_BD,df_BD,left_on=['BDAssigned'], right_on = ['BDObjectId'], how = 'left')

df_Enq_Col_BD.to_csv('final_funnel_leads.csv')
df_Enq_Col_BD['URL_V2'] = np.where(df_Enq_Col_BD['full_url'].isnull(),df_Enq_Col_BD['url'],
                                   np.where(df_Enq_Col_BD['full_url']=="NULL",df_Enq_Col_BD['url'],df_Enq_Col_BD['full_url']))

df_Enq_Col_BD['LeadMode'] = np.where(df_Enq_Col_BD['sr_number'].isnull(),"Forms",
                              np.where(df_Enq_Col_BD['sr_number']=="","Forms",       
                                   np.where(df_Enq_Col_BD['sr_number']=="NULL","Forms",
                                            np.where(df_Enq_Col_BD['sr_number']==0,"Forms","Call"))))

df_Enq_Col_BD

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("/home/ubuntu/Crons/business-reporting-297220-1bd8456f1dda.json", scope)
client = gspread.authorize(creds)
sheet = client.open("CategoryLookUpnew").sheet1  # Open the spreadhseet
data = sheet.get_all_records()  # Get a list of all records
Data = list(data)
df_CatLook=DataFrame(Data)
df_CatLook.drop(df_CatLook[df_CatLook['CatDB'].isnull()].index, inplace = True)
df_CatLook


import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("/home/ubuntu/Crons/business-reporting-297220-1bd8456f1dda.json", scope)
client = gspread.authorize(creds)
sheet = client.open("TeamLookup").sheet1  # Open the spreadhseet
data = sheet.get_all_records()  # Get a list of all records
Data = list(data)
df_teamLook=DataFrame(Data)
df_teamLook.drop(df_teamLook[df_teamLook['TeamDB'].isnull()].index, inplace = True)
df_teamLook


df_Enq_Col_BD_Cat = pd.merge(df_Enq_Col_BD,df_CatLook,left_on=['category_current'], right_on = ['CatDB'], how = 'left')

df_Enq_Col_BD_Cat_team = pd.merge(df_Enq_Col_BD_Cat,df_teamLook,left_on=['concernedTeam'], right_on = ['TeamDB'], how = 'left')
# df_Enq_Col_BD_Cat_team.to_csv('cehck_2.csv')
df_Enq_Col_BD_Cat_team['Final_Category'] = np.where(df_Enq_Col_BD_Cat_team['Team_STD']=="Metro",
                                                    df_Enq_Col_BD_Cat_team['Cat_STD'],df_Enq_Col_BD_Cat_team['Team_STD'])


df_Enq_Col_BD_Cat_team['EnqMonthCount'] = 1
df_Enq_Col_BD_Cat_team_V2 = df_Enq_Col_BD_Cat_team[['Enq_id','leadId',
'mobile_number',
'lead_source',
'sr_number',
'InsStatus',
'disease_marketing',
'form_name',
'lead_source1',
'BDName_Current',
'created_at_ist_date',
'created_at_ist_year',
'created_at_ist_month',
'created_at_ist_day',
'created_at_ist_hour',
'leadStatus',
'leadSubStatus',
'EnqMonthCount',
'Disease_Leads',
'Final_Category',
'LeadMode',
'call_status',
'city',
'city_marketing',
'City_Leads',
'URL_V2',
'Team_STD',
'Cat_STD',
'sevenDay',
'OperationalStatus']]    


df_Enq_Col_BD_Cat_team_V2 = df_Enq_Col_BD_Cat_team_V2.rename(columns = 
                                                            {
'leadId' : 'leadId',
'mobile_number' : 'Mobile',
'lead_source' : 'Lead_Source',
'sr_number' : 'SRNumber',
'InsStatus' : 'InsStatus',
'disease_marketing' : 'Disease_SR',
'form_name' : 'FormName',
'lead_source1' : 'Source_V1',
'BDName_Current' : 'BDName_Current',
'created_at_ist_date' : 'Enq_Date',
'created_at_ist_year' : 'Enq_Year',
'created_at_ist_month' : 'Enq_Month',
'created_at_ist_day' : 'Enq_Day',
'created_at_ist_hour' : 'Enq_hour',
'leadStatus' : 'Lead_Status',
'leadSubStatus' : 'Lead_Sub_Status',
'EnqMonthCount' : 'EnqMonthCount',
'Disease_Leads' : 'SurgeryType',
'Final_Category' : 'Final_Category',
'LeadMode' : 'LeadMode',
'call_status' : 'callStatus',
'city' : 'city',
'city_marketing' : 'City_Initial',
'City_Leads' : 'City_leads',
'URL_V2' : 'URL_V2',
'Team_STD' : 'Team_Current_STD',
'Cat_STD' : 'Category_Current_STD',
'thirty_day_unique_flag' : 'sevenDay',
'OperationalStatus'   :  'OperationalStatus'
                                                            }, inplace = False)


zebrax = datetime.timedelta(days=0, hours=18, minutes=30, seconds=0)
date_now = datetime.datetime.now()
d_xleadv2 = date_now-zebrax
date_needed_leadV1 = d_xleadv2.date
day_needed_leadV1 = d_xleadv2.day
year_needed_leadV1 = d_xleadv2.year
month_needed_leadV1 = d_xleadv2.month
day_1 = datetime.datetime(year_needed_leadV1,month_needed_leadV1,1)

df_Enq_Col_BD_Cat_team_V2['lead_checkdatetime'] = d_xleadv2
df_Enq_Col_BD_Cat_team_V2['lead_checkdate'] = df_Enq_Col_BD_Cat_team_V2['lead_checkdatetime'].dt.date
# df_Enq_Col_BD_Cat_team_V2.drop(df_Enq_Col_BD_Cat_team_V2[df_Enq_Col_BD_Cat_team_V2['Enq_Month'] != datetime.datetime.now().month].index, inplace = True) 
# df_Enq_Col_BD_Cat_team_V2.drop(df_Enq_Col_BD_Cat_team_V2[df_Enq_Col_BD_Cat_team_V2['Enq_Day'] == datetime.datetime.now().day].index, inplace = True) 
df_Enq_Col_BD_Cat_team_V2 = df_Enq_Col_BD_Cat_team_V2[(df_Enq_Col_BD_Cat_team_V2["Enq_Month"] == month_needed_leadV1)]
#df_Enq_Col_BD_Cat_team_V2['date_check'] = day_1

df_Enq_Col_BD_Cat_team_V2 = df_Enq_Col_BD_Cat_team_V2[(df_Enq_Col_BD_Cat_team_V2["Enq_Date"] <= df_Enq_Col_BD_Cat_team_V2['lead_checkdate'])]

df_Enq_Col_BD_Cat_team_V2.drop(df_Enq_Col_BD_Cat_team_V2[df_Enq_Col_BD_Cat_team_V2['leadId'].isnull()].index, inplace = True)

marketing_leads = df_Enq_Col_BD_Cat_team_V2[["leadId", "Mobile", "Lead_Source", "SRNumber", "InsStatus", "Disease_SR", "FormName", "Source_V1", "BDName_Current", "Enq_Date", "Enq_Year", "Enq_Month", "Enq_Day", 
"Enq_hour", "Lead_Status", "Lead_Sub_Status", "EnqMonthCount", "SurgeryType", "Final_Category", "LeadMode", "callStatus", "city", "City_Initial", "City_leads", "URL_V2", 
"Team_Current_STD", "Category_Current_STD", "OperationalStatus","Enq_id"]]

marketing_leads['Final_Category'] = np.where(marketing_leads['Final_Category'].isnull(),marketing_leads['Category_Current_STD'],marketing_leads['Final_Category'])

marketing_leads['FormName'] = marketing_leads['FormName'].str.lower()
# marketing_leads.drop_duplicates(subset='FormName', keep="first",inplace = True)


temp1=marketing_leads.FormName.fillna("0")
temp1.str.lower()

temp2=marketing_leads.Disease_SR.fillna("0")
temp2.str.lower()

temp3=marketing_leads.URL_V2.fillna("0")
temp3.str.lower()

temp4=marketing_leads.SurgeryType.fillna("0")
temp4.str.lower()



# temp2=df_Enquiries_All_V_SR_V3.URL.fillna("0")
# temp3=df_Enquiries_All_V_SR_V3.FormName.fillna("0")

marketing_leads['Disease_forms'] = np.where(temp1.str.contains("pilonidal",na=False, case=False),"Pilonidal Sinus",
                                            np.where(temp1.str.contains("abcess",na=False, case=False),"Abcess",
                                            np.where(temp1.str.contains("lasik",na=False, case=False),"Lasik",
                                            np.where(temp1.str.contains("b-surgery",na=False, case=False),"Breast",
                                            np.where((temp1.str.contains("breast",na=False, case=False))&(temp1.str.contains("surgery",na=False, case=False)),"Breast",
                                            np.where((temp1.str.contains("bartholin",na=False, case=False))&(temp1.str.contains("cyst",na=False, case=False)),"Bartholin's Cyst",
                                            np.where((temp1.str.contains("sebaceous",na=False, case=False))&(temp1.str.contains("cyst",na=False, case=False)),"Sebaceous Cyst",
                                            np.where((temp1.str.contains("plastic",na=False, case=False))&(temp1.str.contains("surgery",na=False, case=False)),"Plastic Surgery",
                                            np.where(temp1.str.contains("sebaceous",na=False, case=False),"Sebaceous Cyst",
                                            np.where((temp1.str.contains("ear",na=False, case=False))&(temp1.str.contains("drum",na=False, case=False)),"Ear Surgery",
                                            np.where((temp1.str.contains("ear",na=False, case=False))&(temp1.str.contains("hole",na=False, case=False)),"Ear Surgery",
                                            np.where((temp1.str.contains("nasal",na=False, case=False))&(temp1.str.contains("polyp",na=False, case=False)),"Nose Surgery",
                                            np.where(temp1.str.contains("tympanoplasty",na=False, case=False),"Ear Surgey",
                                            np.where(temp1.str.contains("tympanic",na=False, case=False),"Ear Surgey",
                                            np.where(temp1.str.contains("tightening",na=False, case=False),"VT",
                                            np.where(temp1.str.contains("acl",na=False, case=False),"ACL",
                                            np.where(temp1.str.contains("append",na=False, case=False),"Appendix",
                                            np.where(temp1.str.contains("bariatric",na=False, case=False),"Bariatric",
                                            np.where(temp1.str.contains("bph",na=False, case=False),"BPH",
                                            np.where(temp1.str.contains("prostate",na=False, case=False),"BPH",
                                            np.where(temp1.str.contains("cataract",na=False, case=False),"Cataract",
                                            np.where(temp1.str.contains("catatract",na=False, case=False),"Cataract",
                                            np.where(temp1.str.contains("contoura",na=False, case=False),"Lasik",
                                            np.where(temp1.str.contains("mothiya",na=False, case=False),"Cataract",
                                            np.where(temp1.str.contains("motibind",na=False, case=False),"Cataract",
                                            np.where(temp1.str.contains("motiya",na=False, case=False),"Cataract",
                                            np.where(temp1.str.contains("focal",na=False, case=False),"Lasik",
                                            np.where(temp1.str.contains("circum",na=False, case=False),"Circumcision",
                                            np.where(temp1.str.contains("dvt",na=False, case=False),"DVT",
                                            np.where(temp1.str.contains("fibroid",na=False, case=False),"Fibroids",
                                            np.where(temp1.str.contains("fissure",na=False, case=False),"Fissure",
                                            np.where(temp1.str.contains("fistula",na=False, case=False),"Fistula",
                                            np.where(temp1.str.contains("abscess",na=False, case=False),"Abscess",
                                            np.where(temp1.str.contains("bawaseer",na=False, case=False),"Piles",
                                            np.where(temp1.str.contains("bhagandar",na=False, case=False),"Piles",
                                            np.where(temp1.str.contains("hemorrhoid",na=False, case=False),"Piles",
                                            np.where(temp1.str.contains("balanitis",na=False, case=False),"Circumcision",
                                            np.where(temp1.str.contains("phimosis",na=False, case=False),"Circumcision",
                                            np.where(temp1.str.contains("balanoposthitis",na=False, case=False),"Circumcision",
                                            np.where(temp1.str.contains("foreskin",na=False, case=False),"Circumcision",
                                            np.where(temp1.str.contains("frenuloplasty",na=False, case=False),"Circumcision",
                                            np.where(temp1.str.contains("galls",na=False, case=False),"Gallstones",
                                            np.where(temp1.str.contains("glaucoma",na=False, case=False),"Glaucoma",
                                            np.where(temp1.str.contains("gynecom",na=False, case=False),"Gynecomastia",
                                            np.where(temp1.str.contains("gynaecom",na=False, case=False),"Gynecomastia",
                                            np.where(temp1.str.contains("hernia",na=False, case=False),"Hernia",
                                            np.where(temp1.str.contains("hydrocele",na=False, case=False),"Hydrocele",
                                            np.where(temp1.str.contains("hymen",na=False, case=False),"Hymenoplasty",
                                            np.where(temp1.str.contains("hyster",na=False, case=False),"Hysterectomy",
                                            np.where(temp1.str.contains("ivf",na=False, case=False),"IVF","Not Mapped"))))))))))))))))))))))))))))))))))))))))))))))))))


marketing_leads
                                            
marketing_leads['Disease_SR_final'] = np.where(temp2.str.contains("pilonidal",na=False, case=False),"Pilonidal Sinus",
                                                np.where(temp2.str.contains("abcess",na=False, case=False),"Abcess",
                                                np.where(temp2.str.contains("lasik",na=False, case=False),"Lasik",
                                                np.where(temp2.str.contains("b-surgery",na=False, case=False),"Breast",
                                                np.where((temp2.str.contains("breast",na=False, case=False))&(temp2.str.contains("surgery",na=False, case=False)),"Breast",
                                                np.where((temp1.str.contains("bartholin",na=False, case=False))&(temp1.str.contains("cyst",na=False, case=False)),"Bartholin's Cyst",
                                                np.where((temp1.str.contains("sebaceous",na=False, case=False))&(temp1.str.contains("cyst",na=False, case=False)),"Sebaceous Cyst",
                                                np.where((temp2.str.contains("plastic",na=False, case=False))&(temp2.str.contains("surgery",na=False, case=False)),"Plastic Surgery",
                                                np.where(temp2.str.contains("sebaceous",na=False, case=False),"Sebaceous Cyst",
                                                np.where((temp2.str.contains("ear",na=False, case=False))&(temp2.str.contains("drum",na=False, case=False)),"Ear Surgery",
                                                np.where((temp2.str.contains("ear",na=False, case=False))&(temp2.str.contains("hole",na=False, case=False)),"Ear Surgery",
                                                np.where((temp2.str.contains("nasal",na=False, case=False))&(temp2.str.contains("polyp",na=False, case=False)),"Nose Surgery",
                                                np.where(temp2.str.contains("tympanoplasty",na=False, case=False),"Ear Surgey",
                                                np.where(temp2.str.contains("tympanic",na=False, case=False),"Ear Surgey",
                                                np.where(temp2.str.contains("tightening",na=False, case=False),"VT",
                                                np.where(temp2.str.contains("acl",na=False, case=False),"ACL",
                                                np.where(temp2.str.contains("append",na=False, case=False),"Appendix",
                                                np.where(temp2.str.contains("bariatric",na=False, case=False),"Bariatric",
                                                np.where(temp2.str.contains("bph",na=False, case=False),"BPH",
                                                np.where(temp2.str.contains("prostate",na=False, case=False),"BPH",
                                                np.where(temp2.str.contains("cataract",na=False, case=False),"Cataract",
                                                np.where(temp2.str.contains("catatract",na=False, case=False),"Cataract",
                                                np.where(temp2.str.contains("contoura",na=False, case=False),"Lasik",
                                                np.where(temp2.str.contains("mothiya",na=False, case=False),"Cataract",
                                                np.where(temp2.str.contains("motibind",na=False, case=False),"Cataract",
                                                np.where(temp2.str.contains("motiya",na=False, case=False),"Cataract",
                                                np.where(temp2.str.contains("focal",na=False, case=False),"Lasik",
                                                np.where(temp2.str.contains("circum",na=False, case=False),"Circumcision",
                                                np.where(temp2.str.contains("dvt",na=False, case=False),"DVT",
                                                np.where(temp2.str.contains("fibroid",na=False, case=False),"Fibroids",
                                                np.where(temp2.str.contains("fissure",na=False, case=False),"Fissure",
                                                np.where(temp2.str.contains("fistula",na=False, case=False),"Fistula",
                                                np.where(temp2.str.contains("abscess",na=False, case=False),"Abscess",
                                                np.where(temp2.str.contains("bawaseer",na=False, case=False),"Piles",
                                                np.where(temp2.str.contains("bhagandar",na=False, case=False),"Piles",
                                                np.where(temp2.str.contains("hemorrhoid",na=False, case=False),"Piles",
                                                np.where(temp2.str.contains("balanitis",na=False, case=False),"Circumcision",
                                                np.where(temp2.str.contains("phimosis",na=False, case=False),"Circumcision",
                                                np.where(temp2.str.contains("balanoposthitis",na=False, case=False),"Circumcision",
                                                np.where(temp2.str.contains("foreskin",na=False, case=False),"Circumcision",
                                                np.where(temp2.str.contains("frenuloplasty",na=False, case=False),"Circumcision",
                                                np.where(temp2.str.contains("galls",na=False, case=False),"Gallstones",
                                                np.where(temp2.str.contains("glaucoma",na=False, case=False),"Glaucoma",
                                                np.where(temp2.str.contains("gynecom",na=False, case=False),"Gynecomastia",
                                                np.where(temp2.str.contains("gynaecom",na=False, case=False),"Gynecomastia",
                                                np.where(temp2.str.contains("hernia",na=False, case=False),"Hernia",
                                                np.where(temp2.str.contains("hydrocele",na=False, case=False),"Hydrocele",
                                                np.where(temp2.str.contains("hymen",na=False, case=False),"Hymenoplasty",
                                                np.where(temp2.str.contains("hyster",na=False, case=False),"Hysterectomy",
                                                np.where(temp2.str.contains("KIDNEY STONE",na=False, case=False),"Kidney Stone",
                                                np.where(temp2.str.contains("ivf",na=False, case=False),"IVF","Not Mapped")))))))))))))))))))))))))))))))))))))))))))))))))))

marketing_leads['Url_final'] = np.where(temp3.str.contains("pilonidal",na=False, case=False),"Pilonidal Sinus",
                                                np.where(temp3.str.contains("abcess",na=False, case=False),"Abcess",
                                                np.where(temp3.str.contains("lasik",na=False, case=False),"Lasik",
                                                np.where(temp3.str.contains("b-surgery",na=False, case=False),"Breast",
                                                np.where((temp3.str.contains("breast",na=False, case=False))&(temp3.str.contains("surgery",na=False, case=False)),"Breast",
                                                np.where((temp1.str.contains("bartholin",na=False, case=False))&(temp1.str.contains("cyst",na=False, case=False)),"Bartholin's Cyst",
                                                np.where((temp1.str.contains("sebaceous",na=False, case=False))&(temp1.str.contains("cyst",na=False, case=False)),"Sebaceous Cyst",
                                                np.where((temp3.str.contains("plastic",na=False, case=False))&(temp3.str.contains("surgery",na=False, case=False)),"Plastic Surgery",
                                                np.where(temp3.str.contains("sebaceous",na=False, case=False),"Sebaceous Cyst",
                                                np.where((temp3.str.contains("ear",na=False, case=False))&(temp3.str.contains("drum",na=False, case=False)),"Ear Surgery",
                                                np.where((temp3.str.contains("ear",na=False, case=False))&(temp3.str.contains("hole",na=False, case=False)),"Ear Surgery",
                                                np.where((temp3.str.contains("nasal",na=False, case=False))&(temp3.str.contains("polyp",na=False, case=False)),"Nose Surgery",
                                                np.where(temp3.str.contains("tympanoplasty",na=False, case=False),"Ear Surgey",
                                                np.where(temp3.str.contains("tympanic",na=False, case=False),"Ear Surgey",
                                                np.where(temp3.str.contains("tightening",na=False, case=False),"VT",
                                                np.where(temp3.str.contains("acl",na=False, case=False),"ACL",
                                                np.where(temp3.str.contains("append",na=False, case=False),"Appendix",
                                                np.where(temp3.str.contains("bariatric",na=False, case=False),"Bariatric",
                                                np.where(temp3.str.contains("bph",na=False, case=False),"BPH",
                                                np.where(temp3.str.contains("prostate",na=False, case=False),"BPH",
                                                np.where(temp3.str.contains("cataract",na=False, case=False),"Cataract",
                                                np.where(temp3.str.contains("catatract",na=False, case=False),"Cataract",
                                                np.where(temp3.str.contains("contoura",na=False, case=False),"Lasik",
                                                np.where(temp3.str.contains("mothiya",na=False, case=False),"Cataract",
                                                np.where(temp3.str.contains("motibind",na=False, case=False),"Cataract",
                                                np.where(temp3.str.contains("motiya",na=False, case=False),"Cataract",
                                                np.where(temp3.str.contains("focal",na=False, case=False),"Lasik",
                                                np.where(temp3.str.contains("circum",na=False, case=False),"Circumcision",
                                                np.where(temp3.str.contains("dvt",na=False, case=False),"DVT",
                                                np.where(temp3.str.contains("fibroid",na=False, case=False),"Fibroids",
                                                np.where(temp3.str.contains("fissure",na=False, case=False),"Fissure",
                                                np.where(temp3.str.contains("fistula",na=False, case=False),"Fistula",
                                                np.where(temp3.str.contains("abscess",na=False, case=False),"Abscess",
                                                np.where(temp3.str.contains("bawaseer",na=False, case=False),"Piles",
                                                np.where(temp3.str.contains("bhagandar",na=False, case=False),"Piles",
                                                np.where(temp3.str.contains("hemorrhoid",na=False, case=False),"Piles",
                                                np.where(temp3.str.contains("balanitis",na=False, case=False),"Circumcision",
                                                np.where(temp3.str.contains("phimosis",na=False, case=False),"Circumcision",
                                                np.where(temp3.str.contains("balanoposthitis",na=False, case=False),"Circumcision",
                                                np.where(temp3.str.contains("foreskin",na=False, case=False),"Circumcision",
                                                np.where(temp3.str.contains("frenuloplasty",na=False, case=False),"Circumcision",
                                                np.where(temp3.str.contains("galls",na=False, case=False),"Gallstones",
                                                np.where(temp3.str.contains("glaucoma",na=False, case=False),"Glaucoma",
                                                np.where(temp3.str.contains("gynecom",na=False, case=False),"Gynecomastia",
                                                np.where(temp3.str.contains("gynaecom",na=False, case=False),"Gynecomastia",
                                                np.where(temp3.str.contains("hernia",na=False, case=False),"Hernia",
                                                np.where(temp3.str.contains("hydrocele",na=False, case=False),"Hydrocele",
                                                np.where(temp3.str.contains("hymen",na=False, case=False),"Hymenoplasty",
                                                np.where(temp3.str.contains("hyster",na=False, case=False),"Hysterectomy",
                                                np.where(temp3.str.contains("ivf",na=False, case=False),"IVF","Not Mapped"))))))))))))))))))))))))))))))))))))))))))))))))))

marketing_leads['SurgeryType_final'] = np.where(temp4.str.contains("pilonidal",na=False, case=False),"Pilonidal Sinus",
                                                np.where(temp4.str.contains("abcess",na=False, case=False),"Abcess",
                                                np.where(temp4.str.contains("lasik",na=False, case=False),"Lasik",         
                                                np.where(temp4.str.contains("b-surgery",na=False, case=False),"Breast",
                                                np.where((temp4.str.contains("breast",na=False, case=False))&(temp4.str.contains("surgery",na=False, case=False)),"Breast",
                                                np.where((temp1.str.contains("bartholin",na=False, case=False))&(temp1.str.contains("cyst",na=False, case=False)),"Bartholin's Cyst",
                                                np.where((temp1.str.contains("sebaceous",na=False, case=False))&(temp1.str.contains("cyst",na=False, case=False)),"Sebaceous Cyst",
                                                np.where((temp4.str.contains("plastic",na=False, case=False))&(temp4.str.contains("surgery",na=False, case=False)),"Plastic Surgery",
                                                np.where(temp4.str.contains("sebaceous",na=False, case=False),"Sebaceous Cyst",
                                                np.where((temp4.str.contains("ear",na=False, case=False))&(temp4.str.contains("drum",na=False, case=False)),"Ear Surgery",
                                                np.where((temp4.str.contains("ear",na=False, case=False))&(temp4.str.contains("hole",na=False, case=False)),"Ear Surgery",
                                                np.where((temp4.str.contains("nasal",na=False, case=False))&(temp4.str.contains("polyp",na=False, case=False)),"Nose Surgery",
                                                np.where(temp4.str.contains("tympanoplasty",na=False, case=False),"Ear Surgey",
                                                np.where(temp4.str.contains("tympanic",na=False, case=False),"Ear Surgey",
                                                np.where(temp4.str.contains("tightening",na=False, case=False),"VT",
                                                np.where(temp4.str.contains("acl",na=False, case=False),"ACL",
                                                np.where(temp4.str.contains("append",na=False, case=False),"Appendix",
                                                np.where(temp4.str.contains("bariatric",na=False, case=False),"Bariatric",
                                                np.where(temp4.str.contains("bph",na=False, case=False),"BPH",
                                                np.where(temp4.str.contains("prostate",na=False, case=False),"BPH",
                                                np.where(temp4.str.contains("cataract",na=False, case=False),"Cataract",
                                                np.where(temp4.str.contains("catatract",na=False, case=False),"Cataract",
                                                np.where(temp4.str.contains("contoura",na=False, case=False),"Lasik",
                                                np.where(temp4.str.contains("mothiya",na=False, case=False),"Cataract",
                                                np.where(temp4.str.contains("motibind",na=False, case=False),"Cataract",
                                                np.where(temp4.str.contains("motiya",na=False, case=False),"Cataract",
                                                np.where(temp4.str.contains("focal",na=False, case=False),"Lasik",
                                                np.where(temp4.str.contains("circum",na=False, case=False),"Circumcision",
                                                np.where(temp4.str.contains("dvt",na=False, case=False),"DVT",
                                                np.where(temp4.str.contains("fibroid",na=False, case=False),"Fibroids",
                                                np.where(temp4.str.contains("fissure",na=False, case=False),"Fissure",
                                                np.where(temp4.str.contains("fistula",na=False, case=False),"Fistula",
                                                np.where(temp4.str.contains("abscess",na=False, case=False),"Abscess",
                                                np.where(temp4.str.contains("bawaseer",na=False, case=False),"Piles",
                                                np.where(temp4.str.contains("bhagandar",na=False, case=False),"Piles",
                                                np.where(temp4.str.contains("hemorrhoid",na=False, case=False),"Piles",
                                                np.where(temp4.str.contains("balanitis",na=False, case=False),"Circumcision",
                                                np.where(temp4.str.contains("phimosis",na=False, case=False),"Circumcision",
                                                np.where(temp4.str.contains("balanoposthitis",na=False, case=False),"Circumcision",
                                                np.where(temp4.str.contains("foreskin",na=False, case=False),"Circumcision",
                                                np.where(temp4.str.contains("frenuloplasty",na=False, case=False),"Circumcision",
                                                np.where(temp4.str.contains("galls",na=False, case=False),"Gallstones",
                                                np.where(temp4.str.contains("glaucoma",na=False, case=False),"Glaucoma",
                                                np.where(temp4.str.contains("gynecom",na=False, case=False),"Gynecomastia",
                                                np.where(temp4.str.contains("gynaecom",na=False, case=False),"Gynecomastia",
                                                np.where(temp4.str.contains("hernia",na=False, case=False),"Hernia",
                                                np.where(temp4.str.contains("hydrocele",na=False, case=False),"Hydrocele",
                                                np.where(temp4.str.contains("hymen",na=False, case=False),"Hymenoplasty",
                                                np.where(temp4.str.contains("hyster",na=False, case=False),"Hysterectomy",
                                                np.where(temp4.str.contains("ivf",na=False, case=False),"IVF","Not Mapped"))))))))))))))))))))))))))))))))))))))))))))))))))
#marketing_leads

marketing_leads["Disease_Final"] = marketing_leads["Disease_SR_final"]
marketing_leads["Disease_Final1"] = np.where(marketing_leads["Disease_Final"] == "Not Mapped",marketing_leads["Disease_forms"],marketing_leads["Disease_Final"])
marketing_leads["Disease_Final2"] = np.where(marketing_leads["Disease_Final1"] == "Not Mapped",marketing_leads["Url_final"],marketing_leads["Disease_Final1"])
marketing_leads["Disease_Final3"] = np.where(marketing_leads["Disease_Final2"] == "Not Mapped",marketing_leads["SurgeryType_final"],marketing_leads["Disease_Final2"])
marketing_leads["Disease_Final4"] = np.where(marketing_leads["Disease_Final3"] == "Not Mapped",marketing_leads["SurgeryType"],marketing_leads["Disease_Final3"])
marketing_leads
marketing_leads_final  = marketing_leads[["leadId", "Mobile", "Lead_Source", "SRNumber", "InsStatus", "Disease_SR", "FormName", "Source_V1", "BDName_Current", "Enq_Date", "Enq_Year", 
                                        "Enq_Month", "Enq_Day", "Enq_hour", "Lead_Status", "EnqMonthCount", "SurgeryType", "Final_Category", "LeadMode", "callStatus", "city", "City_Initial", 
                                        "City_leads", "URL_V2", "Team_Current_STD", "Disease_Final4", "Category_Current_STD", "Lead_Sub_Status", "OperationalStatus","Enq_id"]]

marketing_leads_final["Slot Bracket"] = np.where(marketing_leads_final['Enq_hour']==0,"12 AM to 9 AM",
                                                np.where(marketing_leads_final['Enq_hour']==1,"12 AM to 9 AM",
                                                np.where(marketing_leads_final['Enq_hour']==2,"12 AM to 9 AM",
                                                np.where(marketing_leads_final['Enq_hour']==3,"12 AM to 9 AM",
                                                np.where(marketing_leads_final['Enq_hour']==4,"12 AM to 9 AM",
                                                np.where(marketing_leads_final['Enq_hour']==5,"12 AM to 9 AM",
                                                np.where(marketing_leads_final['Enq_hour']==6,"12 AM to 9 AM",
                                                np.where(marketing_leads_final['Enq_hour']==7,"12 AM to 9 AM",
                                                np.where(marketing_leads_final['Enq_hour']==8,"12 AM to 9 AM",
                                                np.where(marketing_leads_final['Enq_hour']==9,"9 AM to 11 AM",
                                                np.where(marketing_leads_final['Enq_hour']==10,"9 AM to 11 AM",
                                                np.where(marketing_leads_final['Enq_hour']==11,"11 AM to 1 AP",
                                                np.where(marketing_leads_final['Enq_hour']==12,"11 AM to 1 AP",
                                                np.where(marketing_leads_final['Enq_hour']==13,"1 PM to 3 PM",
                                                np.where(marketing_leads_final['Enq_hour']==14,"1 PM to 3 PM",
                                                np.where(marketing_leads_final['Enq_hour']==15,"3 PM to 5 PM",
                                                np.where(marketing_leads_final['Enq_hour']==16,"3 PM to 5 PM",
                                                np.where(marketing_leads_final['Enq_hour']==17,"5 PM to 7 PM",
                                                np.where(marketing_leads_final['Enq_hour']==18,"5 PM to 7 PM",
                                                np.where(marketing_leads_final['Enq_hour']==19,"7 PM to 9 PM",
                                                np.where(marketing_leads_final['Enq_hour']==20,"7 PM to 9 PM",
                                                np.where(marketing_leads_final['Enq_hour']==21,"9 PM to 12 AM",
                                                np.where(marketing_leads_final['Enq_hour']==22,"9 PM to 12 AM",
                                                np.where(marketing_leads_final['Enq_hour']==23,"9 PM to 12 AM","NA"))))))))))))))))))))))))




# marketing_leads_final["city_A"] = np.where(marketing_leads_final["City_leads"].isnull(),marketing_leads_final["City_Initial"],marketing_leads_final["City_leads"])
# marketing_leads_final["city_B"] = np.where(marketing_leads_final["city_A"].isnull(),marketing_leads_final["city"],marketing_leads_final["city_A"])


# marketing_leads_final["city_AA"] = np.where((marketing_leads_final["city_B"]== "Others") & (marketing_leads_final["City_Initial"].notnull()),marketing_leads_final["City_Initial"],marketing_leads_final["city_B"])                     
# # marketing_leads_final["city"] = np.where((marketing_leads_final["city_AB"]== "Others") & (marketing_leads_final["City_leads"].notnull()),marketing_leads_final["City_leads"],marketing_leads_final["city"])
# marketing_leads_final["city"] = np.where((marketing_leads_final["city_AA"]== "Others") & (marketing_leads_final["City_leads"].notnull()),marketing_leads_final["City_leads"],marketing_leads_final["city_AA"]) 

######here___CITY_MARKETING IS CITY_Initial############## 

marketing_leads_final["city_A"] = np.where(marketing_leads_final["city"].isnull(),marketing_leads_final["City_Initial"],marketing_leads_final["city"])
marketing_leads_final["city_B"] = np.where(marketing_leads_final["city_A"].isnull(),marketing_leads_final["City_leads"],marketing_leads_final["city_A"])


marketing_leads_final["city_AA"] = np.where((marketing_leads_final["city_B"]== "Others") & (marketing_leads_final["City_Initial"].notnull()),marketing_leads_final["City_Initial"],marketing_leads_final["city_B"])                     
# marketing_leads_final["city"] = np.where((marketing_leads_final["city_AB"]== "Others") & (marketing_leads_final["City_leads"].notnull()),marketing_leads_final["City_leads"],marketing_leads_final["city"])
marketing_leads_final["city"] = np.where((marketing_leads_final["city_AA"]== "Others") & (marketing_leads_final["City_leads"].notnull()),marketing_leads_final["City_leads"],marketing_leads_final["city_AA"]) 


###########################

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("/home/ubuntu/Crons/business-reporting-297220-1bd8456f1dda.json", scope)
client = gspread.authorize(creds)
sheet = client.open("citySatya2").sheet1  # Open the spreadhseet
data_city = sheet.get_all_records()  # Get a list of all records
Data_city = list(data_city)
df_Data_city=DataFrame(Data_city)
#df_Data_city
df_Data_city.drop(df_Data_city[df_Data_city['City'].isnull()].index, inplace = True)
#df_Data_city

marketing_leads_final_v1 = pd.merge(marketing_leads_final,df_Data_city,left_on=['city'], right_on = ['City'], how = 'left')
marketing_leads_final_v2 = marketing_leads_final_v1[["leadId", "Mobile", "Lead_Source", "SRNumber", "InsStatus", "Disease_SR", "FormName", "Source_V1", "BDName_Current", "Enq_Date","Enq_Year", 
                                                        "Enq_Month", "Enq_Day", "Enq_hour", "Lead_Status", "EnqMonthCount", "SurgeryType", "Final_Category", "LeadMode", "callStatus", "city", "City_Initial", 
                                                        "City_leads", "URL_V2", "Team_Current_STD", "Disease_Final4", "Category_Current_STD", "Lead_Sub_Status", "OperationalStatus",'Final Mapped City','Slot Bracket','Enq_id']]

#marketing_leads_final_v2

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("/home/ubuntu/Crons/business-reporting-297220-1bd8456f1dda.json", scope)
client = gspread.authorize(creds)
sheet = client.open("Source_Mapping_Leads").sheet1  # Open the spreadhseet
data_source = sheet.get_all_records()  # Get a list of all records
Data_source = list(data_source)
df_Data_source = DataFrame(Data_source)
#df_Data_source
df_Data_source.drop(df_Data_source[df_Data_source['Data_Source'].isnull()].index, inplace = True)
#df_Data_source

# df_Data_source['Data_Source1'] = df_Data_source['Data_Source'].str.lower()
# df_Data_source.drop_duplicates(subset='Data_Source1', keep="first",inplace = True)


marketing_leads_final_v3 = pd.merge(marketing_leads_final_v2,df_Data_source,left_on=['Source_V1'], right_on = ['Data_Source'], how = 'left')
#marketing_leads_final_v3


marketing_leads_final_v4 = marketing_leads_final_v3[["leadId", "Mobile", "Lead_Source", "SRNumber", "InsStatus", "Disease_SR", "FormName", "Source_V1", "BDName_Current", "Enq_Date","Enq_Year", 
                                                        "Enq_Month", "Enq_Day", "Enq_hour", "Lead_Status", "EnqMonthCount", "SurgeryType", "Final_Category", "LeadMode", "callStatus", "city", "City_Initial", 
                                                        "City_leads", "URL_V2", "Team_Current_STD", "Disease_Final4", "Category_Current_STD", "Lead_Sub_Status", "OperationalStatus",'Final Mapped City','Slot Bracket','Final Source','Brand Confirmation','Enq_id']]

marketing_leads_final_v4["Final_Source"] = np.where(marketing_leads_final_v4['Final Source'].isnull(),marketing_leads_final_v4['Source_V1'],marketing_leads_final_v4['Final Source'])

marketing_leads_final_v4["FC_DF4"] = marketing_leads_final_v4["Final_Category"].map(str) + "" + marketing_leads_final_v4["Disease_Final4"]
#marketing_leads_final_v4

cope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("/home/ubuntu/Crons/business-reporting-297220-1bd8456f1dda.json", scope)
client = gspread.authorize(creds)
sheet = client.open("Forms_Mapping_Leads").sheet1  # Open the spreadhseet
data_forms = sheet.get_all_records()  # Get a list of all records
Data_forms = list(data_forms)
df_Data_forms =DataFrame(Data_forms)
#df_Data_forms
df_Data_forms.drop(df_Data_forms[df_Data_forms['Super Category'].isnull()].index, inplace = True)
#df_Data_forms

df_Data_forms_v1 = df_Data_forms[['Code','Super Category']]
#df_Data_forms_v1

marketing_leads_final_v5 = pd.merge(marketing_leads_final_v4,df_Data_forms_v1,left_on=['FC_DF4'], right_on = ['Code'], how = 'left')
#marketing_leads_final_v5

marketing_leads_final_v5["Super_Category_A"] = np.where(marketing_leads_final_v5['Super Category'].isnull(),marketing_leads_final_v5["Disease_Final4"],marketing_leads_final_v5['Super Category'])

marketing_leads_final_v5["Super Category"] = np.where(marketing_leads_final_v5['Disease_SR'] == "Axillary Breast Liposuction",marketing_leads_final_v5['Disease_SR'],marketing_leads_final_v5['Super_Category_A'])

marketing_leads_final_v5.rename(columns = {'Super Category': 'Initial_Disease_First'}, inplace = True)
#marketing_leads_final_v5

marketing_leads_final_v6 = marketing_leads_final_v5[["leadId", "Mobile", "Lead_Source", "SRNumber", "InsStatus", "Disease_SR", "FormName", "Source_V1", "BDName_Current", "Enq_Date","Enq_Year", 
                                                        "Enq_Month", "Enq_Day", "Enq_hour", "Lead_Status", "EnqMonthCount", "SurgeryType", "Final_Category", "LeadMode", "callStatus", "city", "City_Initial", 
                                                        "City_leads", "URL_V2", "Team_Current_STD", "Disease_Final4", "Category_Current_STD", "Lead_Sub_Status", "OperationalStatus",'Final Mapped City','Slot Bracket','Final_Source','Initial_Disease_First','Brand Confirmation','Enq_id']]

marketing_leads_final_v6["Brand Confirmation"] = np.where(marketing_leads_final_v6['Brand Confirmation'].isnull(),'Non Brand',marketing_leads_final_v6['Brand Confirmation'])
#marketing_leads_final_v6
# marketing_leads_final_v6["FC_DSR"] = marketing_leads_final_v6["Disease_SR"].map(str) + "" + marketing_leads_final_v6["Final_Category"]
# marketing_leads_final_v6["FC_CCS"] = marketing_leads_final_v6["Disease_SR"].map(str) + "" + marketing_leads_final_v6["Category_Current_STD"]
# marketing_leads_final_v6

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("/home/ubuntu/Crons/business-reporting-297220-1bd8456f1dda.json", scope)
client = gspread.authorize(creds)
sheet = client.open("Category_Mapping_Leads").sheet1  # Open the spreadhseet
data_Category = sheet.get_all_records()  # Get a list of all records
Data_Category = list(data_Category)
#Data_Category
df_Data_Category =DataFrame(Data_Category)
#df_Data_Category

df_Data_Category.drop(df_Data_Category[df_Data_Category['Final Category'].isnull()].index, inplace = True)
#df_Data_Category

marketing_leads_final_v7 = pd.merge(marketing_leads_final_v6,df_Data_Category,left_on=['Final_Category'], right_on = ['Final Category'], how = 'left')
#marketing_leads_final_v7
##########
marketing_leads_final_v7["Cateee"] = np.where(marketing_leads_final_v7['Cateee'].isnull(),marketing_leads_final_v7["Final_Category"],marketing_leads_final_v7['Cateee'])
marketing_leads_final_v7
marketing_leads_final_v7["X1"] = "abc"
marketing_leads_final_v7["Slots 2"] = "abc"
marketing_leads_final_v7["Flaggg"] = "abc"
marketing_leads_final_v7["Others Flag"] = "abc"

marketing_leads_final_v7["Initial_Disease_First"] = np.where(marketing_leads_final_v7["Initial_Disease_First"].isnull(),"Others",marketing_leads_final_v7["Initial_Disease_First"])

marketing_leads_final_v7["Final Disease Last"] = marketing_leads_final_v7["Initial_Disease_First"]
marketing_leads_final_v7["Final Disease"] = marketing_leads_final_v7["Initial_Disease_First"]

marketing_leads_final_v7["Last Final City"] = np.where(marketing_leads_final_v7["Final Mapped City"].isnull(),"Others",marketing_leads_final_v7["Final Mapped City"])

marketing_leads_final_v8 = marketing_leads_final_v7[["leadId", "Mobile", "Lead_Source", "SRNumber", "InsStatus", "Disease_SR", "FormName", "Source_V1", "BDName_Current", "Enq_Date","Enq_Year", 
                                                        "Enq_Month", "Enq_Day", "Enq_hour", "Lead_Status", "EnqMonthCount", "SurgeryType", "Final_Category", "LeadMode", "callStatus", "city", "City_Initial", 
                                                        "City_leads", "URL_V2", "Team_Current_STD", "Disease_Final4", "Category_Current_STD", "Lead_Sub_Status", "OperationalStatus",'X1', 'Final Mapped City','Last Final City',
                                                         'Slot Bracket','Final_Source','Final Disease', 'Cateee','Initial_Disease_First','Brand Confirmation','Slots 2','Flaggg','Others Flag','Final Disease Last','Enq_id']]

marketing_leads_final_v8.rename(columns = {'Disease_Final4': 'Disease_Final'}, inplace = True)
#marketing_leads_final_v8

#############2

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("/home/ubuntu/Crons/business-reporting-297220-1bd8456f1dda.json", scope)
client = gspread.authorize(creds)
sheet = client.open("ENT_Mapping_Leads_SR").sheet1  # Open the spreadhseet
data_ENT_sr = sheet.get_all_records()  # Get a list of all records
Data_ENT_sr = list(data_ENT_sr)
#Data_ENT_sr

df_Data_ENT_sr =DataFrame(Data_ENT_sr)
#df_Data_ENT_sr
df_Data_ENT_sr['SRENT'] = df_Data_ENT_sr['SRENT'].astype(str)

df_Data_ENT_sr.drop(df_Data_ENT_sr[df_Data_ENT_sr['SRENT'].isnull()].index, inplace = True)
#df_Data_ENT_sr

marketing_leads_final_v9 = pd.merge(marketing_leads_final_v8,df_Data_ENT_sr,left_on=['SRNumber'], right_on = ['SRENT'], how = 'left')
#marketing_leads_final_v9

###########3

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("/home/ubuntu/Crons/business-reporting-297220-1bd8456f1dda.json", scope)
client = gspread.authorize(creds)
sheet = client.open("ENT_Mapping_Leads_Forms").sheet1  # Open the spreadhseet
data_ENT_leads = sheet.get_all_records()  # Get a list of all records
Data_ENT_leads = list(data_ENT_leads)
df_Data_ENT_leads =DataFrame(Data_ENT_leads)
#df_Data_ENT_leads
df_Data_ENT_leads.drop(df_Data_ENT_leads[df_Data_ENT_leads['Source marketing'].isnull()].index, inplace = True)
#df_Data_ENT_leads

marketing_leads_final_v10 = pd.merge(marketing_leads_final_v9,df_Data_ENT_leads,left_on=['Final_Source'], right_on = ['Source marketing'], how = 'left')
#marketing_leads_final_v10

marketing_leads_final_v10["FinalENT"] = np.where(marketing_leads_final_v10['ENT1'].isnull(),marketing_leads_final_v10['ENT2'],marketing_leads_final_v10['ENT1'])
#marketing_leads_final_v10

############4

# MTPList=["MTP","Abortion","Bartholin's Cyst","Tubectomy","D&E","Medical Termination of Pregnancy (MTP)","Dilation and Curettage"]
# SurList=["Hysterectomy","Myomectomy","Ovarian Cyst","Hysteroscopy","Fibroids"]

marketing_leads_final_v10["Category_all_Spllt"] =   np.where((marketing_leads_final_v10['Cateee']=="Aesthetics")&(marketing_leads_final_v10['Final Disease Last'].str.contains("breast",na=False, case=False)),"Breast",
                                                    np.where((marketing_leads_final_v10['Cateee']=="Aesthetics")&(marketing_leads_final_v10['Final Disease Last']=="Hair Transplant"),"Hair Transplant",
                                                    np.where((marketing_leads_final_v10['Cateee']=="Ophthal")&(marketing_leads_final_v10['Final Disease Last'].str.contains("LASIK",na=False, case=False)),"Lasik",
                                                    np.where((marketing_leads_final_v10['Cateee']=="Ophthal")&(marketing_leads_final_v10['Final Disease Last'].str.contains("Cataract",na=False, case=False)),"Cataract",
                                                    np.where(marketing_leads_final_v10['Cateee']=="ENT",marketing_leads_final_v10['FinalENT'],marketing_leads_final_v10['Cateee'])))))

marketing_leads_final_v11 = marketing_leads_final_v10[["leadId", "Mobile", "Lead_Source", "SRNumber", "InsStatus", "Disease_SR", "FormName", "Source_V1", "BDName_Current", "Enq_Date","Enq_Year", 
                                                "Enq_Month", "Enq_Day", "Enq_hour", "Lead_Status", "EnqMonthCount", "SurgeryType", "Final_Category", "LeadMode", "callStatus", "city", "City_Initial", 
                                            "City_leads", "URL_V2", "Team_Current_STD", "Disease_Final", "Category_Current_STD", "Lead_Sub_Status", "OperationalStatus",'X1', 'Final Mapped City','Last Final City',
                                        'Slot Bracket','Final_Source','Final Disease', 'Cateee','Initial_Disease_First','Brand Confirmation','Slots 2','Flaggg','Others Flag','Final Disease Last','Category_all_Spllt','Enq_id']]

marketing_leads_final_v11["OperationalStatus"] = np.where(marketing_leads_final_v11['OperationalStatus'].isnull(),"Operational",marketing_leads_final_v11['OperationalStatus'])

marketing_leads_final_v11.rename(columns = {'Final_Source': 'Source'}, inplace = True)
marketing_leads_final_v11.rename(columns = {'Last Final City': 'Last Final City_Standardized'}, inplace = True)

marketing_leads_final_v11.rename(columns = {'Category_all_Spllt': 'Category all Spllt'}, inplace = True)
marketing_leads_final_v11['Category all Spllt'] = np.where(marketing_leads_final_v11['Category all Spllt']=="General Gynaecology","General Gynae OPD",marketing_leads_final_v11['Category all Spllt'])
marketing_leads_final_v11['Category all Spllt'] = np.where(marketing_leads_final_v11['SurgeryType']=="ENLARGED PROSTATE (BPH)","BPH",
                                                           np.where(marketing_leads_final_v11['SurgeryType']=="Kidney Stone BPH TURP","BPH",np.where(marketing_leads_final_v11['SurgeryType']=="BPH Others","BPH",marketing_leads_final_v11['Category all Spllt'])))

marketing_leads_final_v11['Category all Spllt'] = np.where(((marketing_leads_final_v11['Category all Spllt']=="Ophthal")|
                                                              (marketing_leads_final_v11['Category all Spllt']=="Ophthal_Tertiary")|
                                                                    (marketing_leads_final_v11['Category all Spllt']=="T3_Ophthal")),"Cataract",marketing_leads_final_v11['Category all Spllt'])
#marketing_leads_final_v11
marketing_leads_final_v11['Category_Current_STD'] = np.where(marketing_leads_final_v11['Category_Current_STD'].isnull(),"Others",marketing_leads_final_v11['Category_Current_STD'])
marketing_leads_final_v11["Category all Spllt"] = np.where((marketing_leads_final_v11["Category all Spllt"].isnull()) & (marketing_leads_final_v11["Cateee"]== 'ENT'),'Non Surgical ENT',marketing_leads_final_v11["Category all Spllt"])                     

marketing_leads_final_v11.drop_duplicates(['Enq_id'], keep="first",inplace = True)

PT = ["Delivery-Normal",
"Delivery-Instrumental Delivery",
"Delivery-C Section",
"ANC",
"Pregnancy",
"Delivery",
"Pregnancy (Obstetrics)",
"Antenatal Care",
"Normal Delivery"]

marketing_leads_final_v11['Category all Spllt'] = np.where(marketing_leads_final_v11["SurgeryType"].isin(PT),"ANC",marketing_leads_final_v11["Category all Spllt"])
# marketing_leads_final_v11

marketing_leads_final_vV11 = marketing_leads_final_v11[["Enq_id","leadId", "Mobile", "Lead_Source", "SRNumber", 
                                                        "InsStatus", "Disease_SR", "FormName", "Source_V1", 
                                                        "BDName_Current", "Enq_Date", "Enq_Year", "Enq_Month", 
                                                        "Enq_Day", "Enq_hour", "Lead_Status", "EnqMonthCount", 
                                                        "SurgeryType", "Final_Category", "LeadMode", "callStatus", 
                                                        "city", "City_Initial", "City_leads", "URL_V2", "Team_Current_STD",
                                                        "Disease_Final", "Category_Current_STD", "Lead_Sub_Status", 
                                                        "OperationalStatus", "X1", "Final Mapped City", 
                                                        "Last Final City_Standardized", "Slot Bracket", "Source", 
                                                        "Final Disease", "Cateee", "Initial_Disease_First", 
                                                        "Brand Confirmation", "Slots 2", "Flaggg", "Others Flag", 
                                                        "Final Disease Last", "Category all Spllt"]]

seo={'SEO_Ops':'SEO','SEO_NonOps':'SEO','SEO_NonOPs':'SEO','SEO_OPs':'SEO'}
marketing_leads_final_vV11['Source_V1']=marketing_leads_final_vV11['Source_V1'].replace(seo)
marketing_leads_final_vV11['Source']=marketing_leads_final_vV11['Source'].replace(seo)


#below col removed

# marketing_leads_final_vV11['X1']="" 
# marketing_leads_final_vV11['Slots 2']="" 
# marketing_leads_final_vV11['Flaggg']=""
# marketing_leads_final_vV11['Others Flag']=""
# marketing_leads_final_vV11['SRNumber'] = marketing_leads_final_vV11['SRNumber'].astype(int)

marketing_leads_final_vV11 = marketing_leads_final_vV11.sort_values(by=['Enq_Date'], ascending=[True])

marketing_leads_final_vV11.to_csv('MARKETING_DAILY_REPORT_Test_GK.csv',index=False)
marketing_leads_final_vV11.shape


# # Summarizing and writing

# In[7]:


# Summarizing data
import pandas as pd

marketing_leads_final_vV11.fillna("NA", inplace=True)
# marketing_leads_final_vV11['lead count']=1
# marketing_leads_final_vV11

select_col = ['Enq_Date','Source','Lead_Source','Last Final City_Standardized',
              'Category_Current_STD','Team_Current_STD','Cateee',
              'Brand Confirmation','Final Disease Last']


data = marketing_leads_final_vV11[select_col]
data = data.sort_values(by=['Enq_Date'], ascending=[True])

#data.head()

#handling nUll date
data['Enq_Date'] = pd.to_datetime(data['Enq_Date'], format='%Y-%m-%d').dt.date
data['Enq_Date'].fillna('1900-01-01', inplace=True)

#grouping/ summarizing
df = data.groupby(['Enq_Date','Source','Lead_Source','Last Final City_Standardized',
              'Category_Current_STD','Team_Current_STD','Cateee',
              'Brand Confirmation','Final Disease Last']).size().reset_index(name='Lead_Count')

df = df.sort_values(by=['Enq_Date', 'Lead_Count'], ascending=[True, False])

# Reset the index
df = df.reset_index(drop=True)

# df2 = data.groupby(['Enq_Date','Source','Lead_Source','Last Final City_Standardized',
#               'Category_Current_STD','Team_Current_STD','Cateee',
#               'Brand Confirmation','Final Disease Last'])['Enq_id'].count()

# ddf = data.groupby(['Enq_Date','Source','Lead_Source','Last Final City_Standardized',
#               'Category_Current_STD','Team_Current_STD','Cateee',
#               'Brand Confirmation','Final Disease Last'],as_index=True).agg(['count'],margins=True)

# .reset_index(name='Lead_Count')

# df2=df2.to_frame()
ddf=df

#-----------------------------------------------Summary Writing-------------------------------------------------------
#!pip install gspread-dataframe
from datetime import datetime, timedelta
from datetime import date
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/spreadsheets']
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/ubuntu/Crons/business-reporting-297220-1bd8456f1dda.json', scopes=scope)
gsheet = gspread.authorize(creds)

spr = gsheet.open_by_url('https://docs.google.com/spreadsheets/d/19S3VHNKyUqH5zSrZ6wtalpFqMvY45mzDR4H0pL53R1s/edit#gid=758768638')
wks = spr.worksheet('Summarised data')


sheet_range = "'Summarised data'!A:M"
spr.values_clear(sheet_range)
print(f'Data in range has been cleared.')

set_with_dataframe(wks, ddf)
print(f'Data from the DataFrame has been written to {sheet_range}.')


# # -------------------------Writing in batches-----------------------------------

# In[39]:


import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv

# Set your credentials and scope
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/ubuntu/Crons/business-reporting-297220-1bd8456f1dda.json', scope)

# Authorize the client
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)

# Define the Google Sheets details
spreadsheetId = '19S3VHNKyUqH5zSrZ6wtalpFqMvY45mzDR4H0pL53R1s'  # Please set your spreadsheet ID.
sheetName = "Raw" # Please set the sheet name you want to put the CSV data.

# Define the CSV file details
csvFile = 'MARKETING_DAILY_REPORT_Test_GK.csv'  # Please set the filename and path of the CSV file.

# Open the Google Sheet
sh = client.open_by_key(spreadsheetId)

# Clear data in the specified range
sh.values_clear(f"'{sheetName}'!A:AR")
print("Data cleared")

# Read the CSV file in batches
batch_size = 50000  # Define your batch size (e.g., 10000 rows per batch)
start_row=2

# Read the CSV file and write in a batch
with open(csvFile, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    batch = []
    sheet_index = 1  # Initialize the sheet index
    sheetName = f"Raw!A{sheet_index}"  # Initialize the sheetName
    
    for row in csv_reader:
        batch.append(row)
        if len(batch) >= batch_size:
            sh.values_update(
                sheetName,
                params={'valueInputOption': 'USER_ENTERED'},
                body={'values': batch},
            )
            print(f"Batch of {batch_size} rows written starting from row {start_row}")
            start_row += batch_size
            sheet_index += batch_size  # Increment the sheet index
            sheetName = f"Raw!A{sheet_index}"  # Update the sheetName
            batch = []  # Reset batch
    if batch:
        sh.values_update(
            sheetName,
            params={'valueInputOption': 'USER_ENTERED'},
            body={'values': batch},
        )
        print(f"Remaining rows written starting from row {start_row}")

print("All rows written")

#-------------------Email----------------

import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from datetime import datetime, timedelta
from datetime import date


today=date.today()
yesterday = today- timedelta(days=1)

subject = "[Cron runed] Leads Data and Summary updated in 7Day Gsheet till "+str(yesterday)

body = "Hi,\n \n Leads Data and Summary updated in 7Day Gsheet till "+str(yesterday)+"\n Link: \n https://docs.google.com/spreadsheets/d/19S3VHNKyUqH5zSrZ6wtalpFqMvY45mzDR4H0pL53R1s/edit?pli=1#gid=758768638"

sender_email = ["marketing.reports@pristyncare.com"]
receiver_email = ["gaurav.kumar2@pristyncare.com","atiullah.ansari@pristyncare.com","deepinder.singh@pristyncare.com","yogesh@pristyncare.com","saurabh.garg@pristyncare.com"]
# cc_email = ["gagan.arora@pristyncare.com","himanshu.jindal@pristyncare.com","gaurav.kumar2@pristyncare.com"]

password = "Target@7000" #m

email = MIMEMultipart()
email["From"] = ', '.join(sender_email)
email["To"] =  ', '.join(receiver_email)
# email["CC"] = ', '.join(cc_email)
email["Subject"] = subject


email.attach(MIMEText(body,"plain"))
bodytext=""

session = smtplib.SMTP('smtp.gmail.com', 587) 
session.starttls() 
session.login(sender_email[0],password) 
text = email.as_string()
session.sendmail(sender_email, receiver_email,text)
#                      +cc_email,text)
print("mail sent")