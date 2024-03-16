#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Gsheet Connection
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
start_time = datetime.now()
# define the scope
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
# add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/ubuntu/GK-DM/Crons/syed-reporting-83428b4082ce.json', scope)
# authorize the clientsheet 
client = gspread.authorize(creds)
print("done")


# In[2]:


#one sheet at a time - Accessing sheet
#2022
# Mar_22="https://docs.google.com/spreadsheets/d/1yvfHcTxegb0cJ4gNZX5Xfb5zM7U4DKGNwKm6MPTQODU/edit#gid=0"
# Apr_22="https://docs.google.com/spreadsheets/d/1Q00wwj9w33S3PKbPGLWbH1gXuY-PCK8_hFNbmJqjJQo/edit#gid=0"
# May_22="https://docs.google.com/spreadsheets/d/1n5-_zK5ag6C72CLR36BPwNzfLOq4lqjZx7j6JY_LFpw/edit#gid=0"
# Jun_22="https://docs.google.com/spreadsheets/d/1_qdEuq4npICvy7HzWcVYN74E-DQzMXLAzTTKJ8ThZ0A/edit#gid=0"
# Jul_22="https://docs.google.com/spreadsheets/d/1XLdERfh1g1FjpcJBCCQiNC71dzGsXBS9M8sudEb10do/edit#gid=0"
# Aug_22="https://docs.google.com/spreadsheets/d/1erbfzBKp8GMccOVL5nQw1FyoMd6WZp2q0powOAW7LhQ/edit#gid=0"
# Sep_22="https://docs.google.com/spreadsheets/d/1YoOfD8EmZ5xJGlb__iEEaXt0Kpega4czW8LLg7-7ics/edit#gid=0"
# Oct_22="https://docs.google.com/spreadsheets/d/1HDhYzGzPQ9SCzovURwuLZCe-GokVOkKdydumxdpe8nc/edit#gid=0"
# Nov_22="https://docs.google.com/spreadsheets/d/1P4aYJGYHIqZKjKNwnC0pPwVq-J156JXuJVcGLlqRaGY/edit#gid=0"
# Dec_22="https://docs.google.com/spreadsheets/d/1fDO5sQjO4ooUGsNjUFO6I5Ch47Ll6NL3zK4bEuOnyBA/edit"

#2023
# Jan_23="https://docs.google.com/spreadsheets/d/1F-0_qOJ9Ft37_kqZKhGZxcodsM32eQgmBS1S92Tpbgs/edit?usp=drive_web&ouid=106945881853607543429"
# Feb_23="https://docs.google.com/spreadsheets/d/1J9TTNnJ2dN7HnBQ4nazZkU9nt5LUeh2r9IfE6L15A_Q/edit#gid=0"
# Mar_23="https://docs.google.com/spreadsheets/d/1qH6yqKrRGJeIG8RJJuD89fhzgvR4cY7zaK0tRqib-gs/edit#gid=0"
# Apr_23="https://docs.google.com/spreadsheets/d/1m1GBWpgu6kMKpwwsq3HhmwYrFk9LDQ_rRWWFuByDiIg/edit#gid=0"
# May_23="https://docs.google.com/spreadsheets/d/1F0I0Em9TrYtSHHlct0wykeHs2yzIfQnVH81abiY0JfY/edit#gid=0"
# Jun_23="https://docs.google.com/spreadsheets/d/1pN98vDpllc9WDRWwDNijJgwt5JaBKrcTHw51wKy7mNU/edit#gid=0"
# Jul_23="https://docs.google.com/spreadsheets/d/1CzHzYlx1McI4nUM7zb80jlbQMsZhJRHEGewAIm07Pno/edit#gid=0"
# Aug_23="https://docs.google.com/spreadsheets/d/1u_rdomNn9ezqdZkvmGZ_Pm-chTPnG8IDoL71NMA1igI/edit#gid=0"
# Sep_23="https://docs.google.com/spreadsheets/d/1zECwdLyK2gQC0YVfrjD9zRc_ylNkk3s93zoLu6-yW_g/edit#gid=0"
Curr="https://docs.google.com/spreadsheets/d/1DF9G7Td_v-fxi1_P87InccJeYFGD7Jk243CsGfr_dEU/edit#gid=0"

sheet = client.open_by_url(Curr)
wks = sheet.worksheet('Appts')

#getting record
rc = wks.get_all_records()

#converting dict to data frame
gs = pd.DataFrame.from_dict(rc)

gs.head()


# In[3]:


#renaming headers
gs.rename(columns={'leadid_Leads':'leadid','Source Type':'Source_Type','Final Category':'Final_Category'},inplace=True)
# gs.rename(columns={'x':'callStatus_Enq'},inplace=True)

#removing blank and NAN
gs=gs.dropna(subset=['AppointmentStartTime_V2_IST'])
gs = gs[gs['AppointmentStartTime_V2_IST'].astype(str).str.strip() != '']


# In[4]:


# selecting headers
gs['DocName']="-"
gs['category_Initial']="Column Removed from 20dec23"
select_col=['AppointmentStartTime_V2_IST','AppointmentType','Appt_id','appointmentStatus','opdType','SurgeryType',
         'DoctorStatus','doctorsurgeryStatus','Appt_Created_at_V2_IST','PaymentMode','leadid','Lead_Source','SRNumber_Enq',
         'FormName','URL','FullURL','leadCampaignName','callStatus_Enq','Enq_created_at_Enq_IST','Source_V1',
         'Current_Category_Leads','Current_Team_leads','Patient_PaymentType','Insurance_AssignmentStatus','category_Initial',
         'concernedTeam_Initial','BDEMail','First_Created_At','City','DocName','Doccity','Hospital_Name','DocAssigned_2',
         'doctorsurgeryStatus1','PatientName','Final_Category','online_offline','TotalAmount','OPD_Booked_Flag','OPD_Flag',
         'IPD_Flag','Source','Source_Type','CityFinal','Lead_Mode','ApptCreDate']
final=gs[select_col]

final['ApptCreDate']=pd.to_datetime(final['ApptCreDate']).dt.date
final['ApptStartDate']=pd.to_datetime(final['AppointmentStartTime_V2_IST']).dt.date
#final.head()


# In[5]:


from py_topping.data_connection.database import lazy_SQL
mysql = lazy_SQL(sql_type = 'mysql',
                 host_name = 'marketing-db.cmwukub0eama.ap-south-1.rds.amazonaws.com',
                 database_name="DigitalMarketing", 
                 user="digital_market",
                 password="digital")

mysql.dump_replace(final, 'Appts', list_key = ["Appt_id"])


# In[7]:


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

subject = "[Cron runed] Appts Table Updated in SQL till "+str(yesterday)

body = "Hi,\n \n Appts Table Updated in SQL till "+str(yesterday)+"\n"

sender_email = ["marketing.reports@pristyncare.com"]
receiver_email = ["deepinder.singh@pristyncare.com","yogesh@pristyncare.com","gaurav.kumar2@pristyncare.com","atiullah.ansari@pristyncare.com"]
# cc_email = ["gagan.arora@pristyncare.com","himanshu.jindal@pristyncare.com","gaurav.kumar2@pristyncare.com"]

password = "Target@7000" #m

email = MIMEMultipart()
email["From"] = ', '.join(sender_email)
email["To"] =  ', '.join(receiver_email)
# email["CC"] = ', '.join(cc_email)
email["Subject"] = subject


email.attach(MIMEText(body,"plain"))
bodytext="Appts Table Updated"

session = smtplib.SMTP('smtp.gmail.com', 587) 
session.starttls() 
session.login(sender_email[0],password) 
text = email.as_string()
session.sendmail(sender_email, receiver_email,text)
#                      +cc_email,text)
print("mail sent")

