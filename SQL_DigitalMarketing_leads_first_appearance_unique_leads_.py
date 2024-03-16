from datetime import datetime, timedelta
from datetime import date
import pandas as pd
import numpy as np
import pandas.io.sql as psql
import mysql.connector
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv
import time
from IPython.display import clear_output
import traceback
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os

sender_email = ["marketing.reports@pristyncare.com"]
receiver_email = ["gaurav.kumar2@pristyncare.com","somesh.kumar@pristyncare.com",
#                           "deepinder.singh@pristyncare.com",
#                           "yogesh@pristyncare.com","saurabh.garg@pristyncare.com",
                  "deepak.tyagi@pristyncare.com",
                  "vasi.rahman@pristyncare.com"]
password = "Target@7000"

try: 
    today=date.today()
    yesterday = today- timedelta(days=1)
#     print(today,yesterday)

    # startdate = date(2019,6,10)
    # startdate = date(2024,2,20)
    # enddate = date(2024,2,22)
    startdate=today- timedelta(days=3)
    enddate=yesterday

    mydb = mysql.connector.connect(
      host="marketing-db.cmwukub0eama.ap-south-1.rds.amazonaws.com",
      user="admin",
      password="marketing",
      database="marketingDashboard"
    )
    mycursor = mydb.cursor()
    conn = mycursor.execute

    middate = startdate
    while middate <= enddate:
        print(middate)
        temp_date = middate + timedelta(days=7)
        print(temp_date)
        dff = pd.read_sql_query(f'''

        select Created_at_IST,Created_at_IST_Date,first_appearance_date as first_appearance,
               date(first_appearance_date) as first_appearance_date,enquiry_id,leadId,mobile_number,source,
               lead_source,disease_leads,category_final,team_final,city,SR_Number,full_url,sevenDayUniqueSQL,
               thirty_day_unique_flag,Lead_Mode
        from
        (SELECT
            Created_at_IST,Created_at_IST_Date,enquiry_id,leadId,mobile_number,source,
            lead_source,disease_leads,category_final,team_final,sr_number as SR_Number,full_url,sevenDayUniqueSQL,
            thirty_day_unique_flag,
            CASE 
            WHEN city IS NULL THEN city_leads
            WHEN city_leads IS NULL THEN city_marketing
            ELSE city end as city,
            case when lead_source="Call" then "Call" else "Form" end as Lead_Mode
        FROM
            marketingDashboard.enquiry_table AS main_table

        WHERE
            Created_at_IST_Date between '{middate}' and '{temp_date}'
            and thirty_day_unique_flag = 1
            and isResurfaced is null
            and leadId IS NOT NULL
            and enquiry_id IS NOT NULL
            and enquiry_id not in (
                                    select enquiry_id
                                    from marketingDashboard.enquiry_table
                                    where city in ("International","Kathmandu","Comilla","Natore","Rajshahi",
                                    "Wsqy","T5 Farby","T4 Farby","T3 Farby","T2 Farby","Sylhet","Rajshahi",
                                    "Pinjore","Ottawa","Not Defined","Natore","Nairobi","Metro Farby","Matigara",
                                    "Kuwait city","Khulna","Kathmandu","Islamabad","International",
                                    "Internatinonal","Firojpur","Cox's Bazar","Comilla","Chirala","Central",
                                    "Abuja","Abidjan","0")
                                    OR team_final in("MD","PXBD","instituionalSales","Pharma","TVC","App Install",
                                    "Covid-19","General","Diagnostic","Central","test","COVID","CentalBD","Offline_Business",
                                    "satya_test","CentralBD","Other","instituionalSales","Offline_Business","Diagnostic",
                                    "instituionalSales_1","Covid","General Physician","PL","Supply","MedicalSerice",
                                    "Ayurveda",Null)
                                     or category_final in ("Ayurveda","General","Covid-19","Online Consulting",
                                    "Central","Array","Offline","Please Select","General Physician",Null)
                                    or mobile_number in ("None","none","NAN","nan","",Null)
                                    )
        ) all_leads
        left join 
        (
        select min(Created_at_IST) as first_appearance_date ,mobile_number as mobile
        from marketingDashboard.enquiry_table
        where thirty_day_unique_flag = 1
            and isResurfaced is null
            and enquiry_id not in (
                                    select enquiry_id
                                    from marketingDashboard.enquiry_table
                                    where city in ("International","Kathmandu","Comilla","Natore","Rajshahi",
                                    "Wsqy","T5 Farby","T4 Farby","T3 Farby","T2 Farby","Sylhet","Rajshahi",
                                    "Pinjore","Ottawa","Not Defined","Natore","Nairobi","Metro Farby","Matigara",
                                    "Kuwait city","Khulna","Kathmandu","Islamabad","International",
                                    "Internatinonal","Firojpur","Cox's Bazar","Comilla","Chirala","Central",
                                    "Abuja","Abidjan","0")
                                    OR team_final in("MD","PXBD","instituionalSales","Pharma","TVC","App Install",
                                    "Covid-19","General","Diagnostic","Central","test","COVID","CentalBD","Offline_Business",
                                    "satya_test","CentralBD","Other","instituionalSales","Offline_Business","Diagnostic",
                                    "instituionalSales_1","Covid","General Physician","PL","Supply","MedicalSerice",
                                    "Ayurveda",Null)
                                     or category_final in ("Ayurveda","General","Covid-19","Online Consulting",
                                    "Central","Array","Offline","Please Select","General Physician",Null)
                                    or mobile_number in ("None","none","NAN","nan","",Null)
                                    )
        Group BY mobile_number
        ) first_app
        on all_leads.mobile_number=first_app.mobile

        ''',mydb)
        print("SQL query - done")
        # dff

        dff.dropna(subset=['mobile_number'],inplace=True)    
        dff['Created_at_IST_Date']=pd.to_datetime(dff['Created_at_IST_Date']).dt.date
        dff=dff[dff['Created_at_IST_Date']<today]
        # dff

        dff['CM']=pd.to_datetime(dff['Created_at_IST_Date']).dt.month
        dff['CY']=pd.to_datetime(dff['Created_at_IST_Date']).dt.year

        dff['FM']=pd.to_datetime(dff['first_appearance_date']).dt.month
        dff['FY']=pd.to_datetime(dff['first_appearance_date']).dt.year

        dff['Unique_Flag']=np.where((dff['CM']==dff['FM'])&
                                    (dff['CY']==dff['FY']),1,0)

        dff.drop(columns=['FM','FY','CM','CY'],axis=1,inplace=True)

        # dff

        source_mapping={'SEO_OPs':'SEO','SEO_NonOPs':'SEO','SEO_Ops':'SEO','SEO_NonOps':'SEO',
                        'FB_LongForm':'Facebook','FB_Remarketing':'Facebook','FB_Leadgen_Remarketing':'Facebook',
                        'FB_Comment':'Facebook','Fb_Comment':'Facebook','Fb_Leadgen':'Facebook','FB_Longform':'Facebook',
                        'Comms-Brand':'Comms','Comms-Whatsapp':'Comms','Comms_WhastApp':'Comms',
                        'CLI_Inbound':'Google','BookAppointment(Google)':'Google','Google Search':'Google','LeadSourceAMP':'Google',
                        'LeadPages':'Google','BookAppointment':'Google','BookAppointment_Google':'Google','google':'Google',
                        'websitefrom':'Google','bookappointment(google)':'Google','google search':'Google','leadsourceamp':'Google',
                        'leadpages':'Google','bookappointment':'Google','bookappointment_google':'Google','cli_inbound':'Google'}

        dff['source']=dff['source'].replace(source_mapping)
        # dff


        dff['first_appearance_date']=pd.to_datetime(dff['first_appearance_date'])
        dff['Created_at_IST_Date']=pd.to_datetime(dff['Created_at_IST_Date'])

        dff['Age_in_months']=(round((dff['Created_at_IST_Date']-dff['first_appearance_date'])/np.timedelta64(1,'M'),0)+1).astype(int)

        print("Final df ready")

        ###########################################################################
        from py_topping.data_connection.database import lazy_SQL
        mysql = lazy_SQL(sql_type = 'mysql',
                         host_name = 'marketing-db.cmwukub0eama.ap-south-1.rds.amazonaws.com',
                         database_name="DigitalMarketing", 
                         user="digital_market",
                         password="digital")
        print("Writing in DB")

        mysql.dump_replace(dff, 'leads_first_appearance', list_key = ["enquiry_id"])
        print("Record Done")
        time.sleep(3)
        clear_output()
        middate = temp_date + timedelta(days=1)
    #   print(startdate)
        
        

except Exception as e:
    
    error=traceback.format_exc() #to store error
    traceback.print_exc() # to pring in runtime
    subject = "⚠️[Alert !] SQL_DigitalMarketing_leads_first_appearance_unique_leads Failed to run "
    body = f"Hi,\n SQL_DigitalMarketing_leads_first_appearance_unique_leads Report Failed to run because :\n\n {error} "+"\n"+"\n link: \n http://localhost:8887/notebooks/GK-DM/Reports/Regular%20reports/SQL_DigitalMarketing_leads_first_appearance_unique_leads.ipynb#"
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
    print("Alert mail sent")
    
else:
    
    subject = "[Cron runed] SQL_DigitalMarketing_leads_first_appearance_unique_leads "+str(yesterday)
    body = "Hi,\n SQL_DigitalMarketing_leads_first_appearance_unique_leads till "+str(yesterday)
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
    print("Success mail sent")