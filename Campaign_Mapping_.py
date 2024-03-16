#!/usr/bin/env python
# coding: utf-8

# In[95]:


import csv
import pandas as pd
import gspread
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
# from datetime import datetime, timedelta, date

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("/home/ubuntu/GK-DM/Crons/syed-reporting-83428b4082ce.json", scope)
client = gspread.authorize(creds)
# client


# In[96]:


def read_Gsheet_from_specific_range_to_df(spreadsheetId,sheetname,sheetrange):
    
    spreads = client.open_by_key(spreadsheetId)
    worksheet = spreads.worksheet(sheetname)
    data = worksheet.get(sheetrange)
    return data
    print(f'Data has been read from range {sheetrange} in sheet {spreads}')
#--------------------------------------------------------------------------------------------------------------------------
def write_to_Gsheet(spreadsheetId,sheetrange,csvFile,Sheet_name,Metric="Data"):
    
    sh = client.open_by_key(spreadsheetId)
    sh.values_clear(sheetrange) 
    print(f'{Metric} is cleared from {sheetrange} in {sh} ')
    
    sh.values_update(sheetrange,
                     params={'valueInputOption': 'USER_ENTERED'},
                     body={'values': list(csv.reader(open(csvFile)))})
    print(f'{Metric} is written to {sheetrange} in {sh}')
    
    sh.worksheet(Sheet_name).add_rows(1) # adding extra row in sheet
    print(f"One blank Row Appended in {Sheet_name}")


# In[97]:


Metric="Mapping"

Mapping=read_Gsheet_from_specific_range_to_df(spreadsheetId="1GrdFV2d6CWFPvB-rmSZBzYRfKTAGZlWFo9U4nFTbpy8",
                                           sheetname="Index",sheetrange="A:I")

Mapping=pd.DataFrame(Mapping[1:], columns=Mapping[0])
Mapping = Mapping[Mapping['Campaign Name'].astype(str).str.strip() != '']

Mapping['Index']=Mapping.index
Mapping


# In[98]:


# Mapping['Category'] = Mapping['Category'].str.replace(" ", "-")
# columns_to_replace = ['Category', 'City','Tertiary/Metro','Disease','Sub Category','Sub Sub Category']
# Mapping[columns_to_replace] = Mapping[columns_to_replace].apply(lambda x: x.str.replace(" ", "-"))

Category=list(Mapping['Category'].unique())

City=list(Mapping['City'].unique())
elements_to_remove = ['-', 'None', 'PUne','',None,'Others','Metro','Tier 2','Tier 3','Tier 4','Tier 5']
City = [city for city in City if city not in elements_to_remove]

# Team=list(Mapping['Tertiary/Metro'].unique())
# elements_to_remove = ['-', 'None', 'Any','',None,'Others',"Covid","international"]
# Team = [x for x in Team if x not in elements_to_remove]

Disease=list(Mapping['Disease'].unique())
elements_to_remove = ['-', 'None', 'Any','',None,'Others',"Covid","international","Ortho"]
Disease = [x for x in Disease if x not in elements_to_remove]

Final_Category=list(Mapping['Sub Category'].unique())
elements_to_remove = ['-', 'None', 'Any','',None,'Others',"Covid","international"]
Final_Category = [x for x in Final_Category if x not in elements_to_remove]

Final_Sub_Category=list(Mapping['Sub Sub Category'].unique())
elements_to_remove = ['-', 'None', 'Any','',None,'Others',"Covid","international","Ortho"]
Final_Sub_Category = [x for x in Final_Sub_Category if x not in elements_to_remove]

Mapping=Mapping.sort_values(by='Index',ascending=False)
# Team_mapping=Mapping[['Category','City','Tertiary/Metro']].drop_duplicates()
#Logic Changed
Team_mapping=list(Mapping['Tertiary/Metro'].unique())
elements_to_remove = ['-', 'None', 'Any','',None,'Others',"Covid","international","Ortho"]
Team_mapping = [x for x in Team_mapping if x not in elements_to_remove]


# In[107]:


#Finding New Campaigns to Map
new_campaign_name=read_Gsheet_from_specific_range_to_df(spreadsheetId="1GrdFV2d6CWFPvB-rmSZBzYRfKTAGZlWFo9U4nFTbpy8",
                                           sheetname="Consolidated Spends",sheetrange="A:Q")

new_campaign_name=pd.DataFrame(new_campaign_name[1:], columns=new_campaign_name[0])
new_campaign_name = new_campaign_name[new_campaign_name['Campaign name'].astype(str).str.strip() != '']

new_campaign_name=new_campaign_name[new_campaign_name['sdt']=='#N/A'][['Campaign name']]

new_campaign_name['Campaign name_V2'] = new_campaign_name['Campaign name'].str.replace("-", " ")
new_campaign_name=new_campaign_name.drop_duplicates(subset=['Campaign name', 'Campaign name_V2'], keep='first')
#contains duplicate
print(len(new_campaign_name))
new_campaign_name


# In[108]:


# Function to check if category exists in Campaign name
def check_category(campaign_name,check_list):
    for ele in check_list:
        if ele and ele in campaign_name:
            return ele
    return 'Others'


# In[109]:


new_campaign_name['Category'] = new_campaign_name['Campaign name_V2'].apply(lambda x: check_category(x, Category))
### ADDED
new_campaign_name['Category']=np.where(new_campaign_name['Campaign name_V2'].str.contains("Ortho"),"Ortho",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("Procto"),"Proctology",
                                       new_campaign_name['Category']))

new_campaign_name['City'] = new_campaign_name['Campaign name_V2'].apply(lambda x: check_category(x, City))


#Logic Change
# new_campaign_name=new_campaign_name.merge(Team_mapping,on=['Category', 'City'], how='left')

# Added campaign condition for PL team
new_campaign_name['Tertiary/Metro'] = new_campaign_name['Campaign name_V2'].apply(lambda x: check_category(x, Team_mapping))
new_campaign_name['Tertiary/Metro'] = np.where(new_campaign_name['Campaign name_V2'].str.startswith("ET 5007"),"PL",
                                      np.where((new_campaign_name['Campaign name_V2'].str.contains("T2")&
                                                new_campaign_name['Tertiary/Metro'].str.contains("Others")),"Tier 2",
                                      np.where((new_campaign_name['Campaign name_V2'].str.contains("T3")&
                                                new_campaign_name['Tertiary/Metro'].str.contains("Others")),"Tier 3",
                                      np.where((new_campaign_name['Campaign name_V2'].str.contains("T4")&
                                                new_campaign_name['Tertiary/Metro'].str.contains("Others")),"Tier 4",
                                      np.where((new_campaign_name['Campaign name_V2'].str.contains("T5")&
                                                new_campaign_name['Tertiary/Metro'].str.contains("Others")),"Tier 5",
                                      np.where((new_campaign_name['City'] == "Others") & (new_campaign_name['Category'] != "Others"),
                                               "Metro", new_campaign_name['Tertiary/Metro']))))))
new_campaign_name['Tertiary/Metro']=new_campaign_name['Tertiary/Metro'].fillna("Others")

new_campaign_name['Disease'] = new_campaign_name['Campaign name_V2'].apply(lambda x: check_category(x, Disease))

new_campaign_name['Sub Category'] = new_campaign_name['Campaign name_V2'].apply(lambda x: check_category(x, Final_Category))
new_campaign_name['Sub Category'] = np.where(new_campaign_name['Campaign name_V2'].str.startswith("ET 5007"),"PL",
                                      np.where((new_campaign_name['Campaign name_V2'].str.contains("T2")&
                                                new_campaign_name['Sub Category'].str.contains("Others")),"Tier 2",
                                      np.where((new_campaign_name['Campaign name_V2'].str.contains("T3")&
                                                new_campaign_name['Sub Category'].str.contains("Others")),"Tier 3",
                                      np.where((new_campaign_name['Campaign name_V2'].str.contains("T4")&
                                                new_campaign_name['Sub Category'].str.contains("Others")),"Tier 4",
                                      np.where((new_campaign_name['Campaign name_V2'].str.contains("T5")&
                                                new_campaign_name['Sub Category'].str.contains("Others")),"Tier 5",
                                      np.where((new_campaign_name['City'] == "Others") & (new_campaign_name['Category'] != "Others"),
                                               "Metro", new_campaign_name['Sub Category']))))))

new_campaign_name['Sub Sub Category'] = new_campaign_name['Campaign name_V2'].apply(lambda x: check_category(x, Final_Sub_Category))

new_campaign_name


# In[110]:


new_campaign_name['Source'] =  np.where((new_campaign_name['Campaign name_V2'].str.contains("Search")&
                                        new_campaign_name['Campaign name_V2'].str.contains("Brand")), "Brand Search",
                              np.where(new_campaign_name['Campaign name_V2'].str.startswith("ET "), "Adwords",
                              np.where(new_campaign_name['Campaign name_V2'].str.startswith("PC "), "Adwords",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("Remarketing"), "Fb Remarketing",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("Leadgen"), "Fb Leadgen",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("LongForm"), "Fb Long Form",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("Facebook"), "Facebook",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("Youtube"), "Youtube",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("Instagram"), "Instagram",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("App Install"), "App-Install",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("CTW"), "CTW_bot",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("Survey"), "Brand Survey",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("Messenger"), "Fb_Messenger",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("Whatsapp bot"), "Fb_Whatsapp_bot",
                              np.where(new_campaign_name['Campaign name_V2'].str.contains("Bookappointment"), "Fb Bookappointment",
                              "Others")))))))))))))))

new_campaign_name=new_campaign_name.drop(columns='Campaign name_V2')
# new_campaign_name=new_campaign_name.drop_duplicates(subset='Campaign name')

print(len(new_campaign_name))
new_campaign_name


# In[111]:


new_campaign_name['Brand Confirmation'] = np.where(new_campaign_name['Source']== "Adwords","Digital",
                                          np.where(new_campaign_name['Source']=="Fb Remarketing","Brand",
                                          np.where(new_campaign_name['Source']=="Fb Leadgen","Brand",
                                          np.where(new_campaign_name['Source']=="Fb Long Form","Brand",
                                          np.where(new_campaign_name['Source']=="Facebook","Brand",
                                          np.where(new_campaign_name['Source']=="Youtube","Brand",
                                          np.where(new_campaign_name['Source']=="Instagram","Brand",
                                          np.where(new_campaign_name['Source']=="App-Install","Brand",
                                          np.where(new_campaign_name['Source']=="CTW_bot","Digital",
                                          np.where(new_campaign_name['Source']=="Brand Survey","Brand Survey",
                                          np.where(new_campaign_name['Source']=="Fb_Messenger","Brand",
                                          np.where(new_campaign_name['Source']=="Fb_Whatsapp_bot","Brand",
                                          np.where(new_campaign_name['Source']=="Fb Bookappointment","Brand",
                                          np.where(new_campaign_name['Source']=="Brand Search","Brand Search",
                                          "Others"))))))))))))))
import datetime
new_campaign_name['Updated date']=datetime.datetime.now()
new_campaign_name.drop_duplicates(subset='Campaign name',keep='first',inplace=True)
new_campaign_name


# In[91]:


index=len(Mapping['Campaign Name'])+2
index


# In[92]:


if len(new_campaign_name)>0:
    
    csvfile="Spends_mapping.csv"
    Sheet_name="Index"
    new_campaign_name.to_csv(csvfile,index=False,header=False)

    write_to_Gsheet(spreadsheetId='1GrdFV2d6CWFPvB-rmSZBzYRfKTAGZlWFo9U4nFTbpy8',
                    sheetrange=f'{Sheet_name}!A{index}:J',csvFile=csvfile,Metric="Data",Sheet_name=Sheet_name)

    new_campaign_name['Brand Confirmation']=np.where(new_campaign_name['Brand Confirmation']=="Brand","Brand","")

    df=read_Gsheet_from_specific_range_to_df(spreadsheetId="1ilCFgjQPZEDR2WsTdtHQA4FRn0gMeGpVgwclONSq9jo",
                                               sheetname="Index",sheetrange="A:A")
    index=len(df)+1
    index

    csvfile="Spends_mapping_V2.csv"
    Sheet_name="Index"
    new_campaign_name.to_csv(csvfile,index=False,header=False)

    write_to_Gsheet(spreadsheetId='1ilCFgjQPZEDR2WsTdtHQA4FRn0gMeGpVgwclONSq9jo',
                    sheetrange=f'{Sheet_name}!A{index}:J',csvFile=csvfile,Metric="Data",Sheet_name=Sheet_name)
    import email, smtplib, ssl
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    import pandas as pd

    # Assuming new_campaign_name is your DataFrame
    styled_df = new_campaign_name.style.set_properties(**{
        'font-size': '12pt',  # Set font size for the entire DataFrame
        'border': '1px solid black',  # Add border to all cells
        'text-align': 'center'  # Align text to the center
    })

    # Convert the styled DataFrame to HTML
    html_content = """
    <html>
      <head>
        <style>
          /* Define custom styles */
          table {{
            border-collapse: collapse;
            width: 50%;
          }}
          th {{
            background-color: darkblue;
            color: white;
            padding: 5px;
            border-bottom: 1px solid #ddd;
            text-align: center;
          }}
          td {{
            padding: 5px;
            border-bottom: 1px solid #ddd;
            text-align: center;
          }}
          tr:nth-child(even) {{
            background-color: lightgrey;
          }}
        </style>
      </head>
      <body>
        <h3><< Below is/are the Campaign Mapped, Please check >></h3>
        {}
        <br></br>
        <button style="padding: 10px 5px 10px 10px;border: 2px solid Blue;background-color:Black;color:red;font-size:20px">
          <a href="https://docs.google.com/spreadsheets/d/1GrdFV2d6CWFPvB-rmSZBzYRfKTAGZlWFo9U4nFTbpy8/edit#gid=913395051">Marketing Spends Gsheet</a>
        </button>
      </body>
    </html>
    """.format(styled_df.render())

    # Prepare the email
    email = MIMEMultipart()
    email["From"] = "marketing.reports@pristyncare.com"
    receiver_email = ["gaurav.kumar2@pristyncare.com","marketing.reports@pristyncare.com","apeksha.adme@pristyncare.com","digital.marketing@pristyncare.com","digital_marketing-pri-aaaadfy75f2glae2asomtlt3ny@pristyn-care.slack.com"]
    # digital.marketing@pristyncare.com","digital_marketing-pri-aaaadfy75f2glae2asomtlt3ny@pristyn-care.slack.com"
    # email["To"] = ', '.join(receiver_email)
    email["Subject"] = "<< Campaign(s) Mapped >>"
    email.attach(MIMEText(html_content, "html"))

    # Send the email
    password = "Target@7000"
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login("marketing.reports@pristyncare.com", password)
        server.sendmail(email["From"], receiver_email, email.as_string())
    print("Mail Sent")

else: 
    print("No records")
