select city ,sum(lead_count),sum(OPD_Booked_Flag),sum(OPD_Flag),sum(IPD_Flag)
from
(select ul.first_appearance_dt,ul.enquiry_id,ul.leadId,ul.city as city,ul.source,ul.lead_source,ul.disease_leads,
       ul.category_final,ul.team_final,sum(lead_count) as lead_count,
       if(sum(app.OPD_Booked_Flag)>=1,1,0) as OPD_Booked_Flag,if(sum(app.OPD_Flag)>=1,1,0) as OPD_Flag,
       if(sum(app.IPD_Flag)>=1,1,0) as IPD_Flag
from 
(select min(Created_at_IST_date) as first_appearance_date,min(Created_at_IST) as first_appearance_dt,
        Created_at_IST,enquiry_id,leadId,city,source,lead_source,disease_leads,category_final,team_final,
       sum(sevenDayUniqueSQL) as lead_count
from marketingDashboard.enquiry_table 
where mobile_number in 
						(SELECT distinct mobile_number
						FROM marketingDashboard.enquiry_table
						WHERE category_final="Ophthalmology"
                        and disease_leads like "%cataract%"
						AND Created_at_IST_Date BETWEEN '2022-12-01' AND '2022-12-31'
						and sevenDayUniqueSQL=1)                      
group by mobile_number
having lead_count=1
and first_appearance_date BETWEEN '2022-12-01' AND '2022-12-31'
and leadId is not null
)ul
left join 
(
select Appointment_Start_Time,LeadId,Enquiry_Id,
	 sum(case when Appointment_Type="OPD" and 
                  (OPDType in ("Consultation","Walkin","Online Consult","Scan"))
     then 1 else 0 end) as OPD_Booked_Flag,
     sum(case when Appointment_Type="OPD" and Appointment_Status = "Active"
             and (OPDType in ("Consultation","Walkin","Online Consult","Scan"))
    then 1 else 0 end) as OPD_Flag,
    case when Appointment_Type="OPD" and Appointment_Status = "Active"
            and (OPDType in ("Consultation","Walkin","Online Consult","Scan"))
            and  (Doc_Surgery_Status in ("Surgery Suggested","Surgery Needed","Potential Surgery","Ready for surgery"))
            then 1 else 0 end as SS_Flag,
     sum(case when (Appointment_Type="IPD" and Appointment_Status = "Active")
             or (Appointment_Type="OPD" and Appointment_Status = "Active" and OPDType="Procedure" )
    then 1 else 0 end) as IPD_Flag
 from marketingDashboard.Appointments
 where date(Appointment_Start_Time)>='2022-12-01' and date(Appointment_Start_Time)<='2022-12-31'
 group by LeadId
 ) app
 on ul.leadId=app.LeadId
 and app.Appointment_Start_Time>=ul.first_appearance_dt
 group by ul.leadId
 ) final
 where city in ("Delhi","Mumbai","Chennai","Pune","Bangalore","Hyderabad")
 group by 1
