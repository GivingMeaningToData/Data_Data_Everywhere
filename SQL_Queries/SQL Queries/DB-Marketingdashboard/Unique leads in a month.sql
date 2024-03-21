select min(Created_at_IST_date) as first_appearance_date,Created_at_IST,enquiry_id,leadId,source,lead_source,disease_leads,category_final,team_final,
       sum(sevenDayUniqueSQL) as lead_count
from marketingDashboard.enquiry_table 
where mobile_number in 
						(SELECT distinct mobile_number
						FROM marketingDashboard.enquiry_table
						WHERE category_final="Ophthalmology"
						AND Created_at_IST_Date BETWEEN '2022-12-01' AND '2022-12-31'
						and sevenDayUniqueSQL=1)
group by mobile_number
having lead_count=1
and first_appearance_date BETWEEN '2022-12-01' AND '2022-12-31'