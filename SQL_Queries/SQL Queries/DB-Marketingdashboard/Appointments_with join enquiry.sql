select *,
 Appointment_ID,date(Appointment_Start_Time) as Appointment_Start_Time,Appointment_ID,Enquiry_Id,LeadId,
        Appointment_Type,Appointment_Status,OPDType,Doc_Surgery_Status,Payment_Mode,
        case when Appointment_Type="OPD" and 
                  (OPDType in ("Consultation","Walkin","Online Consult","Scan"))
     then 1 else 0 end as OPD_Booked_Flag,
     case when Appointment_Type="OPD" and Appointment_Status = "Active"
             and (OPDType in ("Consultation","Walkin","Online Consult","Scan"))
    then 1 else 0 end as OPD_Flag,
        case when Appointment_Type="OPD" and Appointment_Status = "Active"
             and (OPDType in ("Consultation","Walkin","Online Consult","Scan"))
             and  (Doc_Surgery_Status in ("Surgery Suggested","Surgery Needed",
            "Potential Surgery","Ready for surgery"))
    then 1 else 0 end as SS_Flag,                                   
     case when (Appointment_Type="IPD" and Appointment_Status = "Active")
             or (Appointment_Type="OPD" and Appointment_Status = "Active" and OPDType="Procedure" )
    then 1 else 0 end as IPD_Flag
 from marketingDashboard.Appointments
 where Appointment_Start_Time>='2024-01-01' and Appointment_Start_Time<'2024-01-16'
 and Enquiry_Id in 
      (select Enquiry_id from marketingDashboard.enquiry_table
                     where source in ("SEO_OPs","SEO","SEO_NonOPs","SEO_Ops","SEO_NonOps")
	  )
