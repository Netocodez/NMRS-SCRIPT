from tkinter import ttk
import time
import tkinter as tk
from tkinter import filedialog
from tkinter import *
from tkcalendar import DateEntry
import mysql.connector as sql
import pandas as pd
import numpy as np
from datetime import datetime
import string

db_connection = sql.connect(
    host='localhost',
    database='openmrs',
    user='root',
    password='root',
    sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'  # Add this line
)

# Declare global variables
start_date = None
end_date = None

def get_selected_date():
    global start_date, end_date
    start_date = cal.get_date()
    end_date = cal2.get_date()
    start_date_label.config(text=f"Start Date: {start_date}")
    end_date_label.config(text=f"End Date: {end_date}")
    startDate = start_date
    endDate = end_date
    todayDate = endDate
    todayDate = str(todayDate)
    db_cursor.execute("""
    drop function if exists getoutcome;
    """)
    db_cursor.execute("""
    CREATE DEFINER=`root`@`localhost` FUNCTION `getoutcome`(`Pharmacy_LastPickupdate` date,`daysofarvrefill` numeric,`LTFUdays` numeric, `%s` date) RETURNS text CHARSET utf8
    BEGIN

        DECLARE  LTFUdate DATE;

        DECLARE  LTFUnumber NUMERIC;
        DECLARE  daysdiff NUMERIC;
        DECLARE outcome text;

        SET LTFUnumber=daysofarvrefill+LTFUdays;
        SELECT DATE_ADD(Pharmacy_LastPickupdate, INTERVAL LTFUnumber DAY) INTO LTFUdate;
        SELECT DATEDIFF(LTFUdate,'%s') into daysdiff;
        SELECT IF(daysdiff >=0,"Active","LTFU") into outcome;

        RETURN outcome;
    END;
    """ % (todayDate, todayDate))
    
    return startDate, endDate

#startDate = datetime(2000, 1, 1)
#endDate = datetime(2024, 6, 30)


def get_days_of_arv_refill(obsid, cid, pid):
    # Establish a connection to your MariaDB server
    mydb = sql.connect
    # Create a cursor
    cursor = mydb.cursor()

    # Execute the query
    query = """
    SELECT obs.value_numeric
    FROM obs
    WHERE obs.obs_group_id IS NOT NULL
      AND obs.obs_group_id = %s
      AND obs.concept_id = %s
      AND obs.person_id = %s
      AND obs.voided = 0
    LIMIT 1
    """
    cursor.execute(query, (obsid, cid, pid))

    # Fetch the result
    value_num = cursor.fetchone()[0]

    # Close the cursor and connection
    cursor.close()
    mydb.close()

    return value_num

db_cursor = db_connection.cursor()
db_cursor.execute("""
drop function if exists getconceptval;
""")
db_cursor.execute("""
    CREATE DEFINER=`root`@`localhost` FUNCTION `getconceptval`(`obsid` int, `cid` int, pid int) RETURNS decimal(10,0)
    BEGIN
        DECLARE value_num INT;
        SELECT obs.value_numeric INTO value_num FROM obs WHERE obs.obs_group_id IS NOT NULL AND obs.obs_group_id = obsid AND obs.concept_id = cid AND obs.person_id = pid AND obs.voided = 0 LIMIT 1;
        RETURN value_num;
    END;
""")
#init
def nmrsquery():
    start_time = time.time()  # Start time measurement
    startDate, endDate = get_selected_date()
    #status_label.config(text=f"Pulling data from NMRS database...")
    total_rows, df2 = biometrics()
    status_label2.config(text=f"Please don't close the SOFTWARE, you will be prompted to save when it completes")
    progress_bar['maximum'] = total_rows  # Set to 100 for percentage completion
    for index, row in df2.iterrows():
            
        # Update the progress bar value
        progress_bar['value'] = index + 1
        
        # Calculate the percentage of completion
        percentage = ((index + 1) / total_rows) * 100
        
        # Update the status label with the current percentage
        status_label.config(text=f"Counting rows to process... {index + 1}/{total_rows} ({percentage:.2f}%)")
                
        # Update the GUI to reflect changes
        root.update_idletasks()
        
        # Simulate time-consuming task
        time.sleep(0.0001)
    status_label.config(text=f"Processing... this will take an estimated time of: {0.008 * total_rows} minutes") 
    root.update_idletasks()
           
    db_cursor = db_connection.cursor()
    db_cursor.execute(f"""

    SELECT 
    nigeria_datimcode_mapping.state_name as `State`,
    nigeria_datimcode_mapping.lga_name as `LGA`,
    gp1.property_value as DatimCode,
    gp2.property_value as FaciityName,
    pid1.uuid,
    pid1.identifier as PatientHospitalNo,
    pid2.identifier as PatientUniqueID,
    pid3.identifier as  `ANCNoIdentifier`,
    pid4.identifier as  `HTSNo`,
    pe.gender as Sex,
    CONCAT(pn.given_name, ' ', pn.family_name) AS Patient_Name,
    CAST(psn_atr.value AS CHAR) AS Phone_No,
    pa.address1 AS Patient_Address,
    pe.birthdate as DOB,
    TIMESTAMPDIFF(YEAR,pe.birthdate,'{endDate}') as Age,
    MAX(IF(obs.concept_id=159599,IF(TIMESTAMPDIFF(YEAR,pe.birthdate,obs.value_datetime)>=5,TIMESTAMPDIFF(YEAR,pe.birthdate,obs.value_datetime),@ageAtStart:=0),null)) as  `AgeAtStartOfARTYears`,
    MAX(IF(obs.concept_id=159599,IF(TIMESTAMPDIFF(YEAR,pe.birthdate,obs.value_datetime)<5,TIMESTAMPDIFF(MONTH,pe.birthdate,obs.value_datetime),null),null)) as `AgeAtStartOfARTMonths`,
    CASE obs.concept_id = 165839
            WHEN obs.value_coded = 160529 THEN 'VCT'
            WHEN obs.value_coded = 160546 THEN 'STI'
            WHEN obs.value_coded = 5271 THEN 'FP'
            WHEN obs.value_coded = 160542 THEN 'OPD'
            WHEN obs.value_coded = 161629 THEN 'Ward'
            WHEN obs.value_coded = 5622 THEN 'Other'
            WHEN obs.value_coded = 165788 THEN 'Blood Bank'
            WHEN obs.value_coded = 160545 THEN 'Outreach'
            WHEN obs.value_coded = 165838 THEN 'Standalone HTS'
            WHEN obs.value_coded = 160539 THEN 'TB'
            WHEN obs.value_coded = 166026 THEN 'ANC'
            WHEN obs.value_coded = 166027 THEN 'L&D'
            WHEN obs.value_coded = 166028 THEN 'POSTnatal'
            WHEN obs.value_coded = 165512 THEN 'PMTCT'
        ELSE NULL 
    END AS setting,
    MAX(IF(obs.concept_id=160540,cn1.name,null)) as CareEntryPoint,
    MAX(IF(obs.concept_id=160554,obs.value_datetime, NULL)) as  `HIVConfirmedDate`,
    MAX(IF(obs.concept_id=160534,DATE_FORMAT(obs.value_datetime,'%d-%b-%Y'),null)) as DateTransferredIn,
    MAX(IF(obs.concept_id=165242,cn1.name,null)) as TransferInStatus,
    MAX(IF(obs.concept_id = 159599, obs.value_datetime, NULL)) AS ARTStartDate,
    MAX(IF((obs.concept_id=165708 and enc.form_id=27 AND obs.voided =0),container.last_date,null)) as `LastPickupDate`,
    MAX(IF(obs.concept_id=165708 AND obs.`obs_datetime` <= '{endDate}',DATE_FORMAT(container.last_date, '%d-%b-%Y'), NULL)) AS `LastVisitDate`,
    getconceptval(MAX(IF(obs.concept_id=162240,obs.obs_id,null) ),159368,p.patient_id) AS `DaysOfARVRefil`,
    MAX(IF(obs2.concept_id=165708,cn2.name,NULL)) AS `InitialRegimenLine`,
    MAX(
    IF(obs2.concept_id=164506,cn2.`name`,
    IF(obs2.concept_id=164513,cn2.`name`,
    IF(obs2.concept_id=164507,cn2.name,
    IF(obs2.concept_id=164514,cn2.name,
    IF(obs2.concept_id=165702,cn2.name,
    IF(obs2.concept_id=165703,cn2.name,
    NULL
    ))))))) AS `InitialRegimen`,
    MAX(IF(obs.concept_id=165708,cn1.name,NULL) ) AS `CurrentRegimenLine`,
    ( SELECT  cn.`name` FROM `obs` ob  JOIN `concept_name` cn ON cn.`concept_id` = ob.value_coded JOIN encounter e ON ob.encounter_id=e.encounter_id
        WHERE ob.`concept_id` IN (164506,164513,165702,164507,164514,165703)  AND cn.`locale` = 'en' AND cn.`locale_preferred` = 1 
        AND ob.`obs_datetime` <= '{endDate}'
        AND ob.`person_id` =  p.`patient_id` 
        AND e.encounter_type=13
        AND ob.voided=0
        AND e.voided=0
        ORDER BY ob.obs_datetime DESC LIMIT 1) AS `CurrentRegimen`,
    MAX(IF(obs.concept_id=165050,cn1.name,NULL)) AS `PregnancyStatus`,
    MAX(IF(obs.concept_id=856,obs.value_numeric, NULL))as `CurrentViralLoad(c/ml)`,
    IF(
    MAX(IF(obs.concept_id=856,obs.value_numeric,NULL)) is not null,
    MAX(IF(obs.concept_id=165414, DATE_FORMAT(obs.value_datetime,'%d-%b-%Y'),null)),
    null
    ) as `ViralLoadReportedDate`,
    MAX(IF(obs.concept_id = 856, STR_TO_DATE(obsmax.last_date, '%Y-%m-%d'), NULL)) AS `LastViralLoadDate`,
    ( SELECT  STR_TO_DATE(obs.value_datetime,'%Y-%m-%d') FROM `obs`
     WHERE obs.person_id = p.`patient_id`  
     AND obs.`concept_id` IN (159951) 
     AND obs.`obs_datetime` <= '{endDate}' 
     ORDER BY obs.obs_datetime DESC LIMIT 1) AS `LastDateOfSampleCollection`,
    MAX(IF(obs.concept_id=5089,obs.value_numeric,null)) as `CurrentWeight_Kg`,
    MAX(IF(obs.concept_id=5089,DATE_FORMAT(obs.obs_datetime,'%d-%b-%Y'),null)) as `CurrentWeightDate`,
    MAX(IF(obs.concept_id=5090,obs.value_numeric,null)) as `CurrentHeight_Kg`,  
    MAX(IF(obs.concept_id=5090,DATE_FORMAT(obs.obs_datetime,'%d-%b-%Y'),null)) as `CurrentHeightDate`,
    MAX(IF(obs.concept_id=1659,cn1.name,null)) as `TBStatus`,
    MAX(IF(obs.concept_id=1659,DATE_FORMAT(obs.obs_datetime,'%d-%b-%Y'),null)) as `TBStatusDate`,
    MAX(IF(obs.concept_id=164852,cn1.name,null)) as `INHStartDate`,
    MAX(IF(obs.concept_id=166096,cn1.name,null)) as `INHStopDate`,
    MAX(IF(obs.concept_id=165727 AND obs.value_coded=1679,obs.obs_datetime,null)) as LastINHDispensedDate,
    MAX(IF(obs.concept_id=1113,cn1.name,null)) as `TBTreatmentStartDate`,
    MAX(IF(obs.concept_id=159431,cn1.name,null)) as `TBTreatmentStopDate`,
    MAX(IF(obs.concept_id=166156,DATE_FORMAT(obs.value_datetime,'%d-%b-%Y'),null)) as `OTZStartDate`,
    MAX(IF(obs.concept_id=166158,DATE_FORMAT(obs.value_datetime,'%d-%b-%Y'),null)) as `OTZStopDate`,
    MAX(IF(obs.value_coded in (165681,165682,165688,165691,165692,165697,165699,166187,166194,166196,166197,166198),cn1.name,null)) as `DTGFirstPickUp`,
    DATE_FORMAT(( SELECT  MIN(DATE(`obs_datetime`)) FROM `obs` 
        WHERE value_coded IN (165681,165682,165688,165691,165692,165697,165699,166187,166194,166196,166197,166198)
        AND obs.`person_id` =  p.`patient_id` 
        AND obs.voided=0
        AND obs.value_coded IS NOT NULL),'%d/%m/%Y') AS `DateofFirstDTGPickup`,
    IFNULL(MAX(IF(obs.concept_id=165470,cn1.name,null)) ,
    getoutcome(
    MAX(IF(obs.concept_id=165708,container.last_date,null)),
    getconceptval(MAX(IF(obs.concept_id=162240,obs.obs_id,null) ),159368,p.patient_id) ,
    28,
    '{endDate}'
    ) ) as `CurrentARTStatus`,
    CASE
        WHEN (DATEDIFF(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), NOW()) BETWEEN 1 AND 180) THEN 'Active With Drugs'
        WHEN (DATEDIFF(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), NOW()) BETWEEN -28 AND -1) THEN 'Missed Appointment'
        WHEN (DATEDIFF(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), NOW()) = 0) THEN 'Today Visit'
        WHEN (DATEDIFF(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), NOW()) BETWEEN -90000 AND -29) THEN 'LTFU'
        ELSE 'LTFU'
    END AS 'Appointment_Status', 
    DATE_FORMAT(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), '%d-%b-%Y') AS `Next_Visit_Date`,

    DATEDIFF(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), NOW()) AS Days_To_Schedule
    /*MAX(IF(obs.concept_id = 856, STR_TO_DATE(obsmax.last_date, '%Y-%m-%d'), NULL)) AS `LastViralLoadDate`*/



    FROM patient p
        LEFT JOIN Patient_identifier pid1 on(pid1.patient_id = p.patient_id and pid1.identifier_type=5 and p.voided=0 and pid1.voided=0)
        LEFT JOIN Patient_identifier pid2 on(pid2.patient_id = p.patient_id and pid2.identifier_type=4 and p.voided=0 and pid2.voided=0)
        LEFT JOIN patient_identifier pid3 on(pid3.patient_id=p.patient_id and p.voided=0 and pid3.identifier_type=6 and pid3.voided=0)
        LEFT JOIN patient_identifier pid4 on(pid4.patient_id=p.patient_id and p.voided=0 and pid4.identifier_type=8 and pid4.voided=0)
        LEFT JOIN global_property gp1 on(gp1.property='facility_datim_code')
        LEFT JOIN global_property gp2 on(gp2.property='Facility_Name')
        LEFT JOIN nigeria_datimcode_mapping on(gp1.property_value=nigeria_datimcode_mapping.datim_code)
        LEFT JOIN
            (select 
            obs.person_id,
            obs.concept_id,
            MAX(obs.obs_datetime) as last_date, 
            MIN(obs.obs_datetime) as first_date
            from obs where obs.voided=0 AND obs.obs_datetime <= '{endDate}' and concept_id in(159599,165708,159368,164506,164513,164507,164514,165702,165703,165050,
            856,164980,165470,159635,5089,5090,165988,1659,164852,166096,1113,159431,162240,165242,165724,166156,166158,165727,164982,165414) GROUP BY obs.person_id, obs.concept_id) as container on (container.person_id=p.patient_id and p.voided=0)
        /*INNER JOIN obs ON obs.person_id = p.patient_id*/
        INNER JOIN obs on(obs.person_id=p.patient_id and obs.concept_id=container.concept_id and obs.obs_datetime=container.last_date and obs.voided=0 and obs.obs_datetime<='{endDate}')
        INNER JOIN obs obs2 on(obs2.person_id=p.patient_id and obs2.concept_id=container.concept_id and obs2.obs_datetime=container.first_date and obs2.voided=0 and obs2.obs_datetime<='{endDate}')
        left join concept_name cn1 on(obs.value_coded=cn1.concept_id and cn1.locale='en' and cn1.locale_preferred=1)
        left join concept_name cn2 on(obs2.value_coded=cn2.concept_id and cn2.locale='en' and cn2.locale_preferred=1)
        LEFT join encounter enc on(enc.encounter_id=obs.encounter_id and enc.voided=0 and obs.voided=0)
        left join 
        (
        select obs.person_id,obs.concept_id, MAX(obs.obs_datetime) as last_date from obs where obs.voided=0 and obs.concept_id IN(5096,856,165988)  
        GROUP BY obs.person_id,obs.concept_id
        ) as obsmax on(obs.person_id=obsmax.person_id and 
        obs.concept_id=obsmax.concept_id and obs.obs_datetime=obsmax.last_date)
        LEFT JOIN person_name AS pn ON pn.person_id = p.patient_id
        LEFT JOIN person_attribute AS psn_atr ON psn_atr.person_id = p.patient_id
            AND psn_atr.person_attribute_type_id = (SELECT person_attribute_type_id FROM person_attribute_type WHERE name = 'Telephone Number')
        LEFT JOIN person_address AS pa ON pa.person_id = p.patient_id
        LEFT JOIN person pe on pe.person_id = p.patient_id
        

        
        
    GROUP BY pid1.identifier
    """)
    table_rows = db_cursor.fetchall()

    df1 = pd.DataFrame(table_rows, columns=db_cursor.column_names)
    
    df3 = tracking()
    df1['EnrollmentDate']=df1['uuid'].map(df2.set_index('uuid')['EnrollmentDate'])
    df1['MarkAsDeseased']=df1['uuid'].map(df3.set_index('uuid')['MarkAsDeseased'])
    df1['MarkAsDeseasedDeathDate']=df1['uuid'].map(df3.set_index('uuid')['MarkAsDeseasedDeathDate'])
    df1['DateOfTermination']=df1['uuid'].map(df3.set_index('uuid')['TrackerTerminationDate'])
    df1['EnteredBy']=df1['uuid'].map(df3.set_index('uuid')['EnteredBy'])
    df1['DateCreated']=df1['uuid'].map(df3.set_index('uuid')['DateCreated'])
    df1['ReasonForTracking']=df1['uuid'].map(df3.set_index('uuid')['ReasonForTracking'])
    df1['PatientOutcome']=df1['uuid'].map(df3.set_index('uuid')['ReasonForTermination'])
    df1['PatientOutcomeDate']=df1['uuid'].map(df3.set_index('uuid')['DateOfTermination'])
    df1['CauseOfDeath']=df1['uuid'].map(df3.set_index('uuid')['CauseOfDeath'])
    df1['OtherCauseOfDeath']=df1['uuid'].map(df3.set_index('uuid')['OtherCauseOfDeath'])
    df1['DiscontinuedCareReason']=df1['uuid'].map(df3.set_index('uuid')['DiscontinuedCareReason'])
    df1['DateReturnedToCare']=df1['uuid'].map(df3.set_index('uuid')['DateReturnedToCare'])
    df1['ReferredFor']=df1['uuid'].map(df3.set_index('uuid')['ReferredFor'])
    
    
    #df1['Biometric_date']=df1['uuid'].map(df2.set_index('uuid')['Biometric_date'])
    #df1['Biometric_Status']=df1['uuid'].map(df2.set_index('uuid')['Biometric_Status'])
    df1['BiometricCaptured']=df1['uuid'].map(df2.set_index('uuid')['BiometricCaptured'])
    df1['BiometricCaptureDate']=df1['uuid'].map(df2.set_index('uuid')['BiometricCaptureDate'])
    df1['ValidCapture']=df1['uuid'].map(df2.set_index('uuid')['ValidCapture'])
    df1['RecaptureDate']=df1['uuid'].map(df2.set_index('uuid')['RecaptureDate'])
    df1['RecaptureCount']=df1['uuid'].map(df2.set_index('uuid')['RecaptureCount'])
    
    #df = DataFrame(db_cursor.fetchall())
    #df.columns = db_cursor.column_names

    print(df1)
    end_time = time.time()  # End time measurement
    total_time = end_time - start_time  # Calculate total time taken
    status_label.config(text=f"Thanks for your patience, process Completed! Time taken: {total_time:.2f} seconds")
    #status_label2.config(text=f"Just a moment! Formating and Saving File...")
    #df1.to_excel('df1.xlsx')
    output_file_name = "MAKE-SHIFT ART-LINE LIST".split("/")[-1][:-4]
    status_label2.config(text=f"Just a moment! Formating and Saving Converted File...")
    output_file_path = filedialog.asksaveasfilename(initialdir = '/Desktop', 
                                                    title = 'Select a excel file', 
                                                    filetypes = (('excel file','*.xls'), 
                                                                    ('excel file','*.xlsx')),defaultextension=".xlsx", initialfile=output_file_name)
    if not output_file_path:  # Check if the file save was cancelled
        status_label.config(text="File conversion was cancelled. No file was saved.")
        status_label2.config(text="Conversion Cancelled!")
        progress_bar['value'] = 0
        return  # Exit the function
    writer = pd.ExcelWriter(output_file_path, engine="xlsxwriter")
    df1.to_excel(writer, sheet_name="MAKE-SHIFT ART-LINE LIST", startrow=1, header=False, index=False)
    
    workbook = writer.book
    worksheet = writer.sheets["MAKE-SHIFT ART-LINE LIST"]
    
    # Add a header format.
    header_format = workbook.add_format(
        {
            "bold": True,
            "text_wrap": True,
            "valign": "bottom",
            "fg_color": "#D7E4BC",
            "border": 1,
        }
    )
    
    # Write the column headers with the defined format.
    for col_num, value in enumerate(df1.columns.values):
        worksheet.write(0, col_num + 0, value, header_format)
    
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    status_label2.config(text=f"Line List Generation Completed and Saved!")
    

def biometrics():
    startDate, endDate = get_selected_date()
    db_cursor.execute("""
    SELECT 
    pid1.uuid,
    global_property.property_value as DatimCode,
    pid1.identifier as HopitalNumber,
    pid2.identifier as UniqueID,
    patient_program.date_enrolled as EnrollmentDate,
    DATE_FORMAT(bvinfo.RecaptureDate,'%d-%b-%Y') as RecaptureDate,
    bvinfo.recapture_count as RecaptureCount,
    IF(biometrictable.patient_Id IS NOT NULL,'Yes','No') as BiometricCaptured,
    IF(biometrictable.patient_Id IS NOT NULL,STR_TO_DATE(biometrictable.date_created,'%Y-%m-%d'),NULL) as BiometricCaptureDate,
    IF(biometrictable.patient_Id IS NOT NULL,IF(invalidprint.patient_Id IS NOT NULL,'No','Yes'),"") as ValidCapture
    /*CAST(bi.date_created AS DATE) AS Biometric_date,
    IF(bi.date_created IS NOT NULL, 'Yes', 'No') AS Biometric_Status*/



    FROM patient p
        LEFT JOIN Patient_identifier pid1 on(pid1.patient_id = p.patient_id and pid1.identifier_type=5 and p.voided=0 and pid1.voided=0)
        LEFT JOIN Patient_identifier pid2 on(pid2.patient_id = p.patient_id and pid2.identifier_type=4 and p.voided=0 and pid2.voided=0)
        LEFT JOIN global_property on(global_property.property='facility_datim_code')
        /*LEFT JOIN biometricinfo bi ON (p.patient_id = bi.patient_id )*/
        LEFT JOIN patient_program on(patient_program.patient_id=p.patient_id and patient_program.voided=0 and patient_program.program_id=1)
        LEFT JOIN (
        select 
        DISTINCT biometricinfo.patient_Id,biometricinfo.date_created
        from 
        biometricinfo GROUP BY biometricinfo.patient_Id
        ) as biometrictable 
        on(p.patient_id=biometrictable.patient_Id and p.voided=0)
        LEFT JOIN (
        select 
        DISTINCT biometricinfo.patient_Id
        from 
        biometricinfo where template not like 'Rk1S%' or CONVERT(new_template USING utf8) NOT LIKE 'Rk1S%'
        ) as invalidprint 
        on(p.patient_id=invalidprint.patient_Id and p.voided=0)
        LEFT JOIN (SELECT patient_Id, MAX(date_created) AS RecaptureDate, recapture_count, count(fingerPosition) as NumberOfFingers, MIN(imageQuality) as LowestFPQuality, CEILING(AVG(imageQuality)) as AverageFPQuality, COUNT(IF(imageQuality<80, 1, NULL)) as FPLowQuality, COUNT(IF(imageQuality<80, NULL, 1)) as FPHighQuality
        FROM biometricverificationinfo 
        GROUP BY patient_Id) as bvinfo on(p.patient_id=bvinfo.patient_Id)
        WHERE p.voided=0 and pid1.identifier IS NOT NULL
        
        
    GROUP BY pid1.identifier
    """)
    table_rows = db_cursor.fetchall()
    total_rows1 = db_cursor.rowcount
    #print(total_rows1)
    df2 = pd.DataFrame(table_rows, columns=db_cursor.column_names)
    return total_rows1, df2

def tracking():
    startDate, endDate = get_selected_date()
    db_cursor.execute(f"""
    select
    pid2.uuid,
    pid1.identifier as UniqueID,
    pid2.identifier as HospID,
    IF(person.dead=1,"Dead","") as MarkAsDeseased,
    IF(person.dead=1,person.death_date,"") as MarkAsDeseasedDeathDate,
    encounter.encounter_datetime as TrackerTerminationDate,
    CONCAT(prs1.given_name,' ',prs1.family_name) as EnteredBy,
    encounter.date_created as DateCreated,
    MAX(IF(obs.concept_id=165460, cn1.name, NULL)) as  `ReasonForTracking`
    ,MAX(IF(obs.concept_id=165469, obs.value_datetime, NULL)) as  `DateOfTermination`
    ,MAX(IF(obs.concept_id=165470, cn1.name, NULL)) as  `ReasonForTermination`
    ,MAX(IF(obs.concept_id=165889, cn1.name, NULL)) as  `CauseOfDeath`
    ,MAX(IF(obs.concept_id=165915, obs.value_text, NULL)) as  `OtherCauseOfDeath`
    ,MAX(IF(obs.concept_id=165916, obs.value_text, NULL)) as  `DiscontinuedCareReason`
    ,MAX(IF(obs.concept_id=165775, obs.value_datetime, NULL)) as  `DateReturnedToCare`
    ,MAX(IF(obs.concept_id=165776, cn1.name, NULL)) as  `ReferredFor`

    FROM encounter 
    left join patient on(encounter.patient_id=patient.patient_id and patient.voided=0 and encounter.voided=0)
    left join person_attribute on(person_attribute.person_id=patient.patient_id and person_attribute.voided=0 and person_attribute.person_attribute_type_id=8 and person_attribute.voided=0)
    left join obs on(obs.encounter_id=encounter.encounter_id)
    left join patient_identifier pid1 on(pid1.patient_id=encounter.patient_id and pid1.identifier_type=4)
    left join patient_identifier pid2 on(pid2.patient_id=encounter.patient_id and pid2.identifier_type=5)
    left join users on(encounter.creator=users.user_id)
    left join person_name prs1 on(prs1.person_id=users.person_id and prs1.voided=0)
    left join person on(person.person_id=patient.patient_id)
    left join concept_name cn1 on(cn1.concept_id=obs.value_coded and cn1.locale='en' and cn1.locale_preferred=1)
    where (encounter.form_id=13  AND encounter.voided=0 AND
    encounter.encounter_datetime BETWEEN '{startDate}' and '{endDate}') OR (person.dead=1 and person.death_date BETWEEN '{startDate}' and '{endDate}' and person.voided=0) GROUP BY patient.patient_id;
    """)
    table_rows = db_cursor.fetchall()
    df3 = pd.DataFrame(table_rows, columns=db_cursor.column_names)
    return df3


def nmrscripttoRadet():
    #start_time = time.time()  # Start time measurement
    #status_label.config(text=f"Attempting to read file...")
    start_time = time.time()  # Start time measurement
    status_label.config(text=f"Attempting to read file...")
    input_file_path = filedialog.askopenfilename(initialdir = '/Desktop', 
                                                    title = 'Select a excel file', 
                                                    filetypes = (('excel file','*.xls'),                                                             
                                                                 ('excel file','*.xlsx')))
    if not input_file_path:  # Check if the file selection was cancelled
        status_label.config(text="No file selected. Please select a file to convert.")
        return  # Exit the function
    
    df = pd.read_excel(input_file_path,sheet_name=0, dtype=object)
    df.insert(0, 'S/N.', df.index + 1)
    #df.insert(4, 'Patient id', df.index + 1)
    df['Patient id']=''
    df['Household Unique No']=''
    df['Received OVC Service?']=''
    df.loc[df['Sex']=="M",'Sex']='Male'
    df.loc[df['Sex']=="F",'Sex']='Female'
    df['IIT DATE'] = pd.to_datetime(df['LastPickupDate']) + pd.to_timedelta(pd.to_numeric(df['DaysOfARVRefil']), unit='D') + pd.Timedelta(days=29)
    df['DaysOfARVRefil']=df['DaysOfARVRefil'].astype('float')
    df['DaysOfARVRefil'] = (df['DaysOfARVRefil'] / 30).round(1)
    df['TPT Type']=''
    df['TPT Completion date (yyyy-mm-dd)']=''
    df['Date of Regimen Switch/ Substitution (yyyy-mm-dd)']=''
    df['Date of Full Disclosure (yyyy-mm-dd)']=''
    df['Number of Support Group (OTZ Club) meeting attended']=''
    df['Number of OTZ Modules completed']=''
    df['VL Result After VL Sample Collection (c/ml)']=''
    df['Date of VL Result After VL Sample Collection (yyyy-mm-dd)']=''
    df['Status at Registration']=''
    #df['VL Result After VL Sample Collection (c/ml)']=''
    df['RTT']=''
    df['If Dead, Cause of Dead']=df['CauseOfDeath']
    df['VA Cause of Dead']=df['OtherCauseOfDeath']
    df['If Transferred out, new facility']=''
    df['Reason for Transfer-Out / IIT / Stooped Treatment']=df['DiscontinuedCareReason']
    df['ART Enrollment Setting']=''
    df['Date Commenced DMOC (yyyy-mm-dd)']=''
    df['Type of DMOC']=''
    df['Date of Return of DMOC Client to Facility (yyyy-mm-dd)']=''
    df['Date of Commencement of EAC (yyyy-mm-dd)']=''
    df['Number of EAC Sessions Completed']=''
    df['Date of 3rd EAC Completion (yyyy-mm-dd)']=''
    df['Date of Extended EAC Completion (yyyy-mm-dd)']=''
    df['Date of Repeat Viral Load - Post EAC VL Sample Collected (yyyy-mm-dd)']=''
    df['ViralLoadIndication']=''
    df['Co-morbidities']=''
    df['Date of Cervical Cancer Screening (yyyy-mm-dd)']=''
    df['Cervical Cancer Screening Type']=''
    df['Cervical Cancer Screening Method']=''
    df['Result of Cervical Cancer Screening']=''
    df['Date of Precancerous Lesions Treatment (yyyy-mm-dd)']=''
    df['Precancerous Lesions Treatment Methods']=''
    df['IIT Chance (%)']=''
    df['Date calculated (yyyy-mm-dd)']=''
    df['Case Manager']=''
    #df['EnrollmentDate']=''
    df['Date of Viral Load Sample Collection (yyyy-mm-dd)'] = df['LastDateOfSampleCollection']
    df.loc[(df['CurrentARTStatus'] == 'LTFU') & (df['PatientOutcomeDate'].isna()), 'PatientOutcomeDate'] = df['IIT DATE']
    df.loc[(df['CurrentARTStatus'] == 'Active') & (df['PatientOutcomeDate'].isna()), 'PatientOutcomeDate'] = df['LastPickupDate']
    df.loc[df['CurrentRegimenLine']=="Adult 1st line ARV regimen",'CurrentRegimenLine']='Adult.1st.Line'
    df.loc[df['CurrentRegimenLine']=="Adult 2nd line ARV regimen",'CurrentRegimenLine']='Adult.2nd.Line'
    df.loc[df['CurrentRegimenLine']=="Adult 3rd line ARV regimen",'CurrentRegimenLine']='Adult.3rd.Line'
    df.loc[df['CurrentRegimenLine']=="Child 1st line ARV regimen",'CurrentRegimenLine']='Peds.1st.Line'
    df.loc[df['CurrentRegimenLine']=="Child 2nd line ARV regimen",'CurrentRegimenLine']='Peds.2nd.Line'
    df.loc[df['CurrentRegimenLine']=="Child 3rd line ARV regimen",'CurrentRegimenLine']='Peds.3rd.Line'
    df.loc[df['InitialRegimenLine']=="Adult 1st line ARV regimen",'InitialRegimenLine']='Adult.1st.Line'
    df.loc[df['InitialRegimenLine']=="Adult 2nd line ARV regimen",'InitialRegimenLine']='Adult.2nd.Line'
    df.loc[df['InitialRegimenLine']=="Adult 3rd line ARV regimen",'InitialRegimenLine']='Adult.3rd.Line'
    df.loc[df['InitialRegimenLine']=="Child 1st line ARV regimen",'InitialRegimenLine']='Peds.1st.Line'
    df.loc[df['InitialRegimenLine']=="Child 2nd line ARV regimen",'InitialRegimenLine']='Peds.2nd.Line'
    df.loc[df['InitialRegimenLine']=="Child 3rd line ARV regimen",'InitialRegimenLine']='Peds.3rd.Line'
    
    df.loc[df['CurrentARTStatus']=="Death",'CurrentARTStatus']='Dead'
    df.loc[df['CurrentARTStatus']=="Discontinued Care",'CurrentARTStatus']='Stopped'
    df.loc[df['CurrentARTStatus']=="Transferred out",'CurrentARTStatus']='Transferred Out'
    df.loc[df['CurrentARTStatus']=="LTFU",'CurrentARTStatus']='IIT'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="Death",'CurrentARTStatusWithPillBalance']='Dead'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="Discontinued Care",'CurrentARTStatusWithPillBalance']='Stopped'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="Transferred out",'CurrentARTStatusWithPillBalance']='Transferred Out'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="InActive",'CurrentARTStatusWithPillBalance']='IIT'
    df['ARTStatusPreviousQuarter'] = ""
    df['PatientOutcomeDatePreviousQuarter'] = ''
    df['Date of Current Viral Load (yyyy-mm-dd)']=pd.to_datetime(df['ViralLoadReportedDate'],errors='coerce')
    df['LastViralLoadDate']=pd.to_datetime(df['LastViralLoadDate'],errors='coerce')
    df.loc[((df['Date of Current Viral Load (yyyy-mm-dd)'].isna()) | (df['LastViralLoadDate'] > df['Date of Current Viral Load (yyyy-mm-dd)'])),'Date of Current Viral Load (yyyy-mm-dd)']=df['LastViralLoadDate']
    df.loc[((df['Date of Current Viral Load (yyyy-mm-dd)'].notnull()) & (df['CurrentViralLoad(c/ml)'] <= 1000)),'ViralLoadIndication']='Routine - Routine'
    df.loc[((df['Date of Current Viral Load (yyyy-mm-dd)'].notnull()) & (df['CurrentViralLoad(c/ml)'] > 1000)),'ViralLoadIndication']='Targeted - Post EAC'
    
    #calculate IIT Chance
    
    # Calculate the difference between 'IIT DATE' and today's date
    today = pd.to_datetime('today')
    daystoIIT = (df['IIT DATE'] - today).dt.days
    # Apply the formula to get IIT chance as a percentage
    df['IITChance'] = ((29 - daystoIIT) / 29)
    # Apply IIT chance to active clients
    df.loc[(((df['CurrentARTStatus']== "Active") | (df['CurrentARTStatus']== "Active(A)")) & (df['IITChance'] >= 0)),'IIT Chance (%)']=df['IITChance']
    df.loc[(((df['CurrentARTStatus']== "Active") | (df['CurrentARTStatus']== "Active(A)")) & (df['IITChance'] >= 0)),'Date calculated (yyyy-mm-dd)']=df['IIT DATE']
    
    
    #rearrange columns
    df = df[['S/N.',
                'State',
                'LGA',
                'FaciityName',
                'Patient id',
                'PatientHospitalNo',
                'PatientUniqueID',
                'Household Unique No',
                'Received OVC Service?',
                'Sex',
                'CurrentWeight_Kg',
                'DOB',
                'ARTStartDate',
                'LastPickupDate',
                'DaysOfARVRefil',
                'LastINHDispensedDate',
                'TPT Type',
                'TPT Completion date (yyyy-mm-dd)',
                'InitialRegimenLine',
                'InitialRegimen',
                'CurrentRegimenLine',
                'CurrentRegimen',
                'Date of Regimen Switch/ Substitution (yyyy-mm-dd)',
                'PregnancyStatus',
                'Date of Full Disclosure (yyyy-mm-dd)',
                'OTZStartDate',
                'Number of Support Group (OTZ Club) meeting attended',
                'Number of OTZ Modules completed',
                'Date of Viral Load Sample Collection (yyyy-mm-dd)',
                'CurrentViralLoad(c/ml)',
                'Date of Current Viral Load (yyyy-mm-dd)',
                'ViralLoadIndication',
                'VL Result After VL Sample Collection (c/ml)',
                'Date of VL Result After VL Sample Collection (yyyy-mm-dd)',
                'Status at Registration',
                'EnrollmentDate',
                'ARTStatusPreviousQuarter',
                'PatientOutcomeDatePreviousQuarter',
                'CurrentARTStatus',
                'PatientOutcomeDate',
                'RTT',
                'If Dead, Cause of Dead',
                'VA Cause of Dead',
                'If Transferred out, new facility',
                'Reason for Transfer-Out / IIT / Stooped Treatment',
                'ART Enrollment Setting',
                'Date Commenced DMOC (yyyy-mm-dd)',
                'Type of DMOC',
                'Date of Return of DMOC Client to Facility (yyyy-mm-dd)',
                'Date of Commencement of EAC (yyyy-mm-dd)',
                'Number of EAC Sessions Completed',
                'Date of 3rd EAC Completion (yyyy-mm-dd)',
                'Date of Extended EAC Completion (yyyy-mm-dd)',
                'Date of Repeat Viral Load - Post EAC VL Sample Collected (yyyy-mm-dd)',
                'Co-morbidities',
                'Date of Cervical Cancer Screening (yyyy-mm-dd)',
                'Cervical Cancer Screening Type',
                'Cervical Cancer Screening Method',
                'Result of Cervical Cancer Screening',
                'Date of Precancerous Lesions Treatment (yyyy-mm-dd)',
                'Precancerous Lesions Treatment Methods',
                'BiometricCaptureDate',
                'BiometricCaptured',
                'IIT Chance (%)',
                'Date calculated (yyyy-mm-dd)',
                'Case Manager']]
    
    #Convert Date Objects to Date
    dfDates = ['DOB','ARTStartDate','LastPickupDate','LastINHDispensedDate','TPT Completion date (yyyy-mm-dd)','Date of Regimen Switch/ Substitution (yyyy-mm-dd)','Date of Full Disclosure (yyyy-mm-dd)', 'Date of Viral Load Sample Collection (yyyy-mm-dd)','Date of Current Viral Load (yyyy-mm-dd)','Date of VL Result After VL Sample Collection (yyyy-mm-dd)','EnrollmentDate','PatientOutcomeDatePreviousQuarter','PatientOutcomeDate','Date Commenced DMOC (yyyy-mm-dd)','Date of Return of DMOC Client to Facility (yyyy-mm-dd)','Date of Commencement of EAC (yyyy-mm-dd)','Date of 3rd EAC Completion (yyyy-mm-dd)','Date of Extended EAC Completion (yyyy-mm-dd)','Date of Repeat Viral Load - Post EAC VL Sample Collected (yyyy-mm-dd)','Date of Cervical Cancer Screening (yyyy-mm-dd)','Date of Precancerous Lesions Treatment (yyyy-mm-dd)','BiometricCaptureDate','Date calculated (yyyy-mm-dd)']
    for col in dfDates:
        df[col] = pd.to_datetime(df[col],errors='coerce').dt.date
    
    #rename columns
    df.columns = ['S/No.',
                  'State',
                  'LGA',
                  'Facility',
                  'Patient id',
                  'Hospital Number',
                  'Unique ID',
                  'Household Unique No',
                  'Received OVC Service?',
                  'Sex',
                  'Current Weight (Kg)',
                  'Date of Birth (yyyy-mm-dd)',
                  'ART Start Date (yyyy-mm-dd)',
                  'Last Pickup Date (yyyy-mm-dd)',
                  'Months of ARV Refill',
                  'Date of TPT Start (yyyy-mm-dd)',
                  'TPT Type',
                  'TPT Completion date (yyyy-mm-dd)',
                  'Regimen Line at ART Start',
                  'Regimen at ART Start',
                  'Current Regimen Line',
                  'Current ART Regimen',
                  'Date of Regimen Switch/ Substitution (yyyy-mm-dd)',
                  'Pregnancy Status','Date of Full Disclosure (yyyy-mm-dd)',
                  'Date Enrolled on OTZ (yyyy-mm-dd)',
                  'Number of Support Group (OTZ Club) meeting attended',
                  'Number of OTZ Modules completed',
                  'Date of Viral Load Sample Collection (yyyy-mm-dd)',
                  'Current Viral Load (c/ml)',
                  'Date of Current Viral Load (yyyy-mm-dd)',
                  'Viral Load Indication',
                  'VL Result After VL Sample Collection (c/ml)',
                  'Date of VL Result After VL Sample Collection (yyyy-mm-dd)',
                  'Status at Registration',
                  'Date of Enrollment/Transfer-In (yyyy-mm-dd)',
                  'Previous ART Status',
                  'Confirmed Date of Previous ART Status (yyyy-mm-dd)',
                  'Current ART Status',
                  'Date of Current ART Status (yyyy-mm-dd)',
                  'RTT',
                  'If Dead, Cause of Dead',
                  'VA Cause of Dead',
                  'If Transferred out, new facility',
                  'Reason for Transfer-Out / IIT / Stooped Treatment',
                  'ART Enrollment Setting',
                  'Date Commenced DMOC (yyyy-mm-dd)',
                  'Type of DMOC',
                  'Date of Return of DMOC Client to Facility (yyyy-mm-dd)',
                  'Date of Commencement of EAC (yyyy-mm-dd)',
                  'Number of EAC Sessions Completed',
                  'Date of 3rd EAC Completion (yyyy-mm-dd)',
                  'Date of Extended EAC Completion (yyyy-mm-dd)',
                  'Date of Repeat Viral Load - Post EAC VL Sample Collected (yyyy-mm-dd)',
                  'Co-morbidities',
                  'Date of Cervical Cancer Screening (yyyy-mm-dd)',
                  'Cervical Cancer Screening Type',
                  'Cervical Cancer Screening Method',
                  'Result of Cervical Cancer Screening',
                  'Date of Precancerous Lesions Treatment (yyyy-mm-dd)',
                  'Precancerous Lesions Treatment Methods',
                  'Date Biometrics Enrolled (yyyy-mm-dd)',
                  'Valid Biometrics Enrolled?',
                  'IIT Chance (%)',
                  'Date calculated (yyyy-mm-dd)',
                  'Case Manager']
    
    progress_bar['maximum'] = len(df)  # Set to 100 for percentage completion
    for index, row in df.iterrows():
            
        # Update the progress bar value
        progress_bar['value'] = index + 1
        
        # Calculate the percentage of completion
        percentage = ((index + 1) / len(df)) * 100
        
        # Update the status label with the current percentage
        status_label.config(text=f"Conversion in Progress: {index + 1}/{len(df)} ({percentage:.2f}%)")
        
        # Update the GUI to reflect changes
        root.update_idletasks()
        
        # Simulate time-consuming task
        time.sleep(0.000001)
    
    #format and export
    output_file_name = input_file_path.split("/")[-1][:-4]
    status_label2.config(text=f"Just a moment! Formating and Saving Converted File...")
    output_file_path = filedialog.asksaveasfilename(initialdir = '/Desktop', 
                                                    title = 'Select a excel file', 
                                                    filetypes = (('excel file','*.xls'), 
                                                                 ('excel file','*.xlsx')),defaultextension=".xlsx", initialfile=output_file_name)
    if not output_file_path:  # Check if the file save was cancelled
        status_label.config(text="File conversion was cancelled. No file was saved.")
        status_label2.config(text="File Conversion Cancelled!")
        progress_bar['value'] = 0
        return  # Exit the function
    
    writer = pd.ExcelWriter(output_file_path, engine="xlsxwriter")
    df.to_excel(writer, sheet_name="NMRS-RADET", startrow=1, header=False, index=False)
    
    workbook = writer.book
    worksheet = writer.sheets["NMRS-RADET"]
    
    # Add a header format.
    header_format = workbook.add_format(
        {
            "bold": True,
            "text_wrap": True,
            "valign": "bottom",
            "fg_color": "#D7E4BC",
            "border": 1,
        }
    )
    
    # Write the column headers with the defined format.
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num + 0, value, header_format)
    
    # Add a percent number format
    percent_format = workbook.add_format({'num_format': '0.00%'})

    # Apply the number format to the 'IIT Chance' column (3rd column, index 2)
    worksheet.set_column('BL:BL', None, percent_format)
    
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    end_time = time.time()  # End time measurement
    total_time = end_time - start_time  # Calculate total time taken
    status_label.config(text=f"Conversion Complete! Time taken: {total_time:.2f} seconds")
    status_label2.config(text=f" Converted File Location: {output_file_path}")


def nmrscripttoRadet2():
    #start_time = time.time()  # Start time measurement
    #status_label.config(text=f"Attempting to read file...")
    start_time = time.time()  # Start time measurement
    status_label.config(text=f"Attempting to read file...")
    input_Bseline = filedialog.askopenfilename(initialdir = '/Desktop', 
                                                    title = 'Select a excel file', 
                                                    filetypes = (('excel file','*.xls'), 
                                                                 ('excel file','*.xlsx'),
                                                                 ('excel file','*.xlsb')))
    if not input_Bseline:  # Check if the file selection was cancelled
        status_label.config(text="No file selected. Please select a file to convert.")
        return  # Exit the function
    dfbaseline = pd.read_excel(input_Bseline,sheet_name=0, dtype=object)
        
    status_label.config(text=f"Attempting to read file...")
    input_file_path = filedialog.askopenfilename(initialdir = '/Desktop', 
                                                    title = 'Select a excel file', 
                                                    filetypes = (('excel file','*.xls'),                                                             
                                                                 ('excel file','*.xlsx')))
    if not input_file_path:  # Check if the file selection was cancelled
        status_label.config(text="No file selected. Please select a file to convert.")
        return  # Exit the function
    
    df = pd.read_excel(input_file_path,sheet_name=0, dtype=object)
    df.insert(0, 'S/N.', df.index + 1)
    #df.insert(4, 'Patient id', df.index + 1)
    df['Patient id']=''
    df['Household Unique No']=''
    df['Received OVC Service?']=''
    df.loc[df['Sex']=="M",'Sex']='Male'
    df.loc[df['Sex']=="F",'Sex']='Female'
    df['IIT DATE'] = pd.to_datetime(df['LastPickupDate']) + pd.to_timedelta(pd.to_numeric(df['DaysOfARVRefil']), unit='D') + pd.Timedelta(days=29)
    df['DaysOfARVRefil']=df['DaysOfARVRefil'].astype('float')
    df['DaysOfARVRefil'] = (df['DaysOfARVRefil'] / 30).round(1)
    df['TPT Type']=''
    df['TPT Completion date (yyyy-mm-dd)']=''
    df['Date of Regimen Switch/ Substitution (yyyy-mm-dd)']=''
    df['Date of Full Disclosure (yyyy-mm-dd)']=''
    df['Number of Support Group (OTZ Club) meeting attended']=''
    df['Number of OTZ Modules completed']=''
    df['VL Result After VL Sample Collection (c/ml)']=''
    df['Date of VL Result After VL Sample Collection (yyyy-mm-dd)']=''
    df['Status at Registration']=''
    #df['VL Result After VL Sample Collection (c/ml)']=''
    df['RTT']=''
    df['If Dead, Cause of Dead']=df['CauseOfDeath']
    df['VA Cause of Dead']=df['OtherCauseOfDeath']
    df['If Transferred out, new facility']=''
    df['Reason for Transfer-Out / IIT / Stooped Treatment']=df['DiscontinuedCareReason']
    df['ART Enrollment Setting']=''
    df['Date Commenced DMOC (yyyy-mm-dd)']=''
    df['Type of DMOC']=''
    df['Date of Return of DMOC Client to Facility (yyyy-mm-dd)']=''
    df['Date of Commencement of EAC (yyyy-mm-dd)']=''
    df['Number of EAC Sessions Completed']=''
    df['Date of 3rd EAC Completion (yyyy-mm-dd)']=''
    df['Date of Extended EAC Completion (yyyy-mm-dd)']=''
    df['Date of Repeat Viral Load - Post EAC VL Sample Collected (yyyy-mm-dd)']=''
    df['ViralLoadIndication']=''
    df['Co-morbidities']=''
    df['Date of Cervical Cancer Screening (yyyy-mm-dd)']=''
    df['Cervical Cancer Screening Type']=''
    df['Cervical Cancer Screening Method']=''
    df['Result of Cervical Cancer Screening']=''
    df['Date of Precancerous Lesions Treatment (yyyy-mm-dd)']=''
    df['Precancerous Lesions Treatment Methods']=''
    df['IIT Chance (%)']=''
    df['Date calculated (yyyy-mm-dd)']=''
    df['Case Manager']=''
    #df['EnrollmentDate']=''
    df['Date of Viral Load Sample Collection (yyyy-mm-dd)'] = df['LastDateOfSampleCollection']
    df.loc[(df['CurrentARTStatus'] == 'LTFU') & (df['PatientOutcomeDate'].isna()), 'PatientOutcomeDate'] = df['IIT DATE']
    df.loc[(df['CurrentARTStatus'] == 'Active') & (df['PatientOutcomeDate'].isna()), 'PatientOutcomeDate'] = df['LastPickupDate']
    df.loc[df['CurrentRegimenLine']=="Adult 1st line ARV regimen",'CurrentRegimenLine']='Adult.1st.Line'
    df.loc[df['CurrentRegimenLine']=="Adult 2nd line ARV regimen",'CurrentRegimenLine']='Adult.2nd.Line'
    df.loc[df['CurrentRegimenLine']=="Adult 3rd line ARV regimen",'CurrentRegimenLine']='Adult.3rd.Line'
    df.loc[df['CurrentRegimenLine']=="Child 1st line ARV regimen",'CurrentRegimenLine']='Peds.1st.Line'
    df.loc[df['CurrentRegimenLine']=="Child 2nd line ARV regimen",'CurrentRegimenLine']='Peds.2nd.Line'
    df.loc[df['CurrentRegimenLine']=="Child 3rd line ARV regimen",'CurrentRegimenLine']='Peds.3rd.Line'
    df.loc[df['InitialRegimenLine']=="Adult 1st line ARV regimen",'InitialRegimenLine']='Adult.1st.Line'
    df.loc[df['InitialRegimenLine']=="Adult 2nd line ARV regimen",'InitialRegimenLine']='Adult.2nd.Line'
    df.loc[df['InitialRegimenLine']=="Adult 3rd line ARV regimen",'InitialRegimenLine']='Adult.3rd.Line'
    df.loc[df['InitialRegimenLine']=="Child 1st line ARV regimen",'InitialRegimenLine']='Peds.1st.Line'
    df.loc[df['InitialRegimenLine']=="Child 2nd line ARV regimen",'InitialRegimenLine']='Peds.2nd.Line'
    df.loc[df['InitialRegimenLine']=="Child 3rd line ARV regimen",'InitialRegimenLine']='Peds.3rd.Line'
    
    df.loc[df['CurrentARTStatus']=="Death",'CurrentARTStatus']='Dead'
    df.loc[df['CurrentARTStatus']=="Discontinued Care",'CurrentARTStatus']='Stopped'
    df.loc[df['CurrentARTStatus']=="Transferred out",'CurrentARTStatus']='Transferred Out'
    df.loc[df['CurrentARTStatus']=="LTFU",'CurrentARTStatus']='IIT'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="Death",'CurrentARTStatusWithPillBalance']='Dead'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="Discontinued Care",'CurrentARTStatusWithPillBalance']='Stopped'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="Transferred out",'CurrentARTStatusWithPillBalance']='Transferred Out'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="InActive",'CurrentARTStatusWithPillBalance']='IIT'
    df['ARTStatusPreviousQuarter'] = ""
    df['PatientOutcomeDatePreviousQuarter'] = ''
    df['Date of Current Viral Load (yyyy-mm-dd)']=pd.to_datetime(df['ViralLoadReportedDate'],errors='coerce')
    df['LastViralLoadDate']=pd.to_datetime(df['LastViralLoadDate'],errors='coerce')
    df.loc[((df['Date of Current Viral Load (yyyy-mm-dd)'].isna()) | (df['LastViralLoadDate'] > df['Date of Current Viral Load (yyyy-mm-dd)'])),'Date of Current Viral Load (yyyy-mm-dd)']=df['LastViralLoadDate']

    #remove leading zeros from dfbaseline hosital number
    #df['PatientHospitalNo1'] = df['PatientHospitalNo'].str.lstrip('0')
    df['PatientHospitalNo1'] = df['PatientHospitalNo'].apply(lambda x: x.lstrip('0') if x.isdigit() else x)
    df['PatientUniqueID1'] = df['PatientUniqueID'].apply(lambda x: x.lstrip('0') if x.isdigit() else x)
    dfbaseline['unique identifiers'] = dfbaseline["LGA"].astype(str) + dfbaseline["Facility"].astype(str) + dfbaseline["Hospital Number"].astype(str) + dfbaseline["Unique ID"].astype(str)
    df['unique identifiers'] = df["LGA"].astype(str) + df['FaciityName'].astype(str) + df["PatientHospitalNo1"].astype(str) + df["PatientUniqueID1"].astype(str)
    
    #remove duplicates
    dfbaseline = dfbaseline.drop_duplicates(subset=['unique identifiers'], keep=False)
    d = dict(enumerate(string.ascii_uppercase))
    m = df.duplicated(['unique identifiers'], keep=False)
    df.loc[m, 'unique identifiers'] += '_' + df[m].groupby(['unique identifiers']).cumcount().map(d)

    #Check active clients in baseline Radet
    refill_days = np.where(dfbaseline['Months of ARV Refill'] == "", 15, dfbaseline['Months of ARV Refill'] * 30)
    dfbaseline['IIT DATE_baseline'] = pd.to_datetime(dfbaseline['Last Pickup Date (yyyy-mm-dd)']) + pd.to_timedelta(refill_days, unit='D') + pd.Timedelta(days=29)
    dfbaseline['Revalidation status_baseline'] = np.where((dfbaseline['IIT DATE_baseline'] >= pd.to_datetime('today')) &
        ~dfbaseline['Current ART Status'].isin(["Dead", "Stopped", "Transferred Out", ""]),"Active","Inactive"
    )

    #insert columns in df from dfbaseline
    df['Revalidation status_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['Revalidation status_baseline'])
    df['Current Viral Load (c/ml)_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['Current Viral Load (c/ml)'])
    df['Date of Current Viral Load (yyyy-mm-dd)_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['Date of Current Viral Load (yyyy-mm-dd)'])
    df['Date of Viral Load Sample Collection (yyyy-mm-dd)_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['Date of Viral Load Sample Collection (yyyy-mm-dd)'])
    df['Case Manager_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['Case Manager'])
    
    df['ART Enrollment Setting_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['ART Enrollment Setting'])
    df['Date Commenced DMOC (yyyy-mm-dd)_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['Date Commenced DMOC (yyyy-mm-dd)'])
    df['Type of DMOC_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['Type of DMOC'])
    df['Date of Return of DMOC Client to Facility (yyyy-mm-dd)_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['Date of Return of DMOC Client to Facility (yyyy-mm-dd)'])
    df['Date of TPT Start (yyyy-mm-dd)_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['Date of TPT Start (yyyy-mm-dd)'])
    df['TPT Type_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['TPT Type'])
    df['TPT Completion date (yyyy-mm-dd)_baseline']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['TPT Completion date (yyyy-mm-dd)'])
    df['Patient id']=df['unique identifiers'].map(dfbaseline.set_index('unique identifiers')['Patient ID'])
    
    #Assign columns from baseline Radet to converted Radet
    df['Case Manager'] = df['Case Manager_baseline']
    df['ART Enrollment Setting']=df['ART Enrollment Setting_baseline']
    df['Date Commenced DMOC (yyyy-mm-dd)']=df['Date Commenced DMOC (yyyy-mm-dd)_baseline']
    df['Type of DMOC']=df['Type of DMOC_baseline']
    df['Date of Return of DMOC Client to Facility (yyyy-mm-dd)']=df['Date of Return of DMOC Client to Facility (yyyy-mm-dd)_baseline']
    df['TPT Type']=df['TPT Type_baseline']
    df['TPT Completion date (yyyy-mm-dd)']=df['TPT Completion date (yyyy-mm-dd)_baseline']
        
    #compare and update more recent data from baseline RADET to NMRS if seen
    df.loc[(df['CurrentARTStatus'] == 'LTFU') & (df['Revalidation status_baseline'] == 'Active'), 'CurrentARTStatus'] = "Active(A)"
    df.loc[((df['Date of Current Viral Load (yyyy-mm-dd)'].isna()) & (df["Date of Current Viral Load (yyyy-mm-dd)_baseline"].notnull())) | ((df["Date of Current Viral Load (yyyy-mm-dd)_baseline"] > df['Date of Current Viral Load (yyyy-mm-dd)']) & (df["Current Viral Load (c/ml)_baseline"].notnull())), 'CurrentViralLoad(c/ml)'] = df["Current Viral Load (c/ml)_baseline"]
    df.loc[((df['Date of Current Viral Load (yyyy-mm-dd)'].isna()) & (df["Date of Current Viral Load (yyyy-mm-dd)_baseline"].notnull())) | (df["Date of Current Viral Load (yyyy-mm-dd)_baseline"] > df['Date of Current Viral Load (yyyy-mm-dd)']), 'Date of Current Viral Load (yyyy-mm-dd)'] = df["Date of Current Viral Load (yyyy-mm-dd)_baseline"]
    df.loc[((df['Date of Viral Load Sample Collection (yyyy-mm-dd)'].isna()) & (df["Date of Viral Load Sample Collection (yyyy-mm-dd)_baseline"].notnull())) | (df["Date of Viral Load Sample Collection (yyyy-mm-dd)_baseline"] > df['Date of Viral Load Sample Collection (yyyy-mm-dd)']), 'Date of Viral Load Sample Collection (yyyy-mm-dd)'] = df["Date of Viral Load Sample Collection (yyyy-mm-dd)_baseline"]
    df.loc[((df['Date of Current Viral Load (yyyy-mm-dd)'].notnull()) & (df['CurrentViralLoad(c/ml)'] <= 1000)),'ViralLoadIndication']='Routine - Routine'
    df.loc[((df['Date of Current Viral Load (yyyy-mm-dd)'].notnull()) & (df['CurrentViralLoad(c/ml)'] > 1000)),'ViralLoadIndication']='Targeted - Post EAC'
    df.loc[((df['LastINHDispensedDate'].isna()) & (df["Date of TPT Start (yyyy-mm-dd)_baseline"].notnull())) | (df["Date of TPT Start (yyyy-mm-dd)_baseline"] > df['LastINHDispensedDate']), 'LastINHDispensedDate'] = df["Date of TPT Start (yyyy-mm-dd)_baseline"]
    #df.to_excel("test.xlsx")
    #dfbaseline.to_excel("test2.xlsx")
    
    #Drop Inserted columns no longer in use
    df = df.drop(['unique identifiers','Revalidation status_baseline', 'Current Viral Load (c/ml)_baseline','Date of Current Viral Load (yyyy-mm-dd)_baseline','Date of Viral Load Sample Collection (yyyy-mm-dd)_baseline'], axis=1)
    
    #calculate IIT Chance
    # Calculate the difference between 'IIT DATE' and today's date
    today = pd.to_datetime('today')
    daystoIIT = (df['IIT DATE'] - today).dt.days
    # Apply the formula to get IIT chance as a percentage
    df['IITChance'] = ((29 - daystoIIT) / 29)
    # Apply IIT chance to active clients
    df.loc[(((df['CurrentARTStatus']== "Active") | (df['CurrentARTStatus']== "Active(A)")) & (df['IITChance'] >= 0)),'IIT Chance (%)']=df['IITChance']
    df.loc[(((df['CurrentARTStatus']== "Active") | (df['CurrentARTStatus']== "Active(A)")) & (df['IITChance'] >= 0)),'Date calculated (yyyy-mm-dd)']=df['IIT DATE']

    
    
    #rearrange columns
    df = df[['S/N.',
                'State',
                'LGA',
                'FaciityName',
                'Patient id',
                'PatientHospitalNo',
                'PatientUniqueID',
                'Household Unique No',
                'Received OVC Service?',
                'Sex',
                'CurrentWeight_Kg',
                'DOB',
                'ARTStartDate',
                'LastPickupDate',
                'DaysOfARVRefil',
                'LastINHDispensedDate',
                'TPT Type',
                'TPT Completion date (yyyy-mm-dd)',
                'InitialRegimenLine',
                'InitialRegimen',
                'CurrentRegimenLine',
                'CurrentRegimen',
                'Date of Regimen Switch/ Substitution (yyyy-mm-dd)',
                'PregnancyStatus',
                'Date of Full Disclosure (yyyy-mm-dd)',
                'OTZStartDate',
                'Number of Support Group (OTZ Club) meeting attended',
                'Number of OTZ Modules completed',
                'Date of Viral Load Sample Collection (yyyy-mm-dd)',
                'CurrentViralLoad(c/ml)',
                'Date of Current Viral Load (yyyy-mm-dd)',
                'ViralLoadIndication',
                'VL Result After VL Sample Collection (c/ml)',
                'Date of VL Result After VL Sample Collection (yyyy-mm-dd)',
                'Status at Registration',
                'EnrollmentDate',
                'ARTStatusPreviousQuarter',
                'PatientOutcomeDatePreviousQuarter',
                'CurrentARTStatus',
                'PatientOutcomeDate',
                'RTT',
                'If Dead, Cause of Dead',
                'VA Cause of Dead',
                'If Transferred out, new facility',
                'Reason for Transfer-Out / IIT / Stooped Treatment',
                'ART Enrollment Setting',
                'Date Commenced DMOC (yyyy-mm-dd)',
                'Type of DMOC',
                'Date of Return of DMOC Client to Facility (yyyy-mm-dd)',
                'Date of Commencement of EAC (yyyy-mm-dd)',
                'Number of EAC Sessions Completed',
                'Date of 3rd EAC Completion (yyyy-mm-dd)',
                'Date of Extended EAC Completion (yyyy-mm-dd)',
                'Date of Repeat Viral Load - Post EAC VL Sample Collected (yyyy-mm-dd)',
                'Co-morbidities',
                'Date of Cervical Cancer Screening (yyyy-mm-dd)',
                'Cervical Cancer Screening Type',
                'Cervical Cancer Screening Method',
                'Result of Cervical Cancer Screening',
                'Date of Precancerous Lesions Treatment (yyyy-mm-dd)',
                'Precancerous Lesions Treatment Methods',
                'BiometricCaptureDate',
                'BiometricCaptured',
                'IIT Chance (%)',
                'Date calculated (yyyy-mm-dd)',
                'Case Manager']]
    
    #Convert Date Objects to Date
    dfDates = ['DOB','ARTStartDate','LastPickupDate','LastINHDispensedDate','TPT Completion date (yyyy-mm-dd)','Date of Regimen Switch/ Substitution (yyyy-mm-dd)','Date of Full Disclosure (yyyy-mm-dd)', 'Date of Viral Load Sample Collection (yyyy-mm-dd)','Date of Current Viral Load (yyyy-mm-dd)','Date of VL Result After VL Sample Collection (yyyy-mm-dd)','EnrollmentDate','PatientOutcomeDatePreviousQuarter','PatientOutcomeDate','Date Commenced DMOC (yyyy-mm-dd)','Date of Return of DMOC Client to Facility (yyyy-mm-dd)','Date of Commencement of EAC (yyyy-mm-dd)','Date of 3rd EAC Completion (yyyy-mm-dd)','Date of Extended EAC Completion (yyyy-mm-dd)','Date of Repeat Viral Load - Post EAC VL Sample Collected (yyyy-mm-dd)','Date of Cervical Cancer Screening (yyyy-mm-dd)','Date of Precancerous Lesions Treatment (yyyy-mm-dd)','BiometricCaptureDate','Date calculated (yyyy-mm-dd)']
    for col in dfDates:
        df[col] = pd.to_datetime(df[col],errors='coerce').dt.date
    
    #rename columns
    df.columns = ['S/No.',
                  'State',
                  'LGA',
                  'Facility',
                  'Patient id',
                  'Hospital Number',
                  'Unique ID',
                  'Household Unique No',
                  'Received OVC Service?',
                  'Sex',
                  'Current Weight (Kg)',
                  'Date of Birth (yyyy-mm-dd)',
                  'ART Start Date (yyyy-mm-dd)',
                  'Last Pickup Date (yyyy-mm-dd)',
                  'Months of ARV Refill',
                  'Date of TPT Start (yyyy-mm-dd)',
                  'TPT Type',
                  'TPT Completion date (yyyy-mm-dd)',
                  'Regimen Line at ART Start',
                  'Regimen at ART Start',
                  'Current Regimen Line',
                  'Current ART Regimen',
                  'Date of Regimen Switch/ Substitution (yyyy-mm-dd)',
                  'Pregnancy Status','Date of Full Disclosure (yyyy-mm-dd)',
                  'Date Enrolled on OTZ (yyyy-mm-dd)',
                  'Number of Support Group (OTZ Club) meeting attended',
                  'Number of OTZ Modules completed',
                  'Date of Viral Load Sample Collection (yyyy-mm-dd)',
                  'Current Viral Load (c/ml)',
                  'Date of Current Viral Load (yyyy-mm-dd)',
                  'Viral Load Indication',
                  'VL Result After VL Sample Collection (c/ml)',
                  'Date of VL Result After VL Sample Collection (yyyy-mm-dd)',
                  'Status at Registration',
                  'Date of Enrollment/Transfer-In (yyyy-mm-dd)',
                  'Previous ART Status',
                  'Confirmed Date of Previous ART Status (yyyy-mm-dd)',
                  'Current ART Status',
                  'Date of Current ART Status (yyyy-mm-dd)',
                  'RTT',
                  'If Dead, Cause of Dead',
                  'VA Cause of Dead',
                  'If Transferred out, new facility',
                  'Reason for Transfer-Out / IIT / Stooped Treatment',
                  'ART Enrollment Setting',
                  'Date Commenced DMOC (yyyy-mm-dd)',
                  'Type of DMOC',
                  'Date of Return of DMOC Client to Facility (yyyy-mm-dd)',
                  'Date of Commencement of EAC (yyyy-mm-dd)',
                  'Number of EAC Sessions Completed',
                  'Date of 3rd EAC Completion (yyyy-mm-dd)',
                  'Date of Extended EAC Completion (yyyy-mm-dd)',
                  'Date of Repeat Viral Load - Post EAC VL Sample Collected (yyyy-mm-dd)',
                  'Co-morbidities',
                  'Date of Cervical Cancer Screening (yyyy-mm-dd)',
                  'Cervical Cancer Screening Type',
                  'Cervical Cancer Screening Method',
                  'Result of Cervical Cancer Screening',
                  'Date of Precancerous Lesions Treatment (yyyy-mm-dd)',
                  'Precancerous Lesions Treatment Methods',
                  'Date Biometrics Enrolled (yyyy-mm-dd)',
                  'Valid Biometrics Enrolled?',
                  'IIT Chance (%)',
                  'Date calculated (yyyy-mm-dd)',
                  'Case Manager']
    
    progress_bar['maximum'] = len(df)  # Set to 100 for percentage completion
    for index, row in df.iterrows():
            
        # Update the progress bar value
        progress_bar['value'] = index + 1
        
        # Calculate the percentage of completion
        percentage = ((index + 1) / len(df)) * 100
        
        # Update the status label with the current percentage
        status_label.config(text=f"Conversion in Progress: {index + 1}/{len(df)} ({percentage:.2f}%)")
        
        # Update the GUI to reflect changes
        root.update_idletasks()
        
        # Simulate time-consuming task
        time.sleep(0.000001)
    
    #format and export
    output_file_name = input_file_path.split("/")[-1][:-4]
    status_label2.config(text=f"Just a moment! Formating and Saving Converted File...")
    output_file_path = filedialog.asksaveasfilename(initialdir = '/Desktop', 
                                                    title = 'Select a excel file', 
                                                    filetypes = (('excel file','*.xls'), 
                                                                 ('excel file','*.xlsx')),defaultextension=".xlsx", initialfile=output_file_name)
    if not output_file_path:  # Check if the file save was cancelled
        status_label.config(text="File conversion was cancelled. No file was saved.")
        status_label2.config(text="File Conversion Cancelled!")
        progress_bar['value'] = 0
        return  # Exit the function
    
    writer = pd.ExcelWriter(output_file_path, engine="xlsxwriter")
    df.to_excel(writer, sheet_name="NMRS-RADET", startrow=1, header=False, index=False)
    
    workbook = writer.book
    worksheet = writer.sheets["NMRS-RADET"]
    
    # Add a header format.
    header_format = workbook.add_format(
        {
            "bold": True,
            "text_wrap": True,
            "valign": "bottom",
            "fg_color": "#D7E4BC",
            "border": 1,
        }
    )
    
    # Write the column headers with the defined format.
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num + 0, value, header_format)
        
    # Add a percent number format
    percent_format = workbook.add_format({'num_format': '0.00%'})

    # Apply the number format to the 'IIT Chance' column (3rd column, index 2)
    worksheet.set_column('BL:BL', None, percent_format)
    
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    end_time = time.time()  # End time measurement
    total_time = end_time - start_time  # Calculate total time taken
    status_label.config(text=f"Conversion Complete! Time taken: {total_time:.2f} seconds")
    status_label2.config(text=f" Converted File Location: {output_file_path}")
    

#Creating A tooltip Class
class ToolTip(object):

    def __init__(self, widget):
        self.widget = widget
        self.tipwindow = None
        self.id = None
        self.x = self.y = 0

    def showtip(self, text):
        "Display text in tooltip window"
        self.text = text
        if self.tipwindow or not self.text:
            return
        x, y, cx, cy = self.widget.bbox("insert")
        x = x + self.widget.winfo_rootx() + 260
        y = y + cy + self.widget.winfo_rooty() +27
        self.tipwindow = tw = Toplevel(self.widget)
        tw.wm_overrideredirect(1)
        tw.wm_geometry("+%d+%d" % (x, y))
        label = Label(tw, text=self.text, justify=LEFT,
                      background="#ffffe0", relief=SOLID, borderwidth=1,
                      font=("tahoma", "8", "normal"))
        label.pack(ipadx=1)

    def hidetip(self):
        tw = self.tipwindow
        self.tipwindow = None
        if tw:
            tw.destroy()

def CreateToolTip(widget, text):
    toolTip = ToolTip(widget)
    def enter(event):
        toolTip.showtip(text)
    def leave(event):
        toolTip.hidetip()
    widget.bind('<Enter>', enter)
    widget.bind('<Leave>', leave)

#def removedateevent():
    #start_date, end_date = on_date_selected(None)
    #return start_date, end_date

#start_date, end_date = removedateevent()  
  
    
# Creating Main Window
root = tk.Tk()
root.title("NETO's NMRS STATE MAKESHIFT GENERATOR V002")
root.geometry("650x520")
root.config(bg="#f0f0f0")

#date text
selectinfo = tk.Label(root, text="Please ensure you click get selected date button before you proceed", font=("Helvetica", 10), fg="red")
selectinfo.pack(pady=1)

# Create a frame to hold the DateEntry widgets
frame = tk.Frame(root)
frame.pack(padx=10, pady=2)

# Get the current date
current_date = datetime.now().date()

# Create DateEntry widgets with date format and select mode
cal = DateEntry(frame, width=12, background="darkblue", foreground="white", borderwidth=2, date_pattern='dd-mm-yyyy', selectmode='day', year=2000, month=1, day=1)
cal.pack(side=tk.LEFT, padx=5)
cal2 = DateEntry(frame, width=12, background="darkblue", foreground="white", borderwidth=2, date_pattern='dd-mm-yyyy', selectmode='day')
cal2.set_date(current_date)
cal2.pack(side=tk.LEFT, padx=5)

# Create a button to get the selected date
get_date_button = tk.Button(root, text="Get Selected Date", command=get_selected_date)
get_date_button.pack(pady=2)

# Bind the date selection event to the handler
#cal.bind("<<DateEntrySelected>>", on_date_selected)
#cal2.bind("<<DateEntrySelected>>", on_date_selected)

# Create labels to display the selected dates
start_date_label = tk.Label(root, text="", font=("Helvetica", 9))
start_date_label.pack(padx=10, pady=0.5)
end_date_label = tk.Label(root, text="", font=("Helvetica", 9))
end_date_label.pack(padx=10, pady=0.5)

# Adding a Button to the Window
convert_button = tk.Button(root, text="CONNECT & GENERATE LINE LIST...", command=nmrsquery, font=("Helvetica", 14), bg="#4caf50", fg="#ffffff")
convert_button.pack(pady=5)
radet_export_button = tk.Button(root, text="Exported file to Radet without basleine", command=nmrscripttoRadet)
radet_export_button.pack(pady=0.5)
tooltip = CreateToolTip(radet_export_button, "PEASE ONLY CLICK THIS BUTTON WHEN YOU HAVE SAVED THE FILE \n YOU GENERATED USING (CONNECT & GENERATE LINE LIST button) as it will be required \n you will only be required to select the file you just exported \n This option will not require baseline RADET and no data from baseline RADET will be updated")
radet_export2_button = tk.Button(root, text="Exported file to Radet with baseline info", command=nmrscripttoRadet2)
radet_export2_button.pack(pady=0.5)
tooltip = CreateToolTip(radet_export2_button, "PEASE ONLY CLICK THIS BUTTON WHEN YOU HAVE SAVED THE FILE \n YOU GENERATED USING (CONNECT & GENERATE LINE LIST button) as it will be required \n you will be required to first select: \n Baseline radet file and then the file you just exported \n This will compare baseline Radet with makeshift line list and update where necessary the latest: \n Patient ID, VL results, VL SC, CAS, Date of CAS, ART Enrollment setting, TPT Data, DMOC data and CMG Info")
convert_button1 = tk.Button(root, text="EXIT APPLICATION", command=root.destroy, font=("Helvetica", 14), bg="red", fg="#ffffff")
convert_button1.pack(pady=10)

#def process_data(row):
    #time.sleep(0.0000005)  # Simulating a time-consuming task
        
# Progress bar widget
progress_bar = ttk.Progressbar(root, orient='horizontal', length=300, mode='determinate')
progress_bar.pack(pady=10)

# Label for percentage and progress messages
status_label = tk.Label(root, text="0%", font=('Helvetica', 12))
status_label.pack(pady=5)

status_label2 = tk.Label(root, text='Welcome to NMRS ART line list to RADET converter ', bg="#D3D3D3", font=('Helvetica', 10))
status_label2.pack(pady=10)

text3 = tk.Label(root, text="This Application pulls data from NMRS MYSQL DATABASE \n You will also be prompted to save the file when it completes \n(Please Remember to follow the file requirement procedures)")
text3.pack(pady=1)

text2 = tk.Label(root, text="Contacts: email: chinedum.pius@gmail.com, phone: +2348134453841")
text2.pack(pady=15)


# Adding File Dialog
filedialog = tk.filedialog 

# Running the GUI
root.mainloop()




