import mysql.connector as sql
import pandas as pd
from datetime import datetime

db_connection = sql.connect(
    host='localhost',
    database='openmrs',
    user='root',
    password='root',
    sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'  # Add this line
)

db_cursor = db_connection.cursor()

db_cursor.execute("""
SELECT 
    pid1.uuid,
    global_property.property_value as DatimCode,
    pid1.identifier as HopitalNumber,
    pid2.identifier as UniqueID,
    patient_program.date_enrolled as EnrollmentDate,
    /*CAST(bi.date_created AS DATE) AS Biometric_date,
    IF(bi.date_created IS NOT NULL, 'Yes', 'No') AS Biometric_Status*/
    DATE_FORMAT(bvinfo.RecaptureDate,'%d-%b-%Y') as RecaptureDate,
    bvinfo.recapture_count as RecaptureCount,
    IF(biometrictable.patient_Id IS NOT NULL,'Yes','No') as BiometricCaptured,
    IF(biometrictable.patient_Id IS NOT NULL,STR_TO_DATE(biometrictable.date_created,'%Y-%m-%d'),NULL) as BiometricCaptureDate,
    IF(biometrictable.patient_Id IS NOT NULL,IF(invalidprint.patient_Id IS NOT NULL,'No','Yes'),"") as ValidCapture



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
df2 = pd.DataFrame(table_rows, columns=db_cursor.column_names)
print(df2)
df2.to_excel("df2.xlsx")
