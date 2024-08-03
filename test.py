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
patient_program.date_enrolled as EnrollDate,
/*CAST(bi.date_created AS DATE) AS Biometric_date,
IF(bi.date_created IS NOT NULL, 'Yes', 'No') AS Biometric_Status*/
/*getcodedvalueobsid(getmaxconceptobsidwithformid(patient.patient_id,165470,13,@endDate)) as PatientOutcome,*/
MAX(IF((obs.concept_id=165470 and enc.form_id=13 AND obs.voided =0),p.patient_id,null)) as `Patientoutcome`



FROM patient p
    LEFT JOIN Patient_identifier pid1 on(pid1.patient_id = p.patient_id and pid1.identifier_type=5 and p.voided=0 and pid1.voided=0)
    LEFT JOIN Patient_identifier pid2 on(pid2.patient_id = p.patient_id and pid2.identifier_type=4 and p.voided=0 and pid2.voided=0)
    LEFT JOIN global_property on(global_property.property='facility_datim_code')
    /*LEFT JOIN biometricinfo bi ON (p.patient_id = bi.patient_id )*/
    LEFT JOIN patient_program on(patient_program.patient_id=p.patient_id and patient_program.voided=0 and patient_program.program_id=1)
    /*INNER JOIN obs ON obs.person_id = p.patient_id*/
    LEFT JOIN
            (select 
            obs.person_id,
            obs.concept_id,
            MAX(obs.obs_datetime) as last_date, 
            MIN(obs.obs_datetime) as first_date
            from obs where obs.voided=0 /*AND obs.obs_datetime <= today_date */and concept_id in(159599,165708,159368,164506,164513,164507,164514,165702,165703,165050,
            856,164980,165470,159635,5089,5090,165988,1659,164852,166096,1113,159431,162240,165242,165724,166156,166158,165727,164982,165414) GROUP BY obs.person_id, obs.concept_id) as container on (container.person_id=p.patient_id and p.voided=0)
        /*INNER JOIN obs ON obs.person_id = p.patient_id*/
    INNER JOIN obs on(obs.person_id=p.patient_id and obs.concept_id=container.concept_id and obs.obs_datetime=container.last_date and obs.voided=0 and obs.obs_datetime<=CURDATE())
    LEFT join encounter enc on(enc.encounter_id=obs.encounter_id and enc.voided=0 and obs.voided=0)
    
    
GROUP BY pid1.identifier
""")
table_rows = db_cursor.fetchall()
df2 = pd.DataFrame(table_rows, columns=db_cursor.column_names)
print(df2)
