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
nigeria_datimcode_mapping.state_name as `State`,
nigeria_datimcode_mapping.lga_name as `LGA`,
gp1.property_value as DatimCode,
gp2.property_value as FaciityName,
pid1.uuid,
pid1.identifier as HopitalNumber,
pid2.identifier as UniqueID,
MAX(IF(obs.concept_id=160540,cn1.name,null)) as CareEntryPoint


FROM patient p
    LEFT JOIN Patient_identifier pid1 on(pid1.patient_id = p.patient_id and pid1.identifier_type=5 and p.voided=0 and pid1.voided=0)
    LEFT JOIN Patient_identifier pid2 on(pid2.patient_id = p.patient_id and pid2.identifier_type=4 and p.voided=0 and pid2.voided=0)
    LEFT JOIN global_property gp1 on(gp1.property='facility_datim_code')
    LEFT JOIN global_property gp2 on(gp2.property='Facility_Name')
    LEFT JOIN nigeria_datimcode_mapping on(gp1.property_value=nigeria_datimcode_mapping.datim_code)
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
    INNER JOIN obs obs2 on(obs2.person_id=p.patient_id and obs2.concept_id=container.concept_id and obs2.obs_datetime=container.first_date and obs2.voided=0 and obs2.obs_datetime <=CURDATE())
    LEFT join encounter enc on(enc.encounter_id=obs.encounter_id and enc.voided=0 and obs.voided=0)
    left join concept_name cn1 on(obs.value_coded=cn1.concept_id and cn1.locale='en' and cn1.locale_preferred=1)
    left join concept_name cn2 on(obs2.value_coded=cn2.concept_id and cn2.locale='en' and cn2.locale_preferred=1)
    

GROUP BY pid1.identifier
""")
table_rows = db_cursor.fetchall()
df2 = pd.DataFrame(table_rows, columns=db_cursor.column_names)
print(df2)
df2.to_excel("df2.xlsx")
