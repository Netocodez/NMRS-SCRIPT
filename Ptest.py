import mysql.connector as sql
import pandas as pd
from datetime import datetime
from datetime import date

db_connection = sql.connect(
    host='localhost',
    database='openmrs',
    user='root',
    password='root',
    sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'  # Add this line
)

db_cursor = db_connection.cursor()

'''db_cursor.execute("""
drop function if exists GetEndOfLastQuarter;
""")
db_cursor.execute("""
CREATE FUNCTION GetEndOfLastQuarter(input_date DATE)
RETURNS DATE
BEGIN
    DECLARE end_of_last_quarter DATE;
    
    SET end_of_last_quarter = CASE 
        WHEN QUARTER(input_date) = 1 THEN MAKEDATE(YEAR(input_date) - 1, 1) + INTERVAL 4 QUARTER - INTERVAL 1 DAY
        WHEN QUARTER(input_date) = 2 THEN MAKEDATE(YEAR(input_date), 1) + INTERVAL 1 QUARTER - INTERVAL 1 DAY
        WHEN QUARTER(input_date) = 3 THEN MAKEDATE(YEAR(input_date), 1) + INTERVAL 2 QUARTER - INTERVAL 1 DAY
        ELSE MAKEDATE(YEAR(input_date), 1) + INTERVAL 3 QUARTER - INTERVAL 1 DAY
    END;
    
    RETURN end_of_last_quarter;
END
""" )'''

def get_end_of_last_quarter(input_date):
    year = input_date.year
    quarter = (input_date.month - 1) // 3 + 1

    if quarter == 1:
        end_of_last_quarter = datetime(year - 1, 12, 31)
    elif quarter == 2:
        end_of_last_quarter = datetime(year, 3, 31)
    elif quarter == 3:
        end_of_last_quarter = datetime(year, 6, 30)
    else:
        end_of_last_quarter = datetime(year, 9, 30)

    return end_of_last_quarter.date()

# Get today's date
today = date.today()

# Convert to string in the format "year-month-day"
#today = todays.strftime("%Y-%m-%d")

db_cursor.execute("""
drop function if exists getoutcome;
""")
db_cursor.execute("""
CREATE DEFINER=`root`@`localhost` FUNCTION `getoutcome`(`Pharmacy_LastPickupdate` date,`daysofarvrefill` numeric,`LTFUdays` numeric, `CURDATE()` date) RETURNS text CHARSET utf8
BEGIN

    DECLARE  LTFUdate DATE;

    DECLARE  LTFUnumber NUMERIC;
    DECLARE  daysdiff NUMERIC;
    DECLARE outcome text;

    SET LTFUnumber=daysofarvrefill+LTFUdays;
    SELECT DATE_ADD(Pharmacy_LastPickupdate, INTERVAL LTFUnumber DAY) INTO LTFUdate;
    SELECT DATEDIFF(LTFUdate,CURDATE()) into daysdiff;
    SELECT IF(daysdiff >=0,"Active","LTFU") into outcome;

    RETURN outcome;
END;
""" )

db_cursor.execute("""
drop function if exists getoutcome2;
""")
db_cursor.execute(f"""
CREATE DEFINER=`root`@`localhost` FUNCTION `getoutcome2`(`Pharmacy_LastPickupdate` date,`daysofarvrefill` numeric,`LTFUdays` numeric, `{get_end_of_last_quarter(today)}` date) RETURNS text CHARSET utf8
BEGIN

    DECLARE  LTFUdate DATE;

    DECLARE  LTFUnumber NUMERIC;
    DECLARE  daysdiff NUMERIC;
    DECLARE outcome text;

    SET LTFUnumber=daysofarvrefill+LTFUdays;
    SELECT DATE_ADD(Pharmacy_LastPickupdate, INTERVAL LTFUnumber DAY) INTO LTFUdate;
    SELECT DATEDIFF(LTFUdate,'{get_end_of_last_quarter(today)}') into daysdiff;
    SELECT IF(daysdiff >=0,"Active","LTFU") into outcome;

    RETURN outcome;
END;
""" )

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

db_cursor = db_connection.cursor()
db_cursor.execute("""
drop function if exists getmaxconceptobsidwithformid;
""")
db_cursor.execute("""
    CREATE FUNCTION getmaxconceptobsidwithformid(patient_id INT, concept_id INT, form_id INT, date DATE)
    RETURNS INT
    BEGIN
        RETURN (
            SELECT MAX(obs.obs_id)
            FROM obs
            JOIN encounter enc ON obs.encounter_id = enc.encounter_id
            WHERE obs.person_id = patient_id
            AND obs.concept_id = concept_id
            AND enc.form_id = form_id
            AND obs.voided = 0
            AND obs.obs_datetime <= date
        );
    END;
""")

db_cursor = db_connection.cursor()
db_cursor.execute("""
drop function if exists getobsdatetime;
""")
db_cursor.execute("""
    CREATE FUNCTION getobsdatetime(obs_id INT)
    RETURNS DATETIME
    BEGIN
        RETURN (
            SELECT obs_datetime
            FROM obs
            WHERE obs_id = obs_id
            AND voided = 0
        );
    END;
""")

db_cursor.execute(f"""
SELECT 
nigeria_datimcode_mapping.state_name as `State`,
nigeria_datimcode_mapping.lga_name as `LGA`,
gp1.property_value as DatimCode,
gp2.property_value as FaciityName,
pid1.uuid,
pid1.identifier as HopitalNumber,
pid2.identifier as UniqueID,
MAX(IF(obs.concept_id=160540,cn1.name,null)) as CareEntryPoint,
IFNULL(MAX(IF(obs.concept_id=165470,cn1.name,null)),
    getoutcome(
    MAX(IF(obs.concept_id=165708,container.last_date,null)),
    getconceptval(MAX(IF(obs.concept_id=162240,obs.obs_id,null) ),159368,p.patient_id),
    28,
    CURDATE()
    ) ) as `CurrentARTStatus`,
MAX(IF((obs.concept_id=165708 and enc.form_id=27 AND obs.voided =0),container.last_date,null)) as `LastPickupDate`,

(
    SELECT MAX(obs.obs_datetime)
    FROM obs
    JOIN encounter enc ON obs.encounter_id = enc.encounter_id
    WHERE obs.person_id = p.patient_id
    AND obs.concept_id = 165708
    AND enc.form_id = 27
    AND obs.voided = 0
    AND obs.obs_datetime <= CURDATE()
    AND obs.obs_datetime < (
        SELECT MAX(obs.obs_datetime)
        FROM obs
        JOIN encounter enc ON obs.encounter_id = enc.encounter_id
        WHERE obs.person_id = p.patient_id
        AND obs.concept_id = 165708
        AND enc.form_id = 27
        AND obs.voided = 0
    )
) AS `PREVIOUSPICKUP`,

(
        SELECT obs.obs_datetime
        FROM obs
        JOIN encounter enc ON obs.encounter_id = enc.encounter_id
        WHERE obs.person_id = p.patient_id
        AND obs.concept_id = 165708
        AND enc.form_id = 27
        AND obs.voided = 0
        AND obs.obs_datetime <= CURDATE()
        ORDER BY obs.obs_datetime DESC
        LIMIT 1 OFFSET 1
    ) AS `PreviousPickupDate`,
    
getconceptval(MAX(IF(obs.concept_id=162240,obs.obs_id,null) ),159368,p.patient_id) AS `DaysOfARVRefil`,


(
    SELECT MAX(obs.obs_datetime)
    FROM obs
    JOIN encounter enc ON obs.encounter_id = enc.encounter_id
    WHERE obs.person_id = p.patient_id
    AND obs.concept_id = 165708
    AND enc.form_id = 27
    AND obs.voided = 0
    AND obs.obs_datetime < (
        SELECT obs.obs_datetime
        FROM obs
        JOIN encounter enc ON obs.encounter_id = enc.encounter_id
        WHERE obs.person_id = p.patient_id
        AND obs.concept_id = 165708
        AND enc.form_id = 27
        AND obs.voided = 0
        AND obs.obs_datetime <= CURDATE()
        ORDER BY obs.obs_datetime DESC
        LIMIT 1 OFFSET 0
    )
) AS `previousref`,

(
    SELECT getconceptval(MAX(IF(obs.concept_id=162240, obs.obs_id, NULL)), 159368, p.patient_id)
    FROM obs
    WHERE obs.person_id = p.patient_id
    AND obs.obs_datetime < (
        SELECT obs.obs_datetime
        FROM obs
        JOIN encounter enc ON obs.encounter_id = enc.encounter_id
        WHERE obs.person_id = p.patient_id
        AND obs.concept_id = 165708
        AND enc.form_id = 27
        AND obs.voided = 0
        AND obs.obs_datetime <= CURDATE()
        ORDER BY obs.obs_datetime DESC
        LIMIT 1 OFFSET 0
    )
) AS `previousDaysOfARVRefil`,

(
    SELECT getconceptval(MAX(IF(obs.concept_id=162240, obs.obs_id, NULL)), 159368, p.patient_id)
    FROM obs
    WHERE obs.person_id = p.patient_id
    AND obs.obs_datetime < (
        SELECT MAX(obs.obs_datetime)
        FROM obs
        JOIN encounter enc ON obs.encounter_id = enc.encounter_id
        WHERE obs.person_id = p.patient_id
        AND obs.concept_id = 165708
        AND enc.form_id = 27
        AND obs.voided = 0
    )
) AS `previousDaysOfARVRefiltest`,

(
    SELECT getconceptval(MAX(IF(obs.concept_id=162240, obs.obs_id, NULL)), 159368, p.patient_id)
    FROM obs
    WHERE obs.person_id = p.patient_id
    AND obs.obs_datetime = (
        SELECT MAX(obs.obs_datetime)
        FROM obs
        JOIN encounter enc ON obs.encounter_id = enc.encounter_id
        WHERE obs.person_id = p.patient_id
        AND obs.concept_id = 165708
        AND enc.form_id = 27
        AND obs.voided = 0
        AND obs.obs_datetime <= CURDATE()
        AND obs.obs_datetime < (
            SELECT MAX(obs.obs_datetime)
            FROM obs
            JOIN encounter enc ON obs.encounter_id = enc.encounter_id
            WHERE obs.person_id = p.patient_id
            AND obs.concept_id = 165708
            AND enc.form_id = 27
            AND obs.voided = 0
        )
    )
) AS `previousDaysOfARVRefil2`








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
df2.to_excel("df3.xlsx")
