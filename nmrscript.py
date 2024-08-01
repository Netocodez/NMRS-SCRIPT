from tkinter import ttk
import time
import tkinter as tk
from tkinter import filedialog
from tkinter import *
import mysql.connector as sql
import pandas as pd
from datetime import datetime

today_date = datetime.now().strftime("%Y-%m-%d")
end_date = today_date 


db_connection = sql.connect(
    host='localhost',
    database='openmrs',
    user='root',
    password='root',
    sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'  # Add this line
)


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

db_cursor.execute("""
drop function if exists getoutcome;
""")

db_cursor.execute("""
    CREATE DEFINER=`root`@`localhost` FUNCTION `getoutcome`(`Pharmacy_LastPickupdate` date,`daysofarvrefill` numeric,`LTFUdays` numeric, `today_date` date) RETURNS text CHARSET utf8
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
""")
def nmrsquery():
    start_time = time.time()  # Start time measurement
    #status_label.config(text=f"Pulling data from NMRS database...")
    total_rows, df2 = biometrics()
    status_label2.config(text=f"Please be patient this will take approximately: {0.005 * total_rows} minutes")
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
        
           
    db_cursor = db_connection.cursor()
    db_cursor.execute("""

    SELECT 
    pid1.uuid,
    global_property.property_value as DatimCode,
    pid1.identifier as HopitalNumber,
    pid2.identifier as UniqueID,
    pe.gender as Sex,
    CONCAT(pn.given_name, ' ', pn.family_name) AS Patient_Name,
    CAST(psn_atr.value AS CHAR) AS Phone_No,
    pa.address1 AS Patient_Address,
    pe.birthdate as DOB,
    TIMESTAMPDIFF(YEAR,pe.birthdate,CURDATE()) as Age,
    MAX(IF(obs.concept_id=159599,IF(TIMESTAMPDIFF(YEAR,pe.birthdate,obs.value_datetime)>=5,TIMESTAMPDIFF(YEAR,pe.birthdate,obs.value_datetime),@ageAtStart:=0),null)) as  `AgeAtStartOfARTYears`,
    MAX(IF(obs.concept_id=159599,IF(TIMESTAMPDIFF(YEAR,pe.birthdate,obs.value_datetime)<5,TIMESTAMPDIFF(MONTH,pe.birthdate,obs.value_datetime),null),null)) as `AgeAtStartOfARTMonths`,
    CASE obs.concept_id = 165839
            WHEN obs.value_coded = 160529 THEN 'TB'
            WHEN obs.value_coded = 160546 THEN 'STI'
            WHEN obs.value_coded = 5271 THEN 'FP'
            WHEN obs.value_coded = 160542 THEN 'OPD'
            WHEN obs.value_coded = 161629 THEN 'Ward'
            WHEN obs.value_coded = 5622 THEN 'Other'
            WHEN obs.value_coded = 165788 THEN 'Blood Bank'
            WHEN obs.value_coded = 160545 THEN 'Outreach'
            WHEN obs.value_coded = 165838 THEN 'Standalone HTS'
            WHEN obs.value_coded = 160539 THEN 'VCT'
            WHEN obs.value_coded = 166026 THEN 'ANC'
            WHEN obs.value_coded = 166027 THEN 'L&D'
            WHEN obs.value_coded = 166028 THEN 'POSTnatal'
            WHEN obs.value_coded = 165512 THEN 'PMTCT'
        ELSE NULL 
    END AS setting,
    MAX(IF(obs.concept_id = 159599, obs.value_datetime, NULL)) AS ART_Start_Date,
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
    CURDATE()
    ) ) as `CurrentARTStatus`,
    MAX(IF(obs.concept_id=856,obs.value_numeric, NULL))as `LastViralLoad`,
    MAX(IF(obs.concept_id = 856, STR_TO_DATE(obsmax.last_date, '%Y-%m-%d'), NULL)) AS `LastViralLoadDate`,
    MAX(IF(obs.concept_id=159951,STR_TO_DATE(obs.value_datetime,'%Y-%m-%d'),null)) as `LastSpecimenCollectionDate`,
    MAX(IF(obs2.concept_id=165708,cn2.name,NULL)) AS `RegimenLineAtARTStart`,
    MAX(
    IF(obs2.concept_id=164506,cn2.`name`,
    IF(obs2.concept_id=164513,cn2.`name`,
    IF(obs2.concept_id=164507,cn2.name,
    IF(obs2.concept_id=164514,cn2.name,
    IF(obs2.concept_id=165702,cn2.name,
    IF(obs2.concept_id=165703,cn2.name,
    NULL
    ))))))) AS `RegimenAtARTStart`,
    MAX(IF(obs.concept_id=165708,cn1.name,NULL) ) AS `CurrentRegimenLine`,
    ( SELECT  cn.`name` FROM `obs` ob  JOIN `concept_name` cn ON cn.`concept_id` = ob.value_coded JOIN encounter e ON ob.encounter_id=e.encounter_id
        WHERE ob.`concept_id` IN (164506,164513,165702,164507,164514,165703)  AND cn.`locale` = 'en' AND cn.`locale_preferred` = 1 
        AND ob.`obs_datetime` <= CURDATE()
        AND ob.`person_id` =  p.`patient_id` 
        AND e.encounter_type=13
        AND ob.voided=0
        AND e.voided=0
        ORDER BY ob.obs_datetime DESC LIMIT 1) AS `CurrentARTRegimen`,
    MAX(IF(obs.concept_id=165050,cn1.name,NULL)) AS `CurrentPregnancyStatus`,
    CASE
        WHEN (DATEDIFF(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), NOW()) BETWEEN 1 AND 180) THEN 'Active With Drugs'
        WHEN (DATEDIFF(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), NOW()) BETWEEN -28 AND -1) THEN 'Missed Appointment'
        WHEN (DATEDIFF(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), NOW()) = 0) THEN 'Today Visit'
        WHEN (DATEDIFF(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), NOW()) BETWEEN -90000 AND -29) THEN 'LTFU'
        ELSE 'LTFU'
    END AS 'Appointment_Status', 
    DATE_FORMAT(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), '%d-%b-%Y') AS `Next_Visit_Date`,

    DATEDIFF(DATE_ADD(MAX(IF(obs.concept_id = 165708, container.last_date, null)), INTERVAL MAX(IF(obs.concept_id = 159368, obs.value_numeric, null)) DAY), NOW()) AS Days_To_Schedule,

    MAX(IF(obs.concept_id=165242,cn1.name,null)) as TransferInStatus,
    getconceptval(MAX(IF(obs.concept_id=162240,obs.obs_id,null) ),159368,p.patient_id) AS `DaysOfARVRefill`,
    MAX(IF((obs.concept_id=165708 and enc.form_id=27 AND obs.voided =0),container.last_date,null)) as `Pharmacy_LastPickupdate`,
    MAX(IF(obs.concept_id=165708 AND obs.`obs_datetime` <= CURDATE(),DATE_FORMAT(container.last_date, '%d-%b-%Y'), NULL)) AS `Clinic_Visit_Lastdate`,
    MAX(IF(obs.concept_id = 856, STR_TO_DATE(obsmax.last_date, '%Y-%m-%d'), NULL)) AS `LastViralLoadDate`



    FROM patient p
        LEFT JOIN Patient_identifier pid1 on(pid1.patient_id = p.patient_id and pid1.identifier_type=5 and p.voided=0 and pid1.voided=0)
        LEFT JOIN Patient_identifier pid2 on(pid2.patient_id = p.patient_id and pid2.identifier_type=4 and p.voided=0 and pid2.voided=0)
        LEFT JOIN global_property on(global_property.property='facility_datim_code')
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
        INNER JOIN obs obs2 on(obs2.person_id=p.patient_id and obs2.concept_id=container.concept_id and obs2.obs_datetime=container.first_date and obs2.voided=0 and obs2.obs_datetime<=CURDATE())
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
    df1['Biometric_date']=df1['uuid'].map(df2.set_index('uuid')['Biometric_date'])
    df1['Biometric_Status']=df1['uuid'].map(df2.set_index('uuid')['Biometric_Status'])
    #df = DataFrame(db_cursor.fetchall())
    #df.columns = db_cursor.column_names

    print(df1)
    end_time = time.time()  # End time measurement
    total_time = end_time - start_time  # Calculate total time taken
    status_label.config(text=f"Conversion Complete! Time taken: {total_time:.2f} seconds")
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
    status_label2.config(text=f"Line List Generation Completed and Saved")

def biometrics():
    db_cursor.execute("""
    SELECT 
    pid1.uuid,
    global_property.property_value as DatimCode,
    pid1.identifier as HopitalNumber,
    pid2.identifier as UniqueID,
    patient_program.date_enrolled as EnrollDate,
    CAST(bi.date_created AS DATE) AS Biometric_date,
    IF(bi.date_created IS NOT NULL, 'Yes', 'No') AS Biometric_Status



    FROM patient p
        LEFT JOIN Patient_identifier pid1 on(pid1.patient_id = p.patient_id and pid1.identifier_type=5 and p.voided=0 and pid1.voided=0)
        LEFT JOIN Patient_identifier pid2 on(pid2.patient_id = p.patient_id and pid2.identifier_type=4 and p.voided=0 and pid2.voided=0)
        LEFT JOIN global_property on(global_property.property='facility_datim_code')
        LEFT JOIN biometricinfo bi ON (p.patient_id = bi.patient_id )
        LEFT JOIN patient_program on(patient_program.patient_id=p.patient_id and patient_program.voided=0 and patient_program.program_id=1)
        
        
    GROUP BY pid1.identifier
    """)
    table_rows = db_cursor.fetchall()
    total_rows1 = db_cursor.rowcount
    #print(total_rows1)
    df2 = pd.DataFrame(table_rows, columns=db_cursor.column_names)
    return total_rows1, df2
    

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


# Creating Main Window
root = tk.Tk()
root.title("NETO's NMRS STATE MAKESHIFT GENERATOR V001")
root.geometry("650x400")
root.config(bg="#f0f0f0")

# Adding a Button to the Window
convert_button = tk.Button(root, text="CONNECT & GENERATE LINE LIST...", command=nmrsquery, font=("Helvetica", 14), bg="#4caf50", fg="#ffffff")
convert_button.pack(pady=5)
tooltip = CreateToolTip(convert_button, "Connects and pull data from,\n opeNMRS Database.")
convert_button1 = tk.Button(root, text="EXIT CONVERTER", command=root.destroy, font=("Helvetica", 14), bg="red", fg="#ffffff")
convert_button1.pack(pady=25)

#def process_data(row):
    #time.sleep(0.0000005)  # Simulating a time-consuming task
        
# Progress bar widget
progress_bar = ttk.Progressbar(root, orient='horizontal', length=300, mode='determinate')
progress_bar.pack(pady=15)

# Label for percentage and progress messages
status_label = tk.Label(root, text="0%", font=('Helvetica', 12))
status_label.pack(pady=5)

status_label2 = tk.Label(root, text='Welcome to NMRS ART line list to RADET converter ', bg="#D3D3D3", font=('Helvetica', 13))
status_label2.pack(pady=15)

text3 = tk.Label(root, text="This Application pulls data from NMRS MYSQL DATABASE \n You will also be prompted to save the file when it completes \n(Please Remember to follow the file requirement procedures)")
text3.pack(pady=1)

text2 = tk.Label(root, text="Contacts: email: chinedum.pius@gmail.com, phone: +2348134453841")
text2.pack(pady=19)


# Adding File Dialog
filedialog = tk.filedialog 

# Running the GUI
root.mainloop()




