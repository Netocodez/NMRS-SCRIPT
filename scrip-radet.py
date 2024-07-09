from tkinter import ttk
import time
import pandas as pd
import tkinter as tk
from tkinter import filedialog
from tkinter import *
import string
import csv

def script_Radet():
    start_time = time.time()  # Start time measurement
    status_label.config(text=f"Attempting to read file...")
    input_file_path = filedialog.askopenfilename(initialdir = '/Desktop', 
                                                    title = 'Select a excel file', 
                                                    filetypes = (('excel file','*.xls'), 
                                                                ('excel file','*.csv'),
                                                                ('excel file','*.xlsx')))
    if not input_file_path:  # Check if the file selection was cancelled
        status_label.config(text="No file selected. Please select a file to convert.")
        return  # Exit the function
    df = pd.read_csv(input_file_path, dtype=object, quoting=csv.QUOTE_NONE, on_bad_lines='skip')

    #Clean strings
    dfstr = ['Facility_Name','Patient_Name','Patient_Address','Patient_LGA','RegimenLineAtARTStart','CurrentRegimenLine','CurrentPregnancyStatus','TBStatus','CurrentARTStatus','Appointment_Status','TransferInStatus','Date_Generated']
    for col in dfstr:
        df[col] = df[col].str.replace('\"', '')
        
    #Clean Dates
    dfDates = ['DOB','EnrollmentDate','ART_Start_Date','Pharmacy_LastPickupdate','Clinic_Visit_Lastdate','DateofCurrentViralLoad','LastDateOfSampleCollection','ViralLoadReportedDate','CurrentWeightDate','CurrentHeightDate','TBStatusDate','INHStartDate','INHStopDate','LastINHDispensedDate','TBTreatmentStartDate','TBTreatmentStopDate','OTZStartDate','OTZStopDate','DTGFirstPickUp','DateofFirstDTGPickup','Next_Visit_Date','Biometric_date','Date_Generated']
    #date_format = "%d-%m-%y"
    for col in dfDates:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    df.insert(0, 'S/N.', df.index + 1)
    #df.insert(4, 'Patient id', df.index + 1)
    df['LGA']=''
    df['Patient id']=''
    df['Household Unique No']=''
    df['Received OVC Service?']=''
    df.loc[df['Sex']=="M",'Sex']='Male'
    df.loc[df['Sex']=="F",'Sex']='Female'
    df['IIT DATE'] = pd.to_datetime(df['Pharmacy_LastPickupdate']) + pd.to_timedelta(df['DaysOfARVRefill'], unit='D',errors='coerce') + pd.Timedelta(days=29)
    df['DaysOfARVRefill']=df['DaysOfARVRefill'].astype('float')
    df['DaysOfARVRefill'] = (df['DaysOfARVRefill'] / 30).round(1)
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
    df['If Dead, Cause of Dead']=''
    df['VA Cause of Dead']=''
    df['If Transferred out, new facility']=''
    df['Reason for Transfer-Out / IIT / Stooped Treatment']=''
    df['ART Enrollment Setting']=''
    df['Date Commenced DMOC (yyyy-mm-dd)']=''
    df['Type of DMOC']=''
    df['Date of Return of DMOC Client to Facility (yyyy-mm-dd)']=''
    df['Date of Commencement of EAC (yyyy-mm-dd)']=''
    df['Number of EAC Sessions Completed']=''
    df['Date of 3rd EAC Completion (yyyy-mm-dd)']=''
    df['Date of Extended EAC Completion (yyyy-mm-dd)']=''
    df['Date of Repeat Viral Load - Post EAC VL Sample Collected (yyyy-mm-dd)']=''
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
    #df.loc[(df['CurrentARTStatusWithPillBalance'] == 'InActive') & (df['PatientOutcomeDate'].isna()), 'PatientOutcomeDate'] = df['IIT DATE']
    #df.loc[(df['CurrentARTStatusWithPillBalance'] == 'Active') & (df['PatientOutcomeDate'].isna()), 'PatientOutcomeDate'] = df['LastPickupDate']
    df.loc[df['CurrentARTStatus']=="Death",'CurrentARTStatus']='Dead'
    df.loc[df['CurrentARTStatus']=="Discontinued Care",'CurrentARTStatus']='Stopped'
    df.loc[df['CurrentARTStatus']=="Transferred out",'CurrentARTStatus']='Transferred Out'
    df.loc[df['CurrentARTStatus']=="LTFU",'CurrentARTStatus']='IIT'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="Death",'CurrentARTStatusWithPillBalance']='Dead'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="Discontinued Care",'CurrentARTStatusWithPillBalance']='Stopped'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="Transferred out",'CurrentARTStatusWithPillBalance']='Transferred Out'
    #df.loc[df['CurrentARTStatusWithPillBalance']=="InActive",'CurrentARTStatusWithPillBalance']='IIT'
    #df.loc[df['ARTStatusPreviousQuarter']=="LTFU",'ARTStatusPreviousQuarter']='IIT'
    #df.loc[df['ARTStatusPreviousQuarter']=="Discontinued Care",'ARTStatusPreviousQuarter']='Stopped'
    #df.loc[df['ARTStatusPreviousQuarter']=="Transferred out",'ARTStatusPreviousQuarter']='Transferred Out'
    #df.loc[df['ARTStatusPreviousQuarter']=="Death",'ARTStatusPreviousQuarter']='Dead'
    df['ARTStatusPreviousQuarter']=''
    df['PatientOutcomeDatePreviousQuarter']=''
    df['Date of Current Viral Load (yyyy-mm-dd)']=df['DateofCurrentViralLoad']
    #df.loc[df['Date of Current Viral Load (yyyy-mm-dd)'].isna(),'Date of Current Viral Load (yyyy-mm-dd)']=df['AssayDate']
    #df.loc[df['Date of Current Viral Load (yyyy-mm-dd)'].isna() & (df['CurrentViralLoad(c/ml)'].notnull()),'Date of Current Viral Load (yyyy-mm-dd)']=df['ViralLoadEncounterDate']
    df['Date of Viral Load Sample Collection (yyyy-mm-dd)']=df['LastDateOfSampleCollection']
    #df.loc[df['Date of Viral Load Sample Collection (yyyy-mm-dd)'].isna(),'Date of Viral Load Sample Collection (yyyy-mm-dd)']=df['ViralLoadSampleCollectionDate']
    #df.loc[df['Date of Viral Load Sample Collection (yyyy-mm-dd)'].isna(),'Date of Viral Load Sample Collection (yyyy-mm-dd)']=df['LastSampleTakenDate']
    df['ARTStatusPreviousQuarter']=''
    df['PatientOutcomeDatePreviousQuarter']=''
    df['PatientOutcomeDate']=''


    #rearrange columns
    df = df[['S/N.',
                'State',
                'LGA',
                'Facility_Name',
                'Patient id',
                'HospitalNo',
                'ART_ID',
                'Household Unique No',
                'Received OVC Service?',
                'Sex',
                'CurrentWeight_Kg',
                'DOB',
                'ART_Start_Date',
                'Pharmacy_LastPickupdate',
                'DaysOfARVRefill',
                'LastINHDispensedDate',
                'TPT Type',
                'TPT Completion date (yyyy-mm-dd)',
                'RegimenLineAtARTStart',
                'RegimenAtARTStart',
                'CurrentRegimenLine',
                'CurrentARTRegimen',
                'Date of Regimen Switch/ Substitution (yyyy-mm-dd)',
                'CurrentPregnancyStatus',
                'Date of Full Disclosure (yyyy-mm-dd)',
                'OTZStartDate',
                'Number of Support Group (OTZ Club) meeting attended',
                'Number of OTZ Modules completed',
                'Date of Viral Load Sample Collection (yyyy-mm-dd)',
                'CurrentViralLoad',
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
                'Biometric_date',
                'Biometric_Status',
                'IIT Chance (%)',
                'Date calculated (yyyy-mm-dd)',
                'Case Manager']]

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
    
    #def start_processing():
    progress_bar['maximum'] = len(df)
    for index, row in df.iterrows():
                
        # Update the progress bar value
        progress_bar['value'] = index + 1
        
        # Calculate the percentage of completion
        percentage = ((index + 1) / len(df)) * 100
        
        # Update the status label with the current percentage
        status_label.config(text=f"Converting Date Text to Date: {index + 1}/{len(df)} ({percentage:.2f}%)")
        
        # Update the GUI to reflect changes
        root.update_idletasks()
        
        # Simulate time-consuming task
        time.sleep(0.000001)

    #format and export
    output_file_name = input_file_path.split("/")[-1][:-4]
    output_file_path = filedialog.asksaveasfilename(initialdir = '/Desktop', 
                                                    title = 'Select a excel file', 
                                                    filetypes = (('excel file','*.xls'), 
                                                                ('excel file','*.xlsx')),defaultextension=".xlsx", initialfile=output_file_name)
    if not output_file_path:  # Check if the file save was cancelled
        status_label.config(text="File conversion was cancelled. No file was saved.")
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

    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    # Update the status label upon completion
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


# Creating Main Window
root = tk.Tk()
root.title("NETO's NMRS TO RADET CONVERTER V001")
root.geometry("650x400")
root.config(bg="#f0f0f0")

# Adding a Button to the Window
convert_button = tk.Button(root, text="SELECT FILE & CONVERT...", command=script_Radet, font=("Helvetica", 14), bg="#4caf50", fg="#ffffff")
convert_button.pack(pady=0.5)
tooltip = CreateToolTip(convert_button, "Only Requires NMRS ART line list,\n after which you will be prompted to save. \n Does not append patient ID\n and Case Manager column info.")
#text1 = tk.Label(root, text="Output's empty patiend id! (requires only NMRS file -> Save Converted Radet)")
#text1.pack(pady=1)
#convert_button = tk.Button(root, text="SELECT FILE & CONVERT +", command=BASELINE_FILE, font=("Helvetica", 14), bg="#4caf50", fg="#ffffff")
#convert_button.pack(pady=0.5)
#text1 = tk.Label(root, text="Output's Patient Id and Case Manager (requires: Baseline Radet -> NMRS file -> Save Converted Radet)")
#text1.pack(pady=1)
#tooltip = CreateToolTip(convert_button, "File Requirement Procedure:\n (Baseline RADET -> NMRS ART line list).\n After which you will be prompted to save. \n Auto appends patient ID and Case Manager\n column to the converted file.")
convert_button1 = tk.Button(root, text="EXIT CONVERTER", command=root.destroy, font=("Helvetica", 14), bg="red", fg="#ffffff")
convert_button1.pack(pady=25)

def process_data(row):
    time.sleep(0.00005)  # Simulating a time-consuming task
        
# Progress bar widget
progress_bar = ttk.Progressbar(root, orient='horizontal', length=300, mode='determinate')
progress_bar.pack(pady=15)

# Label for percentage and progress messages
status_label = tk.Label(root, text="0%", font=('Helvetica', 12))
status_label.pack(pady=5)

status_label2 = tk.Label(root, text='Welcome to NMRS ART line list to RADET converter ', bg="#D3D3D3", font=('Helvetica', 9))
status_label2.pack(pady=15)

text3 = tk.Label(root, text="you will be prompted to select required files and the location you want to save the converted file \n(Please Remember to follow the file requirement procedures)")
text3.pack(pady=1)

text2 = tk.Label(root, text="Contacts: email: chinedum.pius@gmail.com, phone: +2348134453841")
text2.pack(pady=19)


# Adding File Dialog
filedialog = tk.filedialog 

# Running the GUI
root.mainloop()

#pyinstaller nmrs-radet.py --onefile --windowed --upx-dir "C:\upx-4.2.4-win64"
#python -m nuitka --windows-console-mode=disable --onefile --enable-plugin=tk-inter nmrs-radet.py