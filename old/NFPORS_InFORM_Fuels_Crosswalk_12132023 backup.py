# Script will crosswalk data from NFPORS to InFORM Fuels table
# Will be desinged to work with multiple bureaus data
# Default will be to read an Excel file download from NFPORS
# Can be modified to read a CSV file download from NFPORS

import os
import sys
import re
import uuid
import pandas as pd
import arcpy
from collections import Counter


#------------------------------------------------------------------------------------------------------------------------------
# functions

# describe an objects fields and field types
def describe(layer):
     # Establish a Describe object for the table
    desc = arcpy.Describe(layer)

    # Create a list to store the field names (headers)
    field_names = []

    # Loop through the fields in the table and append their names to the list
    for field in desc.fields:
        field_names.append(field.name)
        print(f"Field Name: {field.name}, Field Type: {field.type}")
    return field_names


# print dataframe column names
def allColumns(df):
    for column_name in df.columns:
        print(column_name)


# print spreadsheet column names
def read_spreadsheet_cols(file_path):
    df = pd.read_excel(file_path)
    for column_name in df.columns:
        print(column_name)


# add user inputs

def user_inputs():
    agencies = ["BIA", "BLM", "FWS", "NPS"]
    b_name = ""
    ak_true = "N"

    def get_yes_no_input(prompt):
        """Function to get a 'y' or 'n' input from the user."""
        while True:
            response = input(prompt).lower()
            if response in ['y', 'n']:
                return response
            else:
                print("Invalid input. Please enter 'y' for Yes or 'n' for No.")

    for agency in agencies:
        response = get_yes_no_input(f"Is {agency}? (y/n): ")
        if response == 'y':
            b_name = agency
            is_ak = get_yes_no_input("Is Alaska? (y/n): ")
            if is_ak == 'y':
                ak_true = "y"
            break

    return b_name, ak_true


# return region layer and field name based on bureau
def regions(bureau_abbv, bia_path, fws_path):
    bia = bia_path      
    fws = fws_path


    # return the path to the region layer and the field name for the region
    if bureau_abbv == "BIA":
        return bia, "XXXXX"
    elif bureau_abbv == "FWS":
        return fws, "REGNAME"
 
    
# def regions(bureau_abbv, bia_path, fws_path, nps_path, blm_path):
#     bia = bia_path      
#     fws = fws_path
#     nps = nps_path
#     blm = blm_path

#     # return the path to the region layer and the field name for the region
#     if bureau_abbv == "BIA":
#         return bia, "XXXXX"
#     if bureau_abbv == "FWS":
#         return fws, "REGNAME"
#     if bureau_abbv == "NPS":
#         return nps, "XXXX"
#     if bureau_abbv == "BLM":
#         return blm, "XXXX"
    




#------------------------------------------------------------------------------------------------------------------------------

# Classes

class FieldMapping:
    def __init__(self):
        
        # fields to import from the NFPORS spreadsheet
        self.nfpors_fields_Import = [

            "ActivityTreatmentName",
            "ActivityTreatmentID",
            "TreatmentLocalIdentifier",
            "TypeName",
            "CategoryName",
            "Class",
            "ProjectLatitude",
            "ProjectLongitude",
            "TreatmentLatitude",
            "TreatmentLongitude",
            "PlannedAcres",
            "IsWui",
            "PlannedInitiationDate",
            "PlannedInitiationFiscalYear",
            "PlannedInitiationFiscalQuarter",
            "WBSProjectCode",
            "PlannedDirectCost",
            "DepartmentName",
            "BureauName",
            "RegionName",
            "UnitName",
            "SubUnitName",
            "NwcgUnitID",
            "ForceAccountCost",
            "ServiceContractCost",
            "CoopAgreementCost",
            "ActivityTreatmentNotes",
            "LocalApprovalDate",
            "RegionalApprovalDate",
            "BureauApprovalDate",
            "BILFunding",
            "TreatmentDriver",
            "AcresMonitored",
            "BilGeneralFunds",
            "BilThinningFunds",
            "BilPrescribedFireFunds",
            "BilControlLocationsFunds",
            "BilLaborersFunds",
            "GranteeCost",
            "ProjectNotes",
            "BIL Estimated Personnel Cost",
            "BIL Estimated Asset Cost",
            "BIL Estimated Contractual Cost",
            "BIL Estimated Grants Fixed Costs",
            "BIL Estimated Other Cost",
            "Estimated Success Probability",
            "Implementation Feasibility",
            "Estimated Durability",
            "Treatment Priority", 
            "IsBil",
            "IsRtrl",
            "ProjectIsRtrl",
            "FireRegime",
            "PreTreatmentClass1",
            "PreTreatmentClass2",
            "PreTreatmentClass3"
            
            ]
        
        # Inform Fuels and Post Fire fields 
        self.inform_fields = [

            "OBJECTID",
            "CreatedBy",
            "Unit",
            "Region",
            "Bureau",
            "Department",
            "Latitude",
            "Longitude",
            "CalculatedAcres",
            "IsWUI",
            "InitiationDate",
            "CompletionDate",
            "Notes",
            "LastModifiedBy",
            "CreatedOnDate",
            "LastModifiedDate",
            "Status",
            "StatusReason",
            "ActualTreatmentID",
            "ActualActivityID",
            "EstimatedTreatmentID",
            "EstimatedActivityID",
            "InitiationFiscalYear",
            "InitiationFiscalQuarter",
            "CompletionFiscalYear",
            "CompletionFiscalQuarter",
            "Class",
            "Category",
            "Type",
            "Durability",
            "Priority",
            "FundingSource",
            "CongressionalDistrictNumber",
            "County",
            "State",
            "EstimatedPersonnelCost",
            "EstimatedAssetCost",
            "EstimatedGrantsFixedCost",
            "EstimatedContractualCost",
            "EstimatedOtherCost",
            "EstimatedTotalCost",
            "LocalApprovalDate",
            "RegionalApprovalDate",
            "BureauApprovalDate",
            "DepartmentApprovalDate",
            "FundedDate",
            "EstimatedSuccessProbability",
            "Feasibility",
            "IsApproved",
            "IsFunded",
            "TribeName",
            "IsArchived",
            "Name",
            "IsPoint",
            "Agency",
            "AgencyApprovalDate",
            "TotalAcres",
            "IsDepartmentManual",
            "WBSID",
            "FundingUnit",
            "FundingRegion",
            "FundingAgency",
            "FundingDepartment",
            "FundingTribe",
            "CostCenter",
            "FunctionalArea",
            "CostCode",
            "CancelledDate",
            "HasGroup",
            "GroupCount",
            "UnitID",
            "VegDeparturePercentageDerived",
            "VegDeparturePercentageManual",
            "IsVegetationManual",
            "VegDeparture_Flag",
            "IsRtrl",
            "FundingSubUnit",
            "FundingUnitType",
            "IsBIL",
            "BILFunding",
            "TreatmentDriver",
            "ContributedFundingSource",
            "ContributedNotes",
            "ContributedPersonnelCost",
            "ContributedAssetCost",
            "ContributedGrantsFixedCost",
            "ContributedContractualCost",
            "ContributedOtherCost",
            "ContributedTotalCost",
            "ContributedCostCenter",
            "ContributedFunctionalArea",
            "ContributedCostCode"

            ]

        # Initial crosswalk from NFPORS field names to InFORM field names
        self.nfpors_inform_map = {

            "ActivityTreatmentName": "Name",
            "ActivityTreatmentID": "EstimatedTreatmentID",
            "TreatmentLocalIdentifier": "LocalID",
            "TypeName": "Type",
            "CategoryName": "Category",
            "Class": "Class",
            "TreatmentLatitude": "Latitude",
            "TreatmentLongitude": "Longitude",
            "PlannedAcres": "CalculatedAcres",
            "IsWui": "IsWUI",
            "PlannedInitiationDate": "InitiationDate",
            "PlannedInitiationFiscalYear": "InitiationFiscalYear",
            "PlannedInitiationFiscalQuarter": "InitiationFiscalQuarter",
            "WBSProjectCode": "WBS",
            "PlannedDirectCost": "EstimatedTotalCost", # couldn't use "PlannedDirectCost" twice. Funding Source = Planned direct 
            "DepartmentName": "FundingDepartment",
            "BureauName": "FundingAgency",
            "RegionName": "FundingRegion",
            "UnitName": "FundingUnit",
            "SubUnitName": "FundingSubUnit",
            "NwcgUnitID": "FundingUnitID",
            "ForceAccountCost": "EstimatedPersonnelCost",
            "ServiceContractCost": "EstimatedContractualCost",
            "CoopAgreementCost": "EstimatedGrantsFixedCost",
            "ActivityTreatmentNotes": "Notes",
            "LocalApprovalDate": "LocalApprovalDate",
            "RegionalApprovalDate": "RegionalApprovalDate",
            "BureauApprovalDate": "AgencyApprovalDate",
            "TreatmentDriver": "TreatmentDriver",
            "Estimated Durability":"Durability",
            "Treatment Priority": "Priority",
            "ActivityTreatmentNotes": "Notes",
            "IsBil": "IsBIL",
            "Implementation Feasibility": "Feasibility",
            "Estimated Success Probability": "EstimatedSuccessProbability",
            "IsRtrl": "IsRtrl",
            "BILFunding":"BILFunding"
            }

    def get_lists(self):
        return self.nfpors_fields_Import, self.inform_fields

    def get_dictionary(self):
        return self.nfpors_inform_map
    
    def inform_nfpors_ordered(self):

        # add the nfpors fields from dictionary into new list
    
        nfpors_keys_list = list(self.nfpors_inform_map.keys())

        # unique fields from nfpors to remove at end of crosswalk
        l4 = [x for x in self.nfpors_fields_Import if x not in nfpors_keys_list]

        # combined InFORM fields and unique nfpors fields
        l5 = self.inform_fields + l4

        return l5


#------------------------------------------------------------------------------------------------------------------------------

# Setup


# get user input for agency, Alaska
in_bureau, is_alaska = user_inputs()
print(f"Agency: {in_bureau}, Alaska: {is_alaska}")

print (f"Setting up...")

# Input NFPORS spreadsheet
nfpors_table = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\data input\12122023 FWS Carryover\FWS_FY23_carryover_RAW_for_import.xlsx'

# folder for output 
output_folder = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\data output\12122023 FWS Carryover'

# file name for outputs, no extension
out_name_raw = '11282023 BIA Output'

# Output file for temp CSV
temp_table1 = os.path.join(output_folder, out_name_raw + 'tempCSV.csv')

# output_file for temp excel
temp_table2 = os.path.join(output_folder, out_name_raw + 'tempXLS.xls')

# Final output csv
output_csv = 'FWS_FY23_carryover_for_IFPRS_import_Final.csv'
out_csv_path = os.path.join(output_folder, out_name_raw + ".csv")


# Input file gdb for geoprocessing 
gdb_path = r"C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\data input\data.gdb"

# Check if the file geodatabase exists
if arcpy.Exists(gdb_path):
    # If it exists, delete it
    arcpy.Delete_management(gdb_path)

# Create a new file geodatabase
arcpy.CreateFileGDB_management(os.path.dirname(gdb_path), os.path.basename(gdb_path))

# Set the workspace to your file geodatabase
arcpy.env.workspace = gdb_path  

# enable overwriting
arcpy.env.overwriteOutput = True

#------------------------------------------------------------------------------------------------------------------------------

# Derivation layers

# IF ANY LAYERS ARE CHANGED - NEED TO VERIFY THE FIELDS IN THE DERIVATION SECTION BELOW

# WFDSS Jurisdictional Agency
WFDSS_Jurisdictional_Agency = r'C:\Users\warmstrong\Documents\Data\Jurisdictional\07272023 Jurisdictional_Unit_(Public).gdb\470746db-06af-4720-b20a-e530856939c7.gdb\WFDSS_Jurisdictional_Agency'

# Tribe Name
tribe_name = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\TribeName_allFields'

# Tribal Leaders
tribal_leaders = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\Tribal_Leaders_table'

# State
states = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\States'

# County
county = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\CountyName'

# Regions 
bia_regions = r'C:\Users\warmstrong\Documents\Data\Bureau Regions\data.gdb\bia_regions'
fws_regions = r'C:\Users\warmstrong\Documents\Data\Bureau Regions\data.gdb\fws_regions'
nps_regions = "XXXX"
blm_regions = "XXXX"

# Congressional Districts
con_districts = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\USA_118th_Congressional_Districts.gdb\56f52086-3918-488c-b058-92bc23d4d20a.gdb\USA_118th_Congressional_Districts'

# US Veg Departure (no Alaska) (raster)
us_vegDep = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\BIA Crosswalk\US_220VDEP_150m.tif'

# Hawai Veg Departure (polygon)
hi_vegDep = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\HI_220VDEP_poly'

# US Veg Deperature Index 
us_vegIndex = r''


#------------------------------------------------------------------------------------------------------------------------------

# create field map class object     
fmap = FieldMapping()

print (f"Beginning data crosswalk...")

"""Columns to import from NFPORS spreadsheet"""

# get columns to import from field mapping class
columns_to_include = fmap.nfpors_fields_Import
    

# Specify the data type for lat / long. Convert to string. 
dtype_specification = {
    "PlannedInitiationDate": 'str',
    "PlannedInitiationFiscalYear": 'str',
    "PlannedInitiationFiscalQuarter": 'str',
    "LocalApprovalDate": 'str',
    "RegionalApprovalDate": 'str',
    "BureauApprovalDate": 'str',
}

# Create dataframe
df = pd.read_excel(nfpors_table, usecols=columns_to_include)

# Check if the DataFrame was successfully loaded
if df is None:
    print("Error: Unable to read NFPORS spreadsheet.")


"""1st crosswalk from NFPORS field names to InFORM Fuels field names"""
# These are fiels with 1-to-1 mapping. NFPORS name: InFORM name

# Get the column mapping from the field mapping class
column_mapping = fmap.nfpors_inform_map

# Rename the columns in the DataFrame using the mapping
df.rename(columns=column_mapping, inplace=True)

df['FundingSource'] = ''
 # PlannedDirectCost -> FundingSource -> EstimatedTotalCost
df["EstimatedActivityID"] = df["EstimatedTreatmentID"] # ActivityTreatmentID -> EstimatedTreatmentID -> EstimatedActivityID


"""Entire list of InFORM Fuels fields"""
inForm_fields_all = fmap.inform_fields


# list of all InFORM Fuels columns, and NFPORS columns for ordering and testing 
inForm_nfpors_all = fmap.inform_nfpors_ordered()


# Iterate over the list of columns to check if they exist in the DataFrame
for column_name in inForm_nfpors_all:
    
    if column_name not in df.columns:
        # If not, add the column with default values (NaN)
        df[column_name] = None  # You can set default values here


# Reorder columns based on InFORM Fuels column list
df = df[inForm_nfpors_all]

# Add a GUID for geospatial joins
# Add a new column 'GUID' with generated GUIDs
df['GUID'] = [str(uuid.uuid4()) for _ in range(len(df))]

# Write the DataFrame to the output temp1 CSV
df.to_csv(temp_table1, index=False)


# output table name in the file geodatabase
gis_derivation_table = "InFormFuelsFeatureCsvExtract"

# Convert the CSV to a table in the file geodatabase
arcpy.TableToTable_conversion(in_rows=temp_table1, out_path=gdb_path, out_name=gis_derivation_table)

# full path to output table
gis_derivation_table_fullPath = f'{gdb_path}\\{gis_derivation_table}'

verify_fields = describe(gis_derivation_table_fullPath)


#------------------------------------------------------------------------------------------------------------------------------
# columns that have a one-to-many relationship and/or need a transformation 

print (f"Crosswalking one-to many fields...")

# NFPORS to InFORM Fuels domain crosswalk

table_data = {
    "Biological": ["Biological", "Biocontrol"],
    "Biomass Removal": ["Mechanical", "Biomass Removal"],
    "Broadcast Burn": ["Planned Ignition", "Broadcast"],
    "Risk Assessment": ["Assessment", "Community"],
    "Mitigation Plan": ["Document Preparation", "Community Protection Plan"],
    "Mitigation Plan - Federal Land (fs)": ["Document Preparation", "Community Protection Plan"],
    "Mitigation Plan - Non-Federal Land (fs)": ["Document Preparation", "Community Protection Plan"],
    "Consultation - ESA": ["Compliance", "Endangered Species Act"],
    "Mastication": ["Mechanical", "Grinding, Chipping, Crushing, Mowing"],
    "Mastication/Mowing": ["Mechanical", "Grinding, Chipping, Crushing, Mowing"],
    "Chipping": ["Mechanical", "Grinding, Chipping, Crushing, Mowing"],
    "Crushing": ["Mechanical", "Grinding, Chipping, Crushing, Mowing"],
    "Mowing": ["Mechanical", "Grinding, Chipping, Crushing, Mowing"],
    "Chemical": ["Chemical", "Herbicide"],
    "Grazing": ["Biological", "Herbivory"],
    "Lop and Scatter": ["Mechanical", "Lop and Scatter"],
    "Monitoring": ["Data Collection", "Monitoring"],
    "Appeals and Litigation": ["Compliance", "National Environmental Policy Act"],
    "Nepa - CATX": ["Compliance", "National Environmental Policy Act"],
    "Nepa - EA": ["Compliance", "National Environmental Policy Act"],
    "Nepa - EIS": ["Compliance", "National Environmental Policy Act"],
    "Nepa - HFI CATX": ["Compliance", "National Environmental Policy Act"],
    "Nepa - HFRA EA": ["Compliance", "National Environmental Policy Act"],
    "Nepa - HFRA EIS": ["Compliance", "National Environmental Policy Act"],
    "Nepa - Not Required": ["Compliance", "National Environmental Policy Act"],
    "Consultation - SHPO": ["Compliance", "National Historic Preservation Act"],
    "Hand Pile": ["Mechanical", "Pile"],
    "Machine Pile": ["Mechanical", "Pile"],
    "Jackpot Burn": ["Mechanical", "Pile"],
    "Hand Pile Burn": ["Planned Ignition", "Pile Burn"],
    "Machine Pile Burn": ["Planned Ignition", "Pile Burn"],
    "Seeding": ["Biological", "Seeding"],
    "Thinning": ["Mechanical", "Thinning"],
    "Tribal Indirect": ["Program Management", "Tribal Indirect"],
    "Fire Use": ["Unplanned Ignition", "Wildfire"],
    "Treatment Perimeter Data Management": ["Program Management", "Wildland Fire"],
    "Contract Administration": ["Program Management", "Wildland Fire"],
    "Contract Preparation": ["Program Management", "Wildland Fire"]
}

crosswalk_fields = ["Type", "Category"]

with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, crosswalk_fields) as cursor:
    for row in cursor:
        # Extract values from the current row
        typename = row[0]
        category = row[1] 
        # print (f"nfpors typename {typename}, category {category}")

        # Check if the NFPORS typename exists in the table data dictionary
        for t in table_data:

            if typename.upper() == t.upper():
                # print (f"Match found {t}")
                # Update Category and Type based on the dictionary values
                row[0] = table_data[t][1]  # Update Type
                row[1] = table_data[t][0]  # Update Category
                # print (f"InFORM Fuels type {row[0]}, category {row[1]}")
                
            
        cursor.updateRow(row)
#------------------------------------------------------------------------------------------------------------------------------

update_fields = [
    "Class",    # [0]
    "Latitude",     # [1] This was set to TreatmentLatitude, but should be ProjectLatitude for Activity Class
    "Longitude",    # [2]
    "ProjectLatitude",  # [3]
    "ProjectLongitude",     # [4] 
    "AcresMonitored",   # [5]
    "CalculatedAcres",  # [6]
    "BiLGeneralFunds",  # [7]
    "BilThinningFunds",     # [8]
    "BiLPrescribedFireFunds",   # [9]
    "BiLControlLocationsFunds",     # [10]
    "BilLaborersFunds",     # [11]
    "BIL_Estimated_Personnel_Cost",     # [12]
    "BIL_Estimated_Asset_Cost",     # [13]
    "BIL_Estimated_Contractual_Cost",   # [14]
    "BIL_Estimated_Grants_Fixed_Costs",     # [15]
    "BIL_Estimated_Other_Cost",     # [16]
    "IsPoint",  # [17]
    "FundingSource",    # [18]
    "BILFunding",   # [19]
    "Notes",    # [20]
    "ProjectNotes",     # [21]
    "EstimatedTotalCost",   # [22]
    "Category",     # [23]
    "EstimatedPersonnelCost",   # [24]
    "EstimatedAssetCost",   # [25]
    "EstimatedContractualCost",     # [26]
    "EstimatedGrantsFixedCost", # [27]
    "EstimatedOtherCost",   # [28]
    "EstimatedTreatmentID",     # [29]
    "EstimatedActivityID",  # [30]
    "IsRtrl",   # [31]
    "ProjectIsRtrl",    # [32]
    "IsBIL"     # [33]
]

for f in update_fields:
    if f not in verify_fields:
        print (f"field {f} not in table...")



# Open an update cursor to loop through the feature class
with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, update_fields) as cursor:
    for row in cursor:

        Class = row[0]
        Latitude = row[1]
        Longitude = row[2]
        ProjectLatitude = row[3]
        ProjectLongitude = row[4] 
        AcresMonitored = row[5]
        CalculatedAcres = row[6]
        BILGeneralFunds = row[7]
        BilThinningFunds = row[8]
        BILPrescribedFireFunds = row[9]
        BILControlLocationsFunds = row[10]
        BilLaborersFunds = row[11]
        BILEstimatedPersonnelCost = row[12]
        BILEstimatedAssetCost = row[13]
        BILEstimatedContractualCost = row[14]
        BILEstimatedGrantsFixedCost = row[15]
        BILEstimatedOtherCost = row[16]
        IsPoint = row[17]
        FundingSource = row[18]
        BILFunding = row[19]
        Notes = row[20]
        ProjectNotes = row[21]
        EstimatedTotalCost = row[22]
        category = row[23]
        EstimatedTreatmentID = row[29]
        EstimatedActivityID = row[30]
        IsRtrl = row[31]
        ProjectIsRtrl = row[32]
        IsBil = row[33]
        
        # Treatment or Activity, set ActivityTreatmentID to proper field. Set to Treatment by default
        if Class.rstrip().lower() == "activity":
            row[30] = EstimatedTreatmentID
            row[29] = None
        else:
            row[29] = EstimatedActivityID
            row[30] = None

        # latitude / longitude calculation
        # If "Class" == Activity,  Latitude = ProjectLatitude, Longitude = ProjectLongitude. Else, as is. 
        if Class.rstrip().lower() == "activity":
            row[1] = ProjectLatitude
            row[2] = ProjectLongitude

        # Calculated Acres / is Point
        #If Class is Activity (Column W) default to 10 acres, unless...they crosswalk in as "program management, 
        # (and a few others) " - then they should be flagged as "is Point" and an acres of (X?) assigned
        # Acres Monitored - Should replace the 0 from Acres planned when present

        if CalculatedAcres is None or CalculatedAcres == 0:
            if AcresMonitored is not None and AcresMonitored > 0:
                row[6] = AcresMonitored

        if Class.rstrip().lower() == "activity":
            row[6] = 0
            row[17] = 1


        # If "PlannedDirectCost" (converted to "FundingSource") >= 1, look through "BILGeneralFunds","BILThinningFunds",	
        # "BILPrescribedFireFunds","BILControlLocationsFunds","BILLaborersFunds", for funding source. Else leave as is.

        if IsBil is not None and "n" in IsBil.lower().rstrip():
            # set funding source to 
            row[18] = "Fuels Management Regular (FMReg)"

            # print(f"IsBil {IsBil}")

        
        if IsBil is not None and "y" in IsBil.lower().rstrip():

            # print(f"IsBil {IsBil}")


            # print(f"all costs {BILGeneralFunds}, {BilThinningFunds}, {BILPrescribedFireFunds}, {BILControlLocationsFunds}, {BilLaborersFunds}")

            if BILGeneralFunds is not None and BILGeneralFunds >0:
                # print(f"BILGeneralFunds {BILGeneralFunds}")
                row[18] = "Fuels BIL Regular (FMBilReg)"
            if BilThinningFunds is not None and BilThinningFunds >0:
                # print(f"BilThinningFunds {BilThinningFunds}")
                row[18] = "Fuels BIL Thinning (FMBilThinning)"
            if BILPrescribedFireFunds is not None and BILPrescribedFireFunds >0:
                # print(f"BILPrescribedFireFunds {BILPrescribedFireFunds}")
                row[18] = "Fuels BIL Prescribed Fire (FMBilPrescribedFire)"
            if BILControlLocationsFunds is not None and BILControlLocationsFunds >0:
                # print(f"BILControlLocationsFunds {BILControlLocationsFunds}")
                row[18] = "Fuels BIL Control Locations (FMBilControlLocations)"
            if BilLaborersFunds is not None and BilLaborersFunds >0:
                # print(f"BilLaborersFunds {BilLaborersFunds}")
                row[18] = "Fuels BIL Laborers (FMBilLaborers)"
                
        
        # PlannedDirectCost -> EstimatedTotalCost. 
        # If planned direct costs are <=1  and BIL funding >= 1, use the BIL Funding Total and columns FC- FG to populate total cost and component cost fields

        if EstimatedTotalCost is not None and EstimatedTotalCost <= 1 and BILFunding is not None and BILFunding >= 1:
            
            
            row[22] = BILFunding
            row[24] = BILEstimatedPersonnelCost
            row[25] = BILEstimatedAssetCost
            row[26] = BILEstimatedContractualCost
            row[27] = BILEstimatedGrantsFixedCost  
            row[28] = BILEstimatedOtherCost
            
        # Project Notes, should not overwrite the ActivityTreament Notes, 
        # but should be brough over if ActivityTreatmentNotes are blank
        if Notes is None or Notes == "":
            row[20] = ProjectNotes

        # IsRtrl, ProjectIsRtrl. If RTRL does not match between two NPFORS columns, set to null. 
        if IsRtrl is not None and ProjectIsRtrl is not None and ProjectIsRtrl.lower().rstrip() != IsRtrl.lower().rstrip():
            row[31] = None
            
        cursor.updateRow(row)

#------------------------------------------------------------------------------------------------------------------------------
# Create points feature class from table

# Convert Table to Points
Points_Feature_Class = os.path.join(gdb_path, "InFormFuelsFeatureCsvExtract_Points")
arcpy.management.XYTableToPoint(gis_derivation_table_fullPath, Points_Feature_Class, x_field="Longitude", y_field="Latitude", z_field="", coordinate_system="GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision")

# Add Spatial Index 
Indexed_Points = arcpy.management.AddSpatialIndex(in_features=Points_Feature_Class, spatial_grid_1=0, spatial_grid_2=0, spatial_grid_3=0)

#------------------------------------------------------------------------------------------------------------------------------

# Derivation for Jurisdictional Unit
print (f"Jurisdictional Unit derivations...")

# Unit is set for all bureaus data

# Spatial Join Jurisdictional Agency to Points
arcpy.analysis.SpatialJoin(Indexed_Points, WFDSS_Jurisdictional_Agency, "JU_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,points,GUID,0,8000;JurisdictionalUnitName \"JurisdictionalUnitName\" true true false 100 Text 0 0,First,#,WFDSS_Jurisdictional_Agency,JurisdictionalUnitName,0,100;LegendLandownerCategory \"LegendLandownerCategory\" true true false 20 Text 0 0,First,#,WFDSS_Jurisdictional_Agency,LegendLandownerCategory,0,20;LandownerDepartment \"LandownerDepartment\" true true false 80 Text 0 0,First,#,WFDSS_Jurisdictional_Agency,LandownerDepartment,0,80", match_option="INTERSECT", search_radius="", distance_field_name="")

# Initialize an empty dictionary
ju_dict = {}

# Spatial Join Jurisdictional Units fields
fields = ["GUID", "LandownerDepartment", "JurisdictionalUnitName", "LegendLandownerCategory", ]

# Use a search cursor to iterate through the data and populate the dictionary
with arcpy.da.SearchCursor("JU_sj", fields) as cursor:
    for row in cursor:
        guid = row[0] 

        # location / ownership department 
        land_dept = row[1]
        # location unit
        jur_name = row[2]
        # location agency/bureau
        legend_cat = row[3] 
        val_list = [land_dept, jur_name, legend_cat]
        ju_dict[guid] = val_list

# InFORM Fuels fields
fields = ["GUID", "Department", "Unit", "Bureau"]
with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
    for row in cursor:
        guid = row[0]
        if guid in ju_dict:
            # set department
            row[1] = ju_dict[guid][0]
            # set unit
            row[2] = ju_dict[guid][1]
            # set bureau
            row[3] = ju_dict[guid][2]
        # Update the feature with the new values
        cursor.updateRow(row)

#------------------------------------------------------------------------------------------------------------------------------

# State Derivation
# Spatial Join states to points
print (f"State derivations...")
  
arcpy.analysis.SpatialJoin(Indexed_Points, states, "states_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;STATE_NAME \"STATE_NAME\" true true false 25 Text 0 0,First,#,States,STATE_NAME,0,25", match_option="INTERSECT", search_radius="", distance_field_name="")

state_dict = {}
fields = ["GUID", "STATE_NAME"]

# Use a search cursor to iterate through the data and populate the dictionary
with arcpy.da.SearchCursor("states_sj", fields) as cursor:
    for row in cursor:
        guid = row[0] 
        state = row[1]
        val_list = [state]
        state_dict[guid] = val_list

# InFORM Fuels fields
fields = ["GUID", "State"]
with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
    for row in cursor:
        guid = row[0]
        if guid in state_dict:
            row[1] = state_dict[guid][0]
           
        # Update the feature with the new values
        cursor.updateRow(row)

# calculate state for points
fields = ["GUID", "State"]
with arcpy.da.UpdateCursor(Indexed_Points, fields) as cursor:
    for row in cursor:
        guid = row[0]
        if guid in state_dict:
            row[1] = state_dict[guid][0]
           
        # Update the feature with the new values
        cursor.updateRow(row)


#------------------------------------------------------------------------------------------------------------------------------

# County Derivation 
print (f"County derivations...")

arcpy.analysis.SpatialJoin(Indexed_Points, county, "county_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 255 Text 0 0,First,#,C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;CountyName \"CountyName\" true true false 255 Text 0 0,First,#,CountyName,CountyName,0,255", match_option="INTERSECT", search_radius="", distance_field_name="")

# describe("county_sj")

county_dict = {}
fields = ["GUID", "CountyName"]

# Use a search cursor to iterate through the data and populate the dictionary
with arcpy.da.SearchCursor("county_sj", fields) as cursor:
    for row in cursor:
        guid = row[0] 
        county = row[1]
        val_list = [county]
        county_dict[guid] = val_list

# InFORM Fuels fields
fields = ["GUID", "County"]
with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
    for row in cursor:
        guid = row[0]
        if guid in state_dict:
            row[1] = county_dict[guid][0]
           
        # Update the feature with the new values
        cursor.updateRow(row)

#------------------------------------------------------------------------------------------------------------------------------

# Region Derivation - For BIA, it's BIA Region
print (f"Region derivations...")




# regions function
region_path, region_field = regions(in_bureau, bia_regions, fws_regions)
print(f"Region path: {region_path}, Region field: {region_field}")


if in_bureau == "BIA":
    # Spatial Join BIA regions to points
    arcpy.analysis.SpatialJoin(Indexed_Points, bia_regions, "regions_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;REGIONNAME \"REGIONNAME\" true true false 2000000000 Text 0 0,First,#,BIA_Region,REGIONNAME,0,2000000000", match_option="INTERSECT", search_radius="", distance_field_name="")


elif in_bureau == "FWS":
    # Spatial Join FWS regions to points
    arcpy.analysis.SpatialJoin(Indexed_Points, fws_regions, "regions_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;REGNAME \"REGNAME\" true true false 50 Text 0 0,First,#,fws_regions,REGNAME,0,50;Shape_Length \"Shape_Length\" false true true 8 Double 0 0,First,#,fws_regions,Shape_Length,-1,-1;Shape_Area \"Shape_Area\" false true true 8 Double 0 0,First,#,fws_regions,Shape_Area,-1,-1", match_option="INTERSECT", search_radius="", distance_field_name="")
    # describe("regions_sj")

region_dict = {}
# fields = ["GUID", "REGIONNAME"]
fields = ["GUID", region_field]
# print(fields)


# Use a search cursor to iterate through the data and populate the dictionary
with arcpy.da.SearchCursor("regions_sj", fields) as cursor:
    for row in cursor:
        guid = row[0] 
        region = row[1]
        val_list = [region]
        region_dict[guid] = val_list

# InFORM Fuels fields
fields = ["GUID", "Region"]
with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
    for row in cursor:
        guid = row[0]
        if guid in region_dict:
            row[1] = region_dict[guid][0]
           
        # Update the feature with the new values
        cursor.updateRow(row)

#------------------------------------------------------------------------------------------------------------------------------

# Congressional District Derivation
print (f"Congressional District derivations...")

arcpy.analysis.SpatialJoin(Indexed_Points, con_districts, "cd_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;DISTRICTID \"District ID\" true true false 4 Text 0 0,First,#,USA_118th_Congressional_Districts,DISTRICTID,0,4;STATE_ABBR \"State Abbreviation\" true true false 2 Text 0 0,First,#,USA_118th_Congressional_Districts,STATE_ABBR,0,2", match_option="INTERSECT", search_radius="", distance_field_name="") 

cd_dict = {}
fields = ["GUID", "DISTRICTID", "STATE_ABBR"]

# Use a search cursor to iterate through the data and populate the dictionary
with arcpy.da.SearchCursor("cd_sj", fields) as cursor:
    for row in cursor:
        guid = row[0] 
        district = row[1]
        state_abv = row[2]
        val_list = [f"{state_abv}-{district}"]
        cd_dict[guid] = val_list


# InFORM Fuels fields
fields = ["GUID", "CongressionalDistrictNumber"]
with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
    for row in cursor:
        guid = row[0]
        if guid in cd_dict:
            row[1] = cd_dict[guid][0]
           
        # Update the feature with the new values
        cursor.updateRow(row)

#------------------------------------------------------------------------------------------------------------------------------

# Vegetation Departure Derivation



def vdep(L48_layer, HI_layer):

    # run VDEP derivation on US Veg Departure (raster), then HI Veg Departure (polygon)

    # Extract raster Veg Departure Values to Points
    arcpy.sa.ExtractValuesToPoints(Indexed_Points, L48_layer, "vegDp_extract", interpolate_values="NONE", add_attributes="VALUE_ONLY")

    print (f"Calculating Vegetation Departure for Landfire: {L48_layer}")
    # describe("vegDp_extract")

    # Initialize an empty dictionary
    vegDep_dict = {}

    # Spatial Join Congressional District fields
    fields = ["GUID", "RASTERVALU"]

    # Use a search cursor to iterate through the data and populate the dictionary
    with arcpy.da.SearchCursor("vegDp_extract", fields) as cursor:
        for row in cursor:
            guid = row[0] 
            vd = row[1]
            val_list = [vd]
            vegDep_dict[guid] = val_list

    # InFORM Fuels fields
    fields = ["GUID", "VegDeparturePercentageDerived"]

    with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
        for row in cursor:
            guid = row[0]
            if guid in vegDep_dict:
                row[1] = vegDep_dict[guid][0]
            
            # Update the feature with the new values
            if row[1] is not None:
                cursor.updateRow(row)

    with arcpy.da.SearchCursor(gis_derivation_table_fullPath, fields) as cursor:
        for row in cursor:
            print(f"veg dep {row[1]}")

    # Calculate Hawaii Veg Departure

    HI_query = "state LIKE '%Haw%'"

    # Select points in Hawaii
    arcpy.management.SelectLayerByAttribute(Indexed_Points, "NEW_SELECTION", HI_query)


    fields = ["ProjectNotes"]
    with arcpy.da.UpdateCursor(Indexed_Points, fields) as cursor:
        for row in cursor:
            row[0] = "Hawaii Veg Departure Found"
            cursor.updateRow(row)



# run vegetation departure derivation on US Veg Departure and HI Veg Departure
# veg_departure_layers = [us_vegDep, hi_vegDep]

# only run veg dep derivation if not Alaska
if is_alaska == "n":

    print (f"Vegetation Departure derivations...")

    # run vegetation departure derivation on US Veg Departure and HI Veg Departure
    # veg_departure_layers = [us_vegDep]

    #for layer in veg_departure_layers:
        
    vdep (us_vegDep)


    # Vegetation Depatrure > 100 flag
    # reclassify values > 100

    fields= ["VegDeparturePercentageDerived", "VegDeparture_Flag"]

    with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
            for row in cursor:
                vd = row[0]
                
                if vd is not None and int(vd) >= 100:
                    
                    vd_int = int(vd)
                    # Set flag
                    row[1] = 1
                    # Water/Snow/Ice reclass
                    if vd_int == 111 or vd_int == 112:
                        row[0] = 32
                    # Developed reclass
                    if vd_int == 120:
                        row[0] = 80
                    # Barren/Sparse reclass
                    if vd_int == 132:
                        row[0] = 56
                    # Agriculture reclass
                    if vd_int == 180:
                        row[0] = 68
                # Blank reclass
                elif vd is None or vd == "":
                    row[0] = 50

            cursor.updateRow(row)
#------------------------------------------------------------------------------------------------------------------------------

# Tribe Name and BIA Agency Derivation
if in_bureau == "bia":

    print (f"Tribe Name and BIA Agency derivations...")
    # describe(tribal_leaders)

    # tribal leaders dictionary. Key = Tribe Full Name. Value = BIA Agency 
    tribal_leaders_dict = {}

    fields = ["TribeFullName", "BIAAgency"]

    # Use a search cursor to iterate through the data and populate the dictionary
    with arcpy.da.SearchCursor(tribal_leaders, fields) as cursor:
        for row in cursor:
            tname = row[0] 
            agency = row[1]
            val_list = [agency]
            tribal_leaders_dict[tname] = val_list


    # Spatial join points with Tribal Polygons 
    arcpy.analysis.SpatialJoin(Indexed_Points, tribe_name, "tribes_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="", match_option="INTERSECT", search_radius="", distance_field_name="")

    # create a dictionary from tribal polygons spatially joined to points. Key = Guid, tribe full name, tribe short name. Some points have no tribes
    # Initialize an empty dictionary
    tr_name_dict = {}

    # "TRIBE NAME" = full tribe name"NAME_1" is the abbreviated tribe name. 
    fields = ["GUID", "TRIBE_NAME","NAME_1"]

    # Use a search cursor to iterate through the data and populate the dictionary
    with arcpy.da.SearchCursor("tribes_sj", fields) as cursor:
        for row in cursor:
            guid = row[0] 
            tribe_full_name = row[1]
            tribe_short_name = row[2]
            val_list = [tribe_full_name, tribe_short_name]
            tr_name_dict[guid] = val_list

    #------------------------------------------------------------------------------------------------------------------------------

    # InFORM Fuels fields
    fields = ["GUID", "TribeName", "Unit", "FundingUnit"]
    with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
        for row in cursor:
            guid = row[0]

            # should have all guids in tr_name_dict (created from SJ)
            if guid in tr_name_dict:
                
                # set tribe name to full name
                row[1] = tr_name_dict[guid][0]
                # row[4] = tr_name_dict[guid][0]

            # Update the feature with the new values
            cursor.updateRow(row)

    #--------------------638 Tribes-------------------------

    # Create a list of all of the unique tribes in the NFPORS data
    # these will be any values in NFPORS "Units" that contain the word "Tribe or Tribes"
    # These tribes will be used to calculate values for the 638 tribes
    # The 638 tribes will have their location Unit set to the Tribal Leader BIA Agency
    # The 638 tribes will have their Funding Unit set to the Tribal Leader BIA Agency
    # Need to match the Tribe name from NFPORS to the Tribal Leader name
    # Funding tribe will be set to tribe name 


    # Find all unique tribe Names in the NFPORS data

    # this list will be used to link back to InFORM Fuels
    uniqueTribes = []
    all_units = []

    # using "Funding Unit" as the field to search for tribes, because it was populated with NFPORS Units
    fields = ["FundingUnit"]

    with arcpy.da.SearchCursor(gis_derivation_table_fullPath, fields) as cursor:
        for row in cursor:
            unitVal = row[0] 
            if unitVal not in all_units:
                all_units.append(unitVal)

    for t in all_units:
        t_lower = t.rstrip().lower()
        if "tribe" in t_lower:
            uniqueTribes.append(t)

    # Remove "Tribe" from each string in the list
    for t in uniqueTribes:
        if "Tribes" in t:
            t.replace("Tribes", "")
        elif "Tribe" in t:
            t.replace("Tribe", "")


    # list of tribes modified for searching in the tribal leaders dictionary
    # TribeLookup_list = [string.replace("Tribes", "").replace("Tribe", "").replace("&", "and") for string in uniqueTribes]

    # list with tribe for search, and from NFPORS 
    TribeLookup_list = []

    for tribe in uniqueTribes:
        tribe_strip = tribe.replace("Tribes", "").replace("Tribe", "").replace("&", "and")
        tribes = [tribe_strip, tribe]
        TribeLookup_list.append(tribes)


    for t in TribeLookup_list:
        print (t)


    tribal_638_dict = {}

    for key, value in tribal_leaders_dict.items():
        for all in TribeLookup_list:

            # tribe name cleaned
            t=all[0]
            # Nfpors name
            n=all[1]
            # Remove spaces and hyphens from both the search string and the key
            t_cleaned = re.sub(r'[-\s]', '', t.rstrip())
            key_cleaned = re.sub(r'[-\s]', '', key.rstrip())
            
            t_pattern = re.compile(re.escape(t_cleaned), re.IGNORECASE)
            if t_pattern.search(key_cleaned):
                # print(f"{t} is in {key}")
                val_list = [value[0], n]
                tribal_638_dict[key] = val_list
            # else:
            #     del tribal_leaders_dict[key]

    print (f"\n\n")
    # for key, value in tribal_638_dict.items():
    #     print(f"{key}: {value}")

    # find 638 tribes in table, update Location Unit, Funding Unit, with BIA Agency

    # Loop through the table 
    fields = ["GUID", "TribeName", "Unit", "FundingUnit", "FundingTribe"]


    final_list = []

    with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
        for row in cursor:
            for v in tribal_638_dict:
                if tribal_638_dict[v][1] == row[3]:
                    print(f"Match found Tribe name: {row[1]} and funding unit: {row[3]} and BIA Agency = {tribal_638_dict[v][0]} ")
                    row[2] = tribal_638_dict[v][0]
                    row[3] = tribal_638_dict[v][0]
                    row[4] = row[1]

                # updates the location unit and funding unit with the BIA Agency
                cursor.updateRow(row)


#------------------------------------------------------------------------------------------------------------------------------

# Create output spreadsheets
print (f"Creating output spreadsheets...")

# Convert to excel 
arcpy.conversion.TableToExcel(gis_derivation_table_fullPath, os.path.join(output_folder, temp_table2))

# read excel into df 
df = pd.read_excel(temp_table2)


# remove fields from df that are not in InFORM schema
fields_to_remove = [col for col in df.columns if col not in inForm_fields_all]
df.drop(columns=fields_to_remove, inplace=True)

# write the df to the output CSV

out_csv = df.to_csv(out_csv_path, index=False)

# Now, df contains only the columns specified in list1
print(df)