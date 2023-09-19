# Script will crosswalk data from NFPORS to InFORM Fuels table
# Will be desinged to work with multiple bureaus data
# Default will be to read an Excel file download from NFPORS
# Can be modified to read a CSV file download from NFPORS
#


import os
import sys
import pandas as pd
import arcpy
from collections import Counter

# Replace these file paths with your actual file paths
nfpors_table = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\data input\Edited BIA_3year_data_for_Import_9_14_23 TESTING.xlsx'
inform_table = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\data input\InFormFuelsFeatureCsvExtract BIA.csv'

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

#------------------------------------------------------------
# CSV file encoding detection
# List of possible encodings to try
# encodings_to_try = ['utf-8', 'ISO-8859-1', 'latin1']

# Try reading the input CSV using different encodings
#df = None
#for encoding in encodings_to_try:
#    try:
#        df = pd.read_csv(nfpors_table, encoding=encoding)
#        break  # Break the loop if successful
#    except UnicodeDecodeError:
#        continue  # Try the next encoding if decoding fails

# Check if the DataFrame was successfully loaded
#if df is None:
#    print("Error: Unable to read the CSV file with any encoding.")


# print dataframe columns
def allColumns(df):
    for column_name in df.columns:
        print(column_name)

#------------------------------------------------------------
# Columns to include in the dataframe. Make sure it matches the NFPORS table inputs/field mapping dictionary 

columns_to_include = [
    "ActivityTreatmentName",
    "ActivityTreatmentID",
    "TreatmentLocalIdentifier",
    "TypeName",
    "CategoryName",
    "Class",
    "ProjectLatitude",
    "ProjectLongitude",
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
    "ProjectLatitude",
    "ProjectLongitude",
    "AcresMonitored",
    "BILGeneralFunds",
    "BilThinningFunds",
    "BILPrescribedFireFunds",
    "BILControlLocationsFunds",
    "BilLaborersFunds",
    "GranteeCost",
    "ProjectNotes",
    "BILEstimatedPersonnelCost",
    "BILEstimatedAssetCost",
    "BILEstimatedContractualCost",
    "BILEstimatedGrantsFixedCosts",
    "BILEstimatedOtherCost",
    "EstimatedSuccessProbability",
    "ImplementationFeasibility",
    "EstimatedDurability",
    "TreatmentPriority",
    "IsBil"
   
]



# Specify the data type for lat / long. Convert to string. 
dtype_specification = {
    "PlannedInitiationDate": 'str',
    "PlannedInitiationFiscalYear": 'str',
    "PlannedInitiationFiscalQuarter": 'str',
    "LocalApprovalDate": 'str',
    "RegionalApprovalDate": 'str',
    "BureauApprovalDate": 'str'
}


# Read the input Excel file into a Pandas DataFrame with specified columns
#df = pd.read_excel(nfpors_table, usecols=columns_to_include, dtype=dtype_specification)
df = pd.read_excel(nfpors_table, usecols=columns_to_include)

# allColumns(df)
# sys.exit()


# Check if the DataFrame was successfully loaded
if df is None:
    print("Error: Unable to read the CSV file with any encoding.")


# Mapping from NFPORS table to InFORM Fuels table
column_mapping = {
    "ActivityTreatmentName": "Name",
    "ActivityTreatmentID": "EstimatedTreatmentID",
    "TreatmentLocalIdentifier": "LocalID",
    "TypeName": "Type",
    "CategoryName": "Category",
    "Class": "Class",
    "ProjectLatitude": "Latitude",
    "ProjectLongitude": "Longitude",
    "PlannedAcres": "CalculatedAcres",
    "IsWui": "IsWUI",
    "PlannedInitiationDate": "InitiationDate",
    "PlannedInitiationFiscalYear": "InitiationFiscalYear",
    "PlannedInitiationFiscalQuarter": "InitiationFiscalQuarter",
    "WBSProjectCode": "WBS",
    "PlannedDirectCost": "FundingSource",
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
    "EstimatedDurability":"Durability",
    "EstimatedSuccessProbability":"Priority",
    "ActivityTreatmentNotes":"Notes"

}




# column_mapping = {
#     "ActivityTreatmentName": "Name",
#     "BureauName": "FundingAgency",
#     "TreatmentLatitude": "Latitude",
#     "TreatmentLongitude": "Longitude",
    
# }

# Rename the columns in the DataFrame using the mapping
df.rename(columns=column_mapping, inplace=True)

# allColumns(df)
# sys.exit()

# Handling input NFPORS fields that go to more than one InFORM Fuels field
# copy the 1st reasigned column to a new column
df["EstimatedTotalCost"] = df["FundingSource"]
df["EstimatedActivityID"] = df["EstimatedTreatmentID"]


# All InFORM Fuels columns

inForm_fields_all = [
"OBJECTID",
"Name",
"EstimatedTreatmentID",
"EstimatedActivityID",
"ActualTreatmentID",
"ActualActivityID",
"IsPoint",
"ParentID",
"ParentObjectID",
"IsParentTreatment",
"LocalID",
"Type",
"Category",
"Class",
"OwnershipUnit",
"OwnershipRegion",
"OwnershipAgency",
"OwnershipDepartment",
"CongressionalDistrictNumber",
"TribeName",
"County",
"State",
"Latitude",
"Longitude",
"CalculatedAcres",
"Durability",
"Priority",
"Feasibility",
"IsWUI",
"IsFunded",
"IsApproved",
"EstimatedSuccessProbability",
"InitiationDate",
"InitiationFiscalYear",
"InitiationFiscalQuarter",
"CompletionDate",
"CompletionFiscalYear",
"CompletionFiscalQuarter",
"CostCenter",
"FunctionalArea",
"WBS",
"CostCode",
"FundingSource",
"FundingDepartment",
"FundingAgency",
"FundingRegion",
"FundingUnit",
"FundingSubUnit",
"FundingTribe",
"FundingUnitID",
"EstimatedPersonnelCost",
"EstimatedAssetCost",
"EstimatedContractualCost",
"EstimatedGrantsFixedCost",
"EstimatedOtherCost",
"EstimatedTotalCost",
"Notes",
"LocalApprovalDate",
"RegionalApprovalDate",
"AgencyApprovalDate",
"DepartmentApprovalDate",
"FundedDate",
"Status",
"StatusReason",
"IsArchived",
"LastModifiedDate",
"LastModifiedBy",
"CreatedOnDate",
"CreatedBy",
"CancelledDate",
"BILFunding",
"Vegetation Departure Percentage",
"Vegetation Departure Index",
"TreatmentDriver",
"FundingUnitType"


]


# list of all InFORM Fuels columns, and NFPORS columns for ordering and testing 
inForm_nfpors_all = [
"OBJECTID",
"Name",
"EstimatedTreatmentID",
"EstimatedActivityID",
"ActualTreatmentID",
"ActualActivityID",
"IsPoint",
"ParentID",
"ParentObjectID",
"IsParentTreatment",
"LocalID",
"Type",
"Category",
"Class",
"OwnershipUnit",
"OwnershipRegion",
"OwnershipAgency",
"OwnershipDepartment",
"CongressionalDistrictNumber",
"TribeName",
"County",
"State",
"Latitude",
"Longitude",
"CalculatedAcres",
"Durability",
"Priority",
"Feasibility",
"IsWUI",
"IsFunded",
"IsApproved",
"EstimatedSuccessProbability",
"InitiationDate",
"InitiationFiscalYear",
"InitiationFiscalQuarter",
"CompletionDate",
"CompletionFiscalYear",
"CompletionFiscalQuarter",
"CostCenter",
"FunctionalArea",
"WBS",
"CostCode",
"FundingSource",
"FundingDepartment",
"FundingAgency",
"FundingRegion",
"FundingUnit",
"FundingSubUnit",
"FundingTribe",
"FundingUnitID",
"EstimatedPersonnelCost",
"EstimatedAssetCost",
"EstimatedContractualCost",
"EstimatedGrantsFixedCost",
"EstimatedOtherCost",
"EstimatedTotalCost",
"Notes",
"LocalApprovalDate",
"RegionalApprovalDate",
"AgencyApprovalDate",
"DepartmentApprovalDate",
"FundedDate",
"Status",
"StatusReason",
"IsArchived",
"LastModifiedDate",
"LastModifiedBy",
"CreatedOnDate",
"CreatedBy",
"CancelledDate",
"BILFunding",
"Vegetation Departure Percentage",
"Vegetation Departure Index",
"TreatmentDriver",
"FundingUnitType",
"ProjectLatitude",
"ProjectLongitude",
"AcresMonitored",
"BILGeneralFunds",
"BilThinningFunds",
"BILPrescribedFireFunds",
"BILControlLocationsFunds",
"BilLaborersFunds",
"IsBil",
"GranteeCost",
"ProjectNotes",
"BILEstimatedPersonnelCost",
"BILEstimatedGrantsFixedCosts",
"BILEstimatedAssetCost",
"BILEstimatedContractualCost",
"BILEstimatedOtherCost",
# "EstimatedSuccessProbability", # this was added and renamed in the mapping 
"ImplementationFeasibility",
"EstimatedDurability",  
"TreatmentPriority"


]

# Fields to delete before saving to InFORM Fuels. 
del_fields = [
"ProjectLatitude",
"ProjectLongitude",
"AcresMonitored",
"BILGeneralFunds",
"BilThinningFunds",
"BILPrescribedFireFunds",
"BILControlLocationsFunds",
"BilLaborersFunds",
"GranteeCost",
"ProjectNotes",
"BILEstimatedPersonnelCost",
"BILEstimatedAssetCost",
"BILEstimatedContractualCost",
"BILEstimatedGrantsFixedCosts",
"BILEstimatedOtherCost",
# "EstimatedSuccessProbability",
"ImplementationFeasibility",
"EstimatedDurability",  
"TreatmentPriority"
]



# Iterate over the list of columns to check if they exist in the DataFrame
for column_name in inForm_nfpors_all:
    
    if column_name not in df.columns:
        # If not, add the column with default values (NaN)
        df[column_name] = None  # You can set default values here


# Reorder columns based on InFORM Fuels column list
df = df[inForm_nfpors_all]

# allColumns(df)
# sys.exit()

# Write the modified DataFrame to the output CSV
temp_csv = df.to_csv(inform_table, index=False)


# Define the output table name (without the .csv extension)
gis_derivation_table = "InFormFuelsFeatureCsvExtract"

# Convert the CSV to a table in the file geodatabase

# will have to delete the GIS created fields ("CreatedBy", "CreatedOnDate", "LastModifiedBy", "LastModifiedDate", "OBJECTID"")
arcpy.TableToTable_conversion(in_rows=inform_table, out_path=gdb_path, out_name=gis_derivation_table)

# full path to output table
gis_derivation_table_fullPath = f'{gdb_path}\\{gis_derivation_table}'

# Describe the header row of the file geodatabase table


 # Establish a Describe object for the table
desc = arcpy.Describe(f'{gdb_path}\\{gis_derivation_table}')

# Create a list to store the field names (headers)
field_names = []

# Loop through the fields in the table and append their names to the list
for field in desc.fields:
    field_names.append(field.name)

# Print the list of field names
print("Field Names:")
for field_name in field_names:
    print(field_name)

#------------------------------------------------------------

# columns that have a one-to-many relationship or need a transformation 

update_fields = [
    "Class", # [0]
    "Latitude", # [1]
    "Longitude", # [2]
    "ProjectLatitude", # [3]
    "ProjectLongitude", # [4] 
    "AcresMonitored", # [5]
    "CalculatedAcres", # [6]
    "BILGeneralFunds", # [7]
    "BilThinningFunds", # [8]
    "BILPrescribedFireFunds", # [9]
    "BILControlLocationsFunds", # [10]
    "BilLaborersFunds", # [11]
    "BILEstimatedPersonnelCost", # [12]
    "BILEstimatedAssetCost", # [13]
    "BILEstimatedContractualCost", # [14]
    "BILEstimatedGrantsFixedCosts", # [15]
    "BILEstimatedOtherCost", # [16]
    "IsPoint", # [17]
    "FundingSource", # [18]
    "BILFunding", # [19]
    "Notes", # [20]
    "ProjectNotes" # [21]
]



for f in update_fields:
    if f not in field_names:
        print ("\n\n"+f + " not in fields")

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
        FundingSource =row[18]
        BILFunding = row[19]
        Notes = row[20]
        ProjectNotes = row[21]
        
        # latitude / longitude calculation
        # If "Class" == Activity,  Latitude = ProjectLatitude, Longitude = ProjectLongitude. Else, as is. 
        if Class == "Activity":
            Latitude  = ProjectLatitude
            Longitude = ProjectLongitude

        # Calculated Acres
        #If Class is Activity (Column W) default to 10 acres, unless...they crosswalk in as "program management, 
        # (and a few others) " - then they should be flagged as "is Point" and an acres of (X?) assigned
        # Acres Monitored - Should replace the 0 from Acres planned when present
        
        if Class == "Activity":
            if AcresMonitored is not None and AcresMonitored >0:
                CalculatedAcres = AcresMonitored
                IsPoint = 0
            else:                              
                CalculatedAcres = 10
                IsPoint = 0
        
        elif Class == "Program Management":
            CalculatedAcres = 1
            IsPoint = 1
        
        # Funding Source

        # If there is a 1 in "PlannedDirectCost" (converted to "FundingSource"), look through "BILGeneralFunds","BILThinningFunds",	
        # "BILPrescribedFireFunds","BILControlLocationsFunds","BILLaborersFunds", for funding source. Else leave as is.

        if FundingSource == 1:
            if BILGeneralFunds is not None and BILGeneralFunds >0:
                FundingSource = BILGeneralFunds
            elif BilThinningFunds is not None and BilThinningFunds >0:
                FundingSource = BilThinningFunds
            elif BILPrescribedFireFunds is not None and BILPrescribedFireFunds >0:
                FundingSource = BILPrescribedFireFunds
            elif BILControlLocationsFunds is not None and BILControlLocationsFunds >0:
                FundingSource = BILControlLocationsFunds
            elif BilLaborersFunds is not None and BilLaborersFunds >0:
                FundingSource = BilLaborersFunds
            
        # PlannedDirectCost -> EstimatedTotalCost. 
        # If planned direct costs are <=1  and BIL funding >= 1, use the BIL Funding Total and columns FC- FG to populate total cost and component cost fields

        if EstimatedTotalCost <= 1 and BILFunding >= 1:
            EstimatedTotalCost = BILFunding
            EstimatedPersonnelCost = BILEstimatedPersonnelCost
            EstimatedAssetCost = BILEstimatedAssetCost
            EstimatedContractualCost = BILEstimatedContractualCost
            EstimatedGrantsFixedCost = BILEstimatedGrantsFixedCost  #GranteeCost?
            EstimatedOtherCost = BILEstimatedOtherCost

        # Project Notes
        # Should not overwrite the ActivityTreament Notes, 
        # but should be brough over if ActivityTreatmentNotes are blank
        if Notes is None or Notes == "":
            Notes = ProjectNotes

        # Update the feature with the new values
        cursor.updateRow(row)


# ------------------------------------------------------------
# Add specific field transformations and multiple input column logic here



# latitude
# Activities use nfpors field "ProjectLatitude"
# 1. If "Class" == Activity,  Latitude = ProjectLatitude, Longitude = ProjectLongitude. Else, as is. 












# 3. FundingSource/PlannedDirectCost 

# 4. FundingUnit/UnitName. For BIA, there is a crosswalk in MBS code. Nothing to do?

#

# search_column = 'Class'
# lat_update_column = 'OtherColumn'
# lat_update_column = 'OtherColumn'

# # Define the search and replace stringsvc
# search_string = 'test'
# replace_string = 'test update'
# replacement_value = 'test int'

# # Iterate through the DataFrame rows
# for index, row in df.iterrows():
#     text_value = row[search_column]
    
#     # Check if the search string is present in the text value
#     if search_string in text_value:
#         # Replace the search string with the replace string in the text column
#         df.at[index, search_column] = text_value.replace(search_string, replace_string)
        
#         # Update the value in the other column with the replacement value
#         df.at[index, update_column] = replacement_value


# longitude 
# Activities use nfpors field "ProjectLongitude"






# Write the modified DataFrame to the output CSV
df.to_csv(inform_table, index=False)

print("CSV transformation complete.")