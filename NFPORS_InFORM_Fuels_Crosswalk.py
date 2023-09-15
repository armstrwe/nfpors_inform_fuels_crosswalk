# Script will crosswalk data from NFPORS to InFORM Fuels table
# Will be desinged to work with multiple bureaus data
# Default will be to read an Excel file download from NFPORS
# Can be modified to read a CSV file download from NFPORS
#


import os
import sys
import pandas as pd

# Replace these file paths with your actual file paths
nfpors_table = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\data input\Edited BIA_3year_data_for_Import_9_14_23 TESTING.xlsx'
inform_table = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\data input\InFormFuelsFeatureCsvExtract BIA.csv'



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
    "BILThinningFunds",
    "BILPrescribedFireFunds",
    "BILControlLocationsFunds",
    "BILLaborersFunds",
    "GranteeCost",
    "ProjectNotes",
    "BILEstimatedPersonnelCost",
    "BILEstimatedAssetCost",
    "BILEstimatedContractualCost",
    "BILEstimatedGrantsFixedCost",
    "BILEstimatedOtherCost",
    "EstimatedSuccessProbability",
    "ImplementationFeasibility",
    "EstimatedDurability",
    "TreatmentPriority",
   
]


# Specify the data type for lat / long. Convert to string. 
# dtype_specification = {
#     "PlannedDirectCost": 'float'
#}


# Read the input Excel file into a Pandas DataFrame with specified columns
#df = pd.read_excel(nfpors_table, usecols=columns_to_include, dtype=dtype_specification)
df = pd.read_excel(nfpors_table, usecols=columns_to_include)



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
    "BILFunding": "BILFunding",
    "TreatmentDriver": "TreatmentDriver",
    "EstimatedDurability":"Durability",
    "EstimatedSuccessProbability":"Priority",
}




# column_mapping = {
#     "ActivityTreatmentName": "Name",
#     "BureauName": "FundingAgency",
#     "TreatmentLatitude": "Latitude",
#     "TreatmentLongitude": "Longitude",
    
# }

# Rename the columns in the DataFrame using the mapping
df.rename(columns=column_mapping, inplace=True)

# Handling input NFPORS fields that go to more than one InFORM Fuels field
# copy the 1st reasigned column to a new column
df["EstimatedTotalCost"] = df["FundingSource"]
df["EstimatedActivityID"] = df["EstimatedTreatmentID"]


# Add any InFORM Fuels fields that are not in the dataframe. Also add additional 
# nfpors fields for calculations and logic. Remove additional nfpors fields before saving to InFORM Fuels. 

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
"FundingUnitType",
"ProjectLatitude",
"ProjectLongitude",
"AcresMonitored",
"BILGeneralFunds",
"BILThinningFunds",
"BILPrescribedFireFunds",
"BILControlLocationsFunds",
"BILLaborersFunds",
"GranteeCost",
"ProjectNotes",
"BILEstimatedPersonnelCost",
"BILEstimatedAssetCost",
"BILEstimatedContractualCost",
"BILEstimatedGrantsFixedCost",
"BILEstimatedOtherCost",

]

# Fields to delete before saving to InFORM Fuels. 
del_fields = [
"ProjectLatitude",
"ProjectLongitude",
"AcresMonitored",
"BILGeneralFunds",
"BILThinningFunds",
"BILPrescribedFireFunds",
"BILControlLocationsFunds",
"BILLaborersFunds",
"GranteeCost",
"ProjectNotes",
"BILEstimatedPersonnelCost",
"BILEstimatedAssetCost",
"BILEstimatedContractualCost",
"BILEstimatedGrantsFixedCost",
"BILEstimatedOtherCost",
]



# Iterate over the list of columns to check if they exist in the DataFrame
for column_name in inForm_fields_all:
    
    if column_name not in df.columns:
        # If not, add the column with default values (NaN)
        df[column_name] = None  # You can set default values here


# Reorder columns based on InFORM Fuels column list
df = df[inForm_fields_all]

# Reorder the columns in the DataFrame to match the mapping
#df = df[column_mapping.values()]


# ------------------------------------------------------------
# Add specific field transformations and multiple input column logic here

# latitude
# Activities use nfpors field "ProjectLatitude"
# If "Class" is Activity use Project Lat/Long
# Define the column you want to search and the columns you want to update

# search_column = 'Class'
# lat_update_column = 'OtherColumn'
# lat_update_column = 'OtherColumn'

# # Define the search and replace strings
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