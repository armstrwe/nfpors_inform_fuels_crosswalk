# Script will crosswalk data from NFPORS to InFORM Fuels table
# Will be desinged to work with multiple bureaus data
# Default will be to read an Excel file download from NFPORS
# Can be modified to read a CSV file download from NFPORS
#


import os
import sys
import pandas as pd

# Replace these file paths with your actual file paths
nfpors_table = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\data input\NfPORS_forTesting.xlsx'
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
    "TreatmentDriver"
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


# Add any InFORM Fuels fields that are not in the dataframe
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


# Iterate over the list of columns to check
for column_name in inForm_fields_all:
    # Check if the column exists in the DataFrame
    if column_name not in df.columns:
        # If not, add the column with default values (NaN)
        df[column_name] = None  # You can set default values here


# Reorder columns based on InFORM Fuels column list
df = df[inForm_fields_all]

# Reorder the columns in the DataFrame to match the mapping
#df = df[column_mapping.values()]



# Write the modified DataFrame to the output CSV
df.to_csv(inform_table, index=False)

print("CSV transformation complete.")