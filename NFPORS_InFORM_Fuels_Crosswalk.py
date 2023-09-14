# Script will crosswalk data from NFPORS to InFORM Fuels table
# Will be desinged to work with multiple bureaus data
# Default will be to read an Excel file download from NFPORS
# Can be modified to read a CSV file download from NFPORS
#


import os
import sys
import pandas as pd

# Replace these file paths with your actual file paths
nfpors_table = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\data input\hfr_allhazfuelsdata_doi_all.xlsx'
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

df = pd.read_excel(nfpors_table)

# Check if the DataFrame was successfully loaded
if df is None:
    print("Error: Unable to read the CSV file with any encoding.")


# Mapping from NFPORS table to InFORM Fuels table
column_mapping = {
    "ActivityTreatmentName": "Name",
    "ActivityTreatmentID": "EstimatedTreatmentID",
    "ActivityTreatmentID": "EstimatedActivityID",  
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
    "WBS": "WBS",
    "PlannedDirectCost": "FundingSource",
    "DepartmentName": "FundingDepartment",
    "BureauName": "FundingAgency",
    "RegionName": "FundingRegion",
    "UnitName": "FundingUnit",
    "SubUnitName": "FundingSubUnit",
    "NWCGUnitID": "FundingUnitID",
    "ForceAccountCost": "EstimatedPersonnelCost",
    "ServiceContractCost": "EstimatedContractualCost",
    "CoopAgreementCost": "EstimatedGrantsFixedCost",
    "PlannedDirectCost": "EstimatedTotalCost",
    "ActivityTreatmentNotes": "Notes",
    "LocalApprovalDate": "LocalApprovalDate",
    "RegionalApprovalDate": "RegionalApprovalDate",
    "BureauApprovalDate": "AgencyApprovalDate",
    "BILFunding": "BILFunding",
    "TreatmentDriver": "TreatmentDriver",
}

# Rename the columns in the DataFrame using the mapping
df.rename(columns=column_mapping, inplace=True)

# Write the modified DataFrame to the output CSV
df.to_csv(inform_table, index=False)

print("CSV transformation complete.")