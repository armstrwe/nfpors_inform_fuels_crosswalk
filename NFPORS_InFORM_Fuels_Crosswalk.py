import os
import sys
import pandas as pd

# Replace these file paths with your actual file paths
input_csv_path = 'input.csv'
output_csv_path = 'output.csv'

# Read the input CSV into a Pandas DataFrame
df = pd.read_csv(input_csv_path)

# Define the mapping from the first table to the second table
column_mapping = {
    "Name": "ActivityTreatmentName",
    "EstimatedTreatmentID": "ActivityTreatmentID",
    "EstimatedActivityID": "d",
    "LocalID": "TreatmentLocalIdentifier",
    "Type": "TypeName",
    "Category": "CategoryName",
    "Class": "Class",
    "Latitude": "TreatmentLatitude",
    "Longitude": "TreatmentLongitude",
    "CalculatedAcres": "PlannedAcres",
    "IsWUI": "IsWui",
    "InitiationDate": "PlannedInitiationDate",
    "InitiationFiscalYear": "PlannedInitiationFiscalYear",
    "InitiationFiscalQuarter": "PlannedInitiationFiscalQuarter",
    "WBS": "WBS",
    "FundingSource": "PlannedDirectCost",
    "FundingDepartment": "DepartmentName",
    "FundingAgency": "BureauName",
    "FundingRegion": "RegionName",
    "FundingUnit": "UnitName",
    "FundingSubUnit": "SubUnitName",
    "FundingUnitID": "NWCGUnitID",
    "EstimatedPersonnelCost": "ForceAccountCost",
    "EstimatedContractualCost": "ServiceContractCost",
    "EstimatedGrantsFixedCost": "CoopAgreementCost",
    "EstimatedTotalCost": "PlannedDirectCost",
    "Notes": "ActivityTreatmentNotes",
    "LocalApprovalDate": "LocalApprovalDate",
    "RegionalApprovalDate": "RegionalApprovalDate",
    "AgencyApprovalDate": "BureauApprovalDate",
    "BILFunding": "BILFunding",
    "TreatmentDriver": "TreatmentDriver",
}

# Rename the columns in the DataFrame using the mapping
df.rename(columns=column_mapping, inplace=True)

# Write the modified DataFrame to the output CSV
df.to_csv(output_csv_path, index=False)

print("CSV transformation complete.")