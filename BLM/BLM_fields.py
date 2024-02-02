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

    # Datetime formatting check
    dt_prompt = "Input spreadsheet date time fields formatted correctly? (y/n) "
    dt_response = input(dt_prompt)


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


# return region field name based on bureau abbreviation  
def regions(bureau_abbv):
   
    if bureau_abbv == "BIA":
        return "REGIONNAME"
    if bureau_abbv == "FWS":
        return "REGNAME"
    if bureau_abbv == "NPS":
        return "XXXX"
    if bureau_abbv == "BLM":
        return "ADMIN_ST"
    

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



   # BLM Specific fields to import from the NFPORS spreadsheet
        self.BLM_nfpors_fields_Import = [

            "NonNFPSameBureauFunds",
            "NFPSameBureau",
            "NonNFPOtherDOIBureauFunds",
            "NFPOtherDOIBureauFunds",
            "OtherFederalDepartmentFunds",
            "StateLocalPrivateFunds",
            "NonGovernmentalOrganizationFunds",
            "TribalFunds",
            "SageGrouseFlag",
            "FundingRegion",
            "FundingUnit",
            "FundingSubUnit"

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



        self.BLM_nfpors_inform_map = {


            "NonNFPSameBureauFunds": "NonNFPSameBureauFunds",
            "NFPSameBureau": "NFPSameBureau",
            "NonNFPOtherDOIBureauFunds": "NonNFPOtherDOIBureauFunds",
            "NFPOtherDOIBureauFunds": "NFPOtherDOIBureauFunds",
            "OtherFederalDepartmentFunds": "OtherFederalDepartmentFunds",
            "StateLocalPrivateFunds": "StateLocalPrivateFunds",
            "NonGovernmentalOrganizationFunds": "NonGovernmentalOrganizationFunds",
            "TribalFunds": "TribalFunds",
            "SageGrouseFlag": "SageGrouseFlag",
            "FundingRegion": "FundingRegion",
            "FundingUnit": "FundingUnit",
            "FundingSubUnit": "FundingSubUnit"
            }


    

    def get_lists(self):
        return self.nfpors_fields_Import, self.inform_fields
    
    def BLM_get_lists(self):
        return self.nfpors_fields_Import + self.BLM_nfpors_fields_Import, self.inform_fields + self.BLM_nfpors_fields_Import


    def get_dictionary(self):
        return self.nfpors_inform_map
    
    def BLM_get_dictionary(self):
        blm_dict = self.nfpors_inform_map.copy()
        blm_dict.update(self.BLM_nfpors_inform_map)
        return blm_dict
   

    
    def inform_nfpors_ordered(self):

        # add the nfpors fields from dictionary into new list
    
        nfpors_keys_list = list(self.nfpors_inform_map.keys())

        # unique fields from nfpors to remove at end of crosswalk
        l4 = [x for x in self.nfpors_fields_Import if x not in nfpors_keys_list]

        # combined InFORM fields and unique nfpors fields
        l5 = self.inform_fields + l4

        return l5
    



FieldMapping=FieldMapping()
    
nfpors, ifpors = FieldMapping.BLM_get_lists()

# for i in ifpors:
#    print (i)

print (FieldMapping.BLM_get_dictionary())
