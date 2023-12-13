import os, sys

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
            "BiLGeneralFunds",
            "BilThinningFunds",
            "BiLPrescribedFireFunds",
            "BiLControlLocationsFunds",
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


if __name__ == "__main__":
    

    fmap = FieldMapping()

    newfield = fmap.inform_nfpors_ordered()
    
    for x in newfield:
        print(x)
        # print(f"{x}\n\n")

    