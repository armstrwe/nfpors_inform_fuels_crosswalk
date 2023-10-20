import os, sys

# # Define list_1
# list_1 = [
#     "OBJECTID",
#     "CreatedBy",
#     "Unit",
#     "Region",
#     "Bureau",
#     "Department",
#     "Latitude",
#     "Longitude",
#     "CalculatedAcres",
#     "IsWUI",
#     "InitiationDate",
#     "CompletionDate",
#     "Notes",
#     "LastModifiedBy",
#     "CreatedOnDate",
#     "LastModifiedDate",
#     "Status",
#     "StatusReason",
#     "ActualTreatmentID",
#     "InitiationFiscalYear",
#     "InitiationFiscalQuarter",
#     "CompletionFiscalYear",
#     "CompletionFiscalQuarter",
#     "Class",
#     "Category",
#     "Type",
#     "Durability",
#     "Priority",
#     "FundingSource",
#     "CongressionalDistrictNumber",
#     "County",
#     "State",
#     "EstimatedPersonnelCost",
#     "EstimatedAssetCost",
#     "EstimatedGrantsFixedCost",
#     "EstimatedContractualCost",
#     "EstimatedOtherCost",
#     "EstimatedTotalCost",
#     "LocalApprovalDate",
#     "RegionalApprovalDate",
#     "BureauApprovalDate",
#     "DepartmentApprovalDate",
#     "FundedDate",
#     "EstimatedSuccessProbability",
#     "Feasibility",
#     "IsApproved",
#     "IsFunded",
#     "TribeName",
#     "IsArchived",
#     "Name",
#     "IsPoint",
#     "Agency",
#     "AgencyApprovalDate",
#     "TotalAcres",
#     "IsDepartmentManual",
#     "WBSID",
#     "FundingUnit",
#     "FundingRegion",
#     "FundingAgency",
#     "FundingDepartment",
#     "FundingTribe",
#     "CostCenter",
#     "FunctionalArea",
#     "CostCode",
#     "CancelledDate",
#     "HasGroup",
#     "GroupCount",
#     "UnitID",
#     "VegDeparturePercentageDerived",
#     "VegDeparturePercentageManual",
#     "IsVegetationManual",
#     "IsRTRL",
#     "FundingSubUnit",
#     "FundingUnitType",
#     "IsBIL",
#     "BILFunding",
#     "TreatmentDriver",
#     "ContributedFundingSource",
#     "ContributedNotes",
#     "ContributedPersonnelCost",
#     "ContributedAssetCost",
#     "ContributedGrantsFixedCost",
#     "ContributedContractualCost",
#     "ContributedOtherCost",
#     "ContributedTotalCost",
#     "ContributedCostCenter",
#     "ContributedFunctionalArea",
#     "ContributedCostCode",
#     "Shape_Length",
#     "Shape_Area"
# ]

# # Define list_2
# list_2 = [
#     "OBJECTID",
#     "Name",
#     "EstimatedTreatmentID",
#     "EstimatedActivityID",
#     "ActualTreatmentID",
#     "ActualActivityID",
#     "IsPoint",
#     "ParentID",
#     "ParentObjectID",
#     "IsParentTreatment",
#     "LocalID",
#     "Type",
#     "Category",
#     "Class",
#     "OwnershipUnit",
#     "OwnershipRegion",
#     "OwnershipAgency",
#     "OwnershipDepartment",
#     "CongressionalDistrictNumber",
#     "TribeName",
#     "County",
#     "State",
#     "Latitude",
#     "Longitude",
#     "CalculatedAcres",
#     "Durability",
#     "Priority",
#     "Feasibility",
#     "IsWUI",
#     "IsFunded",
#     "IsApproved",
#     "EstimatedSuccessProbability",
#     "InitiationDate",
#     "InitiationFiscalYear",
#     "InitiationFiscalQuarter",
#     "CompletionDate",
#     "CompletionFiscalYear",
#     "CompletionFiscalQuarter",
#     "CostCenter",
#     "FunctionalArea",
#     "WBS",
#     "CostCode",
#     "FundingSource",
#     "FundingDepartment",
#     "FundingAgency",
#     "FundingRegion",
#     "FundingUnit",
#     "FundingSubUnit",
#     "FundingTribe",
#     "FundingUnitID",
#     "EstimatedPersonnelCost",
#     "EstimatedAssetCost",
#     "EstimatedContractualCost",
#     "EstimatedGrantsFixedCost",
#     "EstimatedOtherCost",
#     "EstimatedTotalCost",
#     "Notes",
#     "LocalApprovalDate",
#     "RegionalApprovalDate",
#     "AgencyApprovalDate",
#     "DepartmentApprovalDate",
#     "FundedDate",
#     "Status",
#     "StatusReason",
#     "IsArchived",
#     "LastModifiedDate",
#     "LastModifiedBy",
#     "CreatedOnDate",
#     "CreatedBy",
#     "CancelledDate",
#     "BILFunding",
#     "VegDeparturePercentageManual",
#     "VegDeparturePercentageDerived",
#     "IsVegetationManual",
#     "TreatmentDriver",
#     "FundingUnitType"
# ]

# # Find items in list_1 that are not in list_2
# list_3 = [item for item in list_1 if item not in list_2]

# # Print list_3 to see the items that are in list_1 but not in list_2
# print(list_3)

# Your updated list1 and list2


list1 = [
    "OBJECTID_1",
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
    "ContributedCostCode",
    "ProjectLatitude",
    "ProjectLongitude",
    "AcresMonitored",
    "BiLGeneralFunds",
    "BilThinningFunds",
    "BiLPrescribedFireFunds",
    "BiLControlLocationsFunds",
    "BilLaborersFunds",
    "GranteeCost",
    "ProjectNotes",
    "BIL_Estimated_Personnel_Cost",
    "BIL_Estimated_Grants_Fixed_Costs",
    "BIL_Estimated_Asset_Cost",
    "BIL_Estimated_Contractual_Cost",
    "BIL_Estimated_Other_Cost",
    "Implementation_Feasibility",
    "Treatment_Priority",
    "ProjectIsRtrlFireRegime",
    "PreTreatmentClass1",
    "PreTreatmentClass2",
    "PreTreatmentClass3",
    "VegDeparture_Flag",
    "GUID"
]

list2 = [
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

# Create list3 containing values from list2 that are not in list1
list3 = [value for value in list2 if value not in list1]

# Output list3
print(list3)
