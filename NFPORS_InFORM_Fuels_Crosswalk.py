# Script will crosswalk data from NFPORS to InFORM Fuels table
# Will be desinged to work with multiple bureaus data
# Default will be to read an Excel file download from NFPORS
# Can be modified to read a CSV file download from NFPORS
#


import os
import sys
import uuid
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

# enable overwriting
arcpy.env.overwriteOutput = True

# Derivation layers

# IF ANY LAYERS ARE CHANGED - NEED TO VERIFY THE FIELDS IN THE DERIVATION SECTION BELOW

# WFDSS Jurisdictional Agency
WFDSS_Jurisdictional_Agency = r'C:\Users\warmstrong\Documents\Data\Jurisdictional\07272023 Jurisdictional_Unit_(Public).gdb\470746db-06af-4720-b20a-e530856939c7.gdb\WFDSS_Jurisdictional_Agency'

# Tribe Name
tribe_name = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\TribeName'

# Tribal Leaders
tribal_leaders = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\Tribal_Leaders_table'

# State
states = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\States'

# County
county = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\CountyName'

# Regions 
bia_regions = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\BIA_Region'

# Congressional Districts
con_districts = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\USA_118th_Congressional_Districts.gdb\56f52086-3918-488c-b058-92bc23d4d20a.gdb\USA_118th_Congressional_Districts'

# US Veg Departure
us_vegDep = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\BIA Crosswalk\US_220VDEP'

# Hawai Veg Departure
hi_vegDep = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\HI_220VDEP'


# functions
def describe(layer):
     # Establish a Describe object for the table
    desc = arcpy.Describe(layer)

    # Create a list to store the field names (headers)
    field_names = []

    # Loop through the fields in the table and append their names to the list
    for field in desc.fields:
        field_names.append(field.name)

    # Print the list of field names
    print(f"\n\nField Names for {layer}:")
    for field_name in field_names:
        print(field_name)





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
    "TreatmentLatitude": "Latitude",
    "TreatmentLongitude": "Longitude",
    "PlannedAcres": "CalculatedAcres",
    "IsWui": "IsWUI",
    "PlannedInitiationDate": "InitiationDate",
    "PlannedInitiationFiscalYear": "InitiationFiscalYear",
    "PlannedInitiationFiscalQuarter": "InitiationFiscalQuarter",
    "WBSProjectCode": "WBS",
    "PlannedDirectCost": "FundingSource", # couldn't use "PlannedDirectCost" twice. Funding Source = Planned direct 
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
df["EstimatedTotalCost"] = df["FundingSource"]  # PlannedDirectCost -> FundingSource -> EstimatedTotalCost
df["EstimatedActivityID"] = df["EstimatedTreatmentID"] # ActivityTreatmentID -> EstimatedTreatmentID -> EstimatedActivityID


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
"VegDeparturePercentageManual",
"VegDeparturePercentageDerived",
"IsVegetationManual",
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
"VegDeparturePercentageManual",
"VegDeparturePercentageDerived",
"IsVegetationManual",
"TreatmentDriver",
"FundingUnitType",
"ProjectLatitude",  # project lat long for Activity class
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
"Durability",  
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

# Add a GUID for geospatial joins
# Add a new column 'GUID' with generated GUIDs
df['GUID'] = [str(uuid.uuid4()) for _ in range(len(df))]


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

# describe(gis_derivation_table_fullPath)



#------------------------------------------------------------

# columns that have a one-to-many relationship or need a transformation 

update_fields = [
    "Class", # [0]
    "Latitude", # [1] This was set to TreatmentLatitude, but should be ProjectLatitude for Activity Class
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
    "ProjectNotes", # [21]
    "EstimatedTotalCost", # [22]
    "Category", # [23]
    "EstimatedPersonnelCost", # [24]
    "EstimatedAssetCost", # [25]
    "EstimatedContractualCost", # [26]
    "EstimatedGrantsFixedCost",# [27]
    "EstimatedOtherCost", # [28]
]



# for f in update_fields:
#     if f not in field_names:
#         print ("\n\n"+f + " not in fields")

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
        
        
        
        # latitude / longitude calculation
        # If "Class" == Activity,  Latitude = ProjectLatitude, Longitude = ProjectLongitude. Else, as is. 
        if Class.rstrip().lower() == "activity":

            # print (f"Activity found. Original lat/long {Latitude}, {Longitude}")

            row[1]  = ProjectLatitude
            row[2] = ProjectLongitude

            # print (f"Activity found: Updated lat/long {row[1]}, {row[2]}")

        # Calculated Acres / is Point
        #If Class is Activity (Column W) default to 10 acres, unless...they crosswalk in as "program management, 
        # (and a few others) " - then they should be flagged as "is Point" and an acres of (X?) assigned
        # Acres Monitored - Should replace the 0 from Acres planned when present

        # print(f"class {Class}, category {category}")
        
        if CalculatedAcres is None or CalculatedAcres == 0:
            if AcresMonitored is not None and AcresMonitored > 0:
                row[6] = AcresMonitored

        if Class.rstrip().lower() == "activity":
            row[6] = 10
            row[17] = 0

        
        # print(f"class {Class.lower()}, category {category.lower()}")
        elif Class.rstrip().lower() == "activity" and category.lower().rstrip() == "program management":
            print(f"found program management")
            row[6] = 1
            row[17] = 1
           
        
        # Funding Source

        # If "PlannedDirectCost" (converted to "FundingSource") >= 1, look through "BILGeneralFunds","BILThinningFunds",	
        # "BILPrescribedFireFunds","BILControlLocationsFunds","BILLaborersFunds", for funding source. Else leave as is.

        if FundingSource >= 1:
            bil_total = 0
            if BILGeneralFunds is not None and BILGeneralFunds >0:
                bil_total += BILGeneralFunds
            if BilThinningFunds is not None and BilThinningFunds >0:
                bil_total += BilThinningFunds
            if BILPrescribedFireFunds is not None and BILPrescribedFireFunds >0:
                bil_total += BILPrescribedFireFunds
            if BILControlLocationsFunds is not None and BILControlLocationsFunds >0:
                bil_total += BILControlLocationsFunds
            if BilLaborersFunds is not None and BilLaborersFunds >0:
                bil_total += BilLaborersFunds
                
            row[18] = bil_total
            
        # PlannedDirectCost -> EstimatedTotalCost. 
        # If planned direct costs are <=1  and BIL funding >= 1, use the BIL Funding Total and columns FC- FG to populate total cost and component cost fields

        if EstimatedTotalCost is not None and EstimatedTotalCost <= 1 and BILFunding is not None and BILFunding >= 1:
            
            row[22] = BILFunding
            row[24] = BILEstimatedPersonnelCost
            row[25] = BILEstimatedAssetCost
            row[26] = BILEstimatedContractualCost
            row[27] = BILEstimatedGrantsFixedCost  
            row[28] = BILEstimatedOtherCost
            

        # Project Notes
        # Should not overwrite the ActivityTreament Notes, 
        # but should be brough over if ActivityTreatmentNotes are blank
        if Notes is None or Notes == "":
            row[20] = ProjectNotes

        # Update the feature with the new values
        cursor.updateRow(row)


# sys.exit()

# ------------------------------------------------------------
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
    "Contract Preparation": ["Program Management", "Wildland Fire"],
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
            
            # print(f"crosswalk typename {t}, category {table_data[t][0]}, type {table_data[t][1]}")

            if typename.upper() == t.upper():

                print (f"Match found {t}")
                # Update Category and Type based on the dictionary values
                row[0] = table_data[t][1]  # Update Type
                row[1] = table_data[t][0]  # Update Category

                print (f"InFORM Fuels type {row[0]}, category {row[1]}")

       
            
        cursor.updateRow(row)



#------------------------------------------------------------
# Create points feature class from table

# Convert Table to Points
Points_Feature_Class = os.path.join(gdb_path, "InFormFuelsFeatureCsvExtract_Points")
arcpy.management.XYTableToPoint(gis_derivation_table_fullPath, Points_Feature_Class, x_field="Longitude", y_field="Latitude", z_field="", coordinate_system="GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision")

# Add Spatial Index 
Indexed_Points = arcpy.management.AddSpatialIndex(in_features=Points_Feature_Class, spatial_grid_1=0, spatial_grid_2=0, spatial_grid_3=0)

sys.exit()

#------------------------------------------------------------
# Derivation for Jurisdictional Unit

# Spatial Join Jurisdictional Agency to Points

arcpy.analysis.SpatialJoin(Indexed_Points, WFDSS_Jurisdictional_Agency, "JU_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,points,GUID,0,8000;JurisdictionalUnitName \"JurisdictionalUnitName\" true true false 100 Text 0 0,First,#,WFDSS_Jurisdictional_Agency,JurisdictionalUnitName,0,100;LegendLandownerCategory \"LegendLandownerCategory\" true true false 20 Text 0 0,First,#,WFDSS_Jurisdictional_Agency,LegendLandownerCategory,0,20;LandownerDepartment \"LandownerDepartment\" true true false 80 Text 0 0,First,#,WFDSS_Jurisdictional_Agency,LandownerDepartment,0,80", match_option="INTERSECT", search_radius="", distance_field_name="")

# describe("JU_sj")

# Initialize an empty dictionary
ju_dict = {}

# Spatial Join Jurisdictional Units fields
fields = ["GUID", "LandownerDepartment", "JurisdictionalUnitName", "LegendLandownerCategory", ]

# Use a search cursor to iterate through the data and populate the dictionary
with arcpy.da.SearchCursor("JU_sj", fields) as cursor:
    for row in cursor:
        guid = row[0] 
        land_dept = row[1]
        jur_name = row[2]
        legend_cat = row[3] 
        
        val_list = [land_dept, jur_name, legend_cat]
        
        ju_dict[guid] = val_list

#for v in ju_dict:
    
    # split_values = [value.split(",") for value in ju_dict[v]]
    #print (ju_dict[v])

    # Print the resulting list
    #print(split_values)

# InFORM Fuels fields
fields = ["GUID", "OwnershipDepartment", "OwnershipUnit", "OwnershipAgency"]
with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
    for row in cursor:
        guid = row[0]
        if guid in ju_dict:
            row[1] = ju_dict[guid][0]
            row[2] = ju_dict[guid][1]
            row[3] = ju_dict[guid][2]
        # Update the feature with the new values
        cursor.updateRow(row)


# with arcpy.da.SearchCursor(gis_derivation_table_fullPath, fields) as cursor:
#     for row in cursor:
#         print(row[1])
#         print(row[2])
#         print(row[3])


#------------------------------------------------------------

# State Derivation


# Spatial Join states to points
  
arcpy.analysis.SpatialJoin(Indexed_Points, states, "states_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;STATE_NAME \"STATE_NAME\" true true false 25 Text 0 0,First,#,States,STATE_NAME,0,25", match_option="INTERSECT", search_radius="", distance_field_name="")

# Initialize an empty dictionary
state_dict = {}

# Spatial Join State fields
fields = ["GUID", "STATE_NAME"]

# Use a search cursor to iterate through the data and populate the dictionary
with arcpy.da.SearchCursor("states_sj", fields) as cursor:
    for row in cursor:
        guid = row[0] 
        state = row[1]

   #     print (guid, state)
       
        val_list = [state]
        
        state_dict[guid] = val_list





# for v in state_dict:
    
    # split_values = [value.split(",") for value in ju_dict[v]]
    # print (state_dict[v])

    # Print the resulting list
    #print(split_values)



# InFORM Fuels fields
fields = ["GUID", "State"]
with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
    for row in cursor:
        guid = row[0]
        if guid in state_dict:
            row[1] = state_dict[guid][0]
           
        # Update the feature with the new values
        cursor.updateRow(row)

# with arcpy.da.SearchCursor(gis_derivation_table_fullPath, fields) as cursor:
#     for row in cursor:
#         print(row[1])


#------------------------------------------------------------
# 
# County Derivation 
arcpy.analysis.SpatialJoin(Indexed_Points, county, "county_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 255 Text 0 0,First,#,C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;CountyName \"CountyName\" true true false 255 Text 0 0,First,#,CountyName,CountyName,0,255", match_option="INTERSECT", search_radius="", distance_field_name="")


# describe("county_sj")
# sys.exit()


# fields = ["CountyName"]
# with arcpy.da.SearchCursor("county_sj", fields) as cursor:
#      for row in cursor:
#          print(row[0])



# sys.exit()


# Initialize an empty dictionary
county_dict = {}

# Spatial Join State fields
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


# with arcpy.da.SearchCursor(gis_derivation_table_fullPath, fields) as cursor:
#      for row in cursor:
#          print(row[1])




#------------------------------------------------------------

# Region Derivation - For BIA, it's BIA Region

# Spatial Join regions to points
arcpy.analysis.SpatialJoin(Indexed_Points, bia_regions, "regions_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;REGIONNAME \"REGIONNAME\" true true false 2000000000 Text 0 0,First,#,BIA_Region,REGIONNAME,0,2000000000", match_option="INTERSECT", search_radius="", distance_field_name="")

describe("regions_sj")

# Initialize an empty dictionary
region_dict = {}

# Spatial Join State fields
fields = ["GUID", "REGIONNAME"]

# Use a search cursor to iterate through the data and populate the dictionary
with arcpy.da.SearchCursor("regions_sj", fields) as cursor:
    for row in cursor:
        guid = row[0] 
        region = row[1]
       
        val_list = [region]
        
        region_dict[guid] = val_list


# InFORM Fuels fields
fields = ["GUID", "OwnershipRegion"]
with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
    for row in cursor:
        guid = row[0]
        if guid in region_dict:
            row[1] = region_dict[guid][0]
           
        # Update the feature with the new values
        cursor.updateRow(row)

# with arcpy.da.SearchCursor(gis_derivation_table_fullPath, fields) as cursor:
#      for row in cursor:
#         print(row[1])


#----------------------------------------------------------------------------

# Congressional District Derivation

arcpy.analysis.SpatialJoin(Indexed_Points, con_districts, "cd_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;DISTRICTID \"District ID\" true true false 4 Text 0 0,First,#,USA_118th_Congressional_Districts,DISTRICTID,0,4;STATE_ABBR \"State Abbreviation\" true true false 2 Text 0 0,First,#,USA_118th_Congressional_Districts,STATE_ABBR,0,2", match_option="INTERSECT", search_radius="", distance_field_name="") 

# describe("cd_sj")

# Initialize an empty dictionary
cd_dict = {}

# Spatial Join Congressional District fields
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

# with arcpy.da.SearchCursor(gis_derivation_table_fullPath, fields) as cursor:
#      for row in cursor:
#          print(row[1])


#---------------------------------------------------------------------------------- 

# Vegetation Departure Derivation
# US Veg Departure

# Extract raster Veg Departure Values to Points 

def vdep(layer):

    arcpy.sa.ExtractValuesToPoints(Indexed_Points, layer, "vegDp_extract", interpolate_values="NONE", add_attributes="VALUE_ONLY")

    print (f"Calculating Vegetation Departure for Landfire: {layer}")
    # describe("vegDp_extract")


    # Only calculate veg dep if there are points in the layer
    # Use GetCount_management to count the features
    # result = arcpy.GetCount_management("vegDp_extract")

    # # Get the count as an integer
    # count = int(result.getOutput(0))



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


# run vegetation departure derivation on US Veg Departure and HI Veg Departure
veg_departure_layers = [us_vegDep, hi_vegDep]

for layer in veg_departure_layers:
    vdep(layer)



#------------------------------------------------------------------------------------------------

# Tribe Name and BIA Agency Derivation


# describe(tribal_leaders)

tribe_name
tribal_leaders

# tribal leaders dictionary 
tribal_leaders_dict = {}

fields = ["tribefullname", "biaagency"]


# Use a search cursor to iterate through the data and populate the dictionary
with arcpy.da.SearchCursor(tribal_leaders, fields) as cursor:
    for row in cursor:
        tname = row[0] 
        agency = row[1]
        
        val_list = [agency]
        
        tribal_leaders_dict[tname] = val_list


# for v in tribal_leaders_dict:
    
#     # split_values = [value.split(",") for value in ju_dict[v]]
#     print (f"{v}: tribal_leaders_dict[v]")



# if repeating for BIA with new data, run this code on first import
# Process: Add Field (Add Field) (management)
# arcpy.management.AddField(tribe_name, field_name="tribe_name_edit", field_type="TEXT", field_precision=None, field_scale=None, field_length=None, field_alias="", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="")[0]

# # Process: Calculate Field (Calculate Field) (management)
# arcpy.management.CalculateField(tribe_name, field="tribe_name_edit", expression="!NAME!", expression_type="PYTHON3", code_block="", field_type="TEXT", enforce_domains="NO_ENFORCE_DOMAINS")

arcpy.analysis.SpatialJoin(Indexed_Points, tribe_name, "tribes_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="guid \"guid\" true true false 255 Text 0 0,First,#,InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;tribe_name \"tribe_name\" true true false 255 Text 0 0,First,#,TribeName,tribe_name_edit,0,255", match_option="INTERSECT", search_radius="", distance_field_name="")


# Initialize an empty dictionary
tr_name_dict = {}

fields = ["GUID", "tribe_name"]

# Use a search cursor to iterate through the data and populate the dictionary
with arcpy.da.SearchCursor("tribes_sj", fields) as cursor:
    for row in cursor:
        guid = row[0] 
        tr = row[1]

        val_list = [tr]
        
        tr_name_dict[guid] = val_list



        
fields = ["GUID", "TribeName", "OwnershipUnit"]
with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
    for row in cursor:
        guid = row[0] 
        tr_sj = tr_name_dict[guid][0]
        
        # get tribe name from sj 
        # print(f"guid: {tr_name_dict[guid][0]}")

        

        # row[1] = tr_name_dict[guid]

        # check if tribe name in tribal leaders dict
        for key, value in tribal_leaders_dict.items():
            # print(f"key: {key}, value: {value}")
            if tr_sj is not None and tr_sj.rstrip().lower() in key.rstrip().lower():
                print (f"Match found: {key}: {value}")
                row[2] = key
                break


        # Update the feature with the new values
        row[1] = tr_sj
        print (f"row[1] - tribename: {row[1]}")
        print (f"row[2] - ownershipunit: {row[2]}")


        cursor.updateRow(row)


sys.exit()


for v in tr_name_dict:
    
    
    print (f"{v}: {tr_name_dict[v]}")


# InFORM Fuels fields
fields = ["GUID", "TribeName"]
with arcpy.da.UpdateCursor(gis_derivation_table_fullPath, fields) as cursor:
    for row in cursor:
        guid = row[0]
        if guid in tr_name_dict:
            row[1] = tr_name_dict[guid][0]
           
        # Update the feature with the new values
        cursor.updateRow(row)

with arcpy.da.SearchCursor(gis_derivation_table_fullPath, fields) as cursor:
      for row in cursor:
          print(f"Tribal land/tribe {row[1]}")




sys.exit()















# Write the modified DataFrame to the output CSV
df.to_csv(inform_table, index=False)

print("CSV transformation complete.")