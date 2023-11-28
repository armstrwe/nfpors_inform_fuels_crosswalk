

#-------------------------------------------------------------------------------------------------------------------------------

def jurisdictional_unit_derivation(output_table, points, ju_layer):

    # Derivation for Jurisdictional Unit
    # Unit is set for all bureaus data

    # Spatial Join Jurisdictional Agency to Points
    arcpy.analysis.SpatialJoin(points, ju_layer, "JU_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,points,GUID,0,8000;JurisdictionalUnitName \"JurisdictionalUnitName\" true true false 100 Text 0 0,First,#,WFDSS_Jurisdictional_Agency,JurisdictionalUnitName,0,100;LegendLandownerCategory \"LegendLandownerCategory\" true true false 20 Text 0 0,First,#,WFDSS_Jurisdictional_Agency,LegendLandownerCategory,0,20;LandownerDepartment \"LandownerDepartment\" true true false 80 Text 0 0,First,#,WFDSS_Jurisdictional_Agency,LandownerDepartment,0,80", match_option="INTERSECT", search_radius="", distance_field_name="")

    # jurasdictionary units dictionary
    ju_dict = {}

    # Spatial Join Jurisdictional Units fields
    fields = ["GUID", "LandownerDepartment", "JurisdictionalUnitName", "LegendLandownerCategory", ]

    # populate the dictionary
    with arcpy.da.SearchCursor("JU_sj", fields) as cursor:
        for row in cursor:
            guid = row[0] 
            land_dept = row[1]
            jur_name = row[2]
            legend_cat = row[3] 
            val_list = [land_dept, jur_name, legend_cat]
            ju_dict[guid] = val_list

    # populate InFORM Fuels fields
    fields = ["GUID", "Department", "Unit", "Bureau"]
    with arcpy.da.UpdateCursor(output_table, fields) as cursor:
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

#-------------------------------------------------------------------------------------------------------------------------------

def state_derivations(output_table, points, states_layer):
    
    # State Derivation
    # Spatial Join states to points
    
    arcpy.analysis.SpatialJoin(points, states_layer, "states_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;STATE_NAME \"STATE_NAME\" true true false 25 Text 0 0,First,#,States,STATE_NAME,0,25", match_option="INTERSECT", search_radius="", distance_field_name="")

    # state dictionary 
    state_dict = {}
    fields = ["GUID", "STATE_NAME"]

    # populate the dictionary
    with arcpy.da.SearchCursor("states_sj", fields) as cursor:
        for row in cursor:
            guid = row[0] 
            state = row[1]
            val_list = [state]
            state_dict[guid] = val_list

    # populate InFORM Fuels fields
    fields = ["GUID", "State"]
    with arcpy.da.UpdateCursor(output_table, fields) as cursor:
        for row in cursor:
            guid = row[0]
            if guid in state_dict:
                row[1] = state_dict[guid][0]
            
            # Update the feature with the new values
            cursor.updateRow(row)

#-------------------------------------------------------------------------------------------------------------------------------

def county_derivations(output_table, points, county_layer):
    
    # County Derivations 
    # spatial join points with county
    arcpy.analysis.SpatialJoin(points, county_layer, "county_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 255 Text 0 0,First,#,C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;CountyName \"CountyName\" true true false 255 Text 0 0,First,#,CountyName,CountyName,0,255", match_option="INTERSECT", search_radius="", distance_field_name="")
    
    # county dictionary 
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
    with arcpy.da.UpdateCursor(output_table, fields) as cursor:
        for row in cursor:
            guid = row[0]
            if guid in state_dict:
                row[1] = county_dict[guid][0]
            
            # Update the feature with the new values
            cursor.updateRow(row)


#-------------------------------------------------------------------------------------------------------------------------------

def regions_derivation(output_table, points, regions_layer):
    
    # Region Derivation - For BIA, it's BIA Region

    # Spatial Join regions to points
    arcpy.analysis.SpatialJoin(points, regions_layer, "regions_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;REGIONNAME \"REGIONNAME\" true true false 2000000000 Text 0 0,First,#,BIA_Region,REGIONNAME,0,2000000000", match_option="INTERSECT", search_radius="", distance_field_name="")

    # describe("regions_sj")

    region_dict = {}
    fields = ["GUID", "REGIONNAME"]

    # Use a search cursor to iterate through the data and populate the dictionary
    with arcpy.da.SearchCursor("regions_sj", fields) as cursor:
        for row in cursor:
            guid = row[0] 
            region = row[1]
            val_list = [region]
            region_dict[guid] = val_list

    # InFORM Fuels fields
    fields = ["GUID", "Region"]
    with arcpy.da.UpdateCursor(output_table, fields) as cursor:
        for row in cursor:
            guid = row[0]
            if guid in region_dict:
                row[1] = region_dict[guid][0]
            
            # Update the feature with the new values
            cursor.updateRow(row)


#-------------------------------------------------------------------------------------------------------------------------------

def congressional_district(output_table, points, districts_layer):

    # Congressional District Derivation
    # spatial join congressional districts to points 
    arcpy.analysis.SpatialJoin(points, districts_layer, "cd_sj", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;DISTRICTID \"District ID\" true true false 4 Text 0 0,First,#,USA_118th_Congressional_Districts,DISTRICTID,0,4;STATE_ABBR \"State Abbreviation\" true true false 2 Text 0 0,First,#,USA_118th_Congressional_Districts,STATE_ABBR,0,2", match_option="INTERSECT", search_radius="", distance_field_name="") 

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
    with arcpy.da.UpdateCursor(output_table, fields) as cursor:
        for row in cursor:
            guid = row[0]
            if guid in cd_dict:
                row[1] = cd_dict[guid][0]
            
            # Update the feature with the new values
            cursor.updateRow(row)

#-------------------------------------------------------------------------------------------------------------------------------

def vdep(output_table, points, vegDepartureLayer):


# ToDo 11/16/2023
# ToDo - have HI points seperated out. Select, export, spatial join. then use these points to populate InFORM






    arcpy.sa.ExtractValuesToPoints(points, vegDepartureLayer, "vegDp_extract", interpolate_values="NONE", add_attributes="VALUE_ONLY")

    print (f"Calculating Vegetation Departure for Landfire: {vegDepartureLayer}")
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

    with arcpy.da.UpdateCursor(output_table, fields) as cursor:
        for row in cursor:
            guid = row[0]
            if guid in vegDep_dict:
                row[1] = vegDep_dict[guid][0]
           
            # Update the feature with the new values
            
            if row[1] is not None:
                cursor.updateRow(row)

    with arcpy.da.SearchCursor(output_table, fields) as cursor:
        for row in cursor:
            print(f"veg dep {row[1]}")

#------------------------------------------------------------------------------------------------------------------------------

class Tribal_Leaders(self, tribe_full_name, bia_agency):

    # BIA / Tribal derivation class
    # Tribe Name and BIA Agency Derivation
    # describe(tribal_leaders)


    def tribal_leaders():
        self.Tribe_Full_Name = 

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

    #------------------------------------------------------------


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




class nfpors_inform_fields(self):

    def nfpors_fields():
        self.nfpors_import_fields = [
            
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

        return 