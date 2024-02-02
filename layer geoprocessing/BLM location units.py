import arcpy, sys

# Set up the environment
arcpy.env.overwriteOutput = True
# Set the workspace to the directory containing the InFORM Fuels Feature CSV Extract
arcpy.env.workspace = r'C:\Temp\temp.gdb'

points = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\data input\data.gdb\InFormFuelsFeatureCsvExtract_Points'

# BLM Field Unit
field_offices = r'C:\Users\warmstrong\Documents\Data\BLM\BLM_National_Administrative_Unit_B.gdb\BLM_National_Administrative_Unit_B.gdb\blm_natl_admu_field_poly_webpub_1'

# BLM Other Unit
blm_other = r'C:\Users\warmstrong\Documents\Data\BLM\BLM_National_Administrative_Unit_B.gdb\BLM_National_Administrative_Unit_B.gdb\blm_natl_admu_other_poly_webpub_1'


# Select points that intersect with field_offices
arcpy.MakeFeatureLayer_management(points, "points_lyr")
arcpy.SelectLayerByLocation_management("points_lyr", "INTERSECT", field_offices)

result = arcpy.GetCount_management("points_lyr")
count = int(result.getOutput(0))
print(count)


arcpy.analysis.SpatialJoin(points, field_offices, "field_office_units", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,Regions\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;ADMU_NAME \"Administrative Unit Name\" true true false 40 Text 0 0,First,#,Location Unit Layers\\blm_natl_admu_field_poly_webpub_1,ADMU_NAME,0,40", match_option="INTERSECT", search_radius="", distance_field_name="")
result = arcpy.GetCount_management("field_office_units")
count = int(result.getOutput(0))
print(count)

arcpy.SelectLayerByLocation_management("points_lyr", "INTERSECT", blm_other)
result = arcpy.GetCount_management("points_lyr")
other_count = int(result.getOutput(0))
print(other_count)



if other_count > 0: 


    other_dict = {}
    arcpy.analysis.SpatialJoin("points_lyr", blm_other, "other_units", join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,Regions\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;ADMU_NAME \"Administrative Unit Name\" true true false 40 Text 0 0,First,#,Location Unit Layers\\blm_natl_admu_field_poly_webpub_1,ADMU_NAME,0,40", match_option="INTERSECT", search_radius="", distance_field_name="")
    
    with arcpy.da.SearchCursor("other_units", ["GUID", "ADMU_NAME"]) as cursor:
        for row in cursor:
            other_dict[row[0]] = row[1]

    # add other units to points
    with arcpy.da.UpdateCursor("field_office_units", ["GUID", "ADMU_NAME"]) as cursor:
        for row in cursor:
            if row[0] in other_dict:
                row[1] = other_dict[row[0]]
                cursor.updateRow(row)
    
    "ADMU_NAME"


# arcpy.MakeFeatureLayer_management("field_office_units", "field_office_units_layer")

# arcpy.SelectLayerByAttribute_management("field_office_units_layer", "NEW_SELECTION", """ "ADMU_NAME" IS NULL """)

