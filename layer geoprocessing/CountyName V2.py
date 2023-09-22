# -*- coding: utf-8 -*-
"""
Generated by ArcGIS ModelBuilder on : 2023-09-21 14:52:36
"""
import arcpy

def CountyName():  # CountyName

    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = False

    InFormFuelsFeatureCsvExtract = "C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract"
    InFormFuelsFeatureCsvExtract_Points = "C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points"
    County = "County"

    # Process: Spatial Join (Spatial Join) (analysis)
    InFormFuelsFeatu_SpatialJoin1 = "C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\BIA Crosswalk\\BIA Crosswalk.gdb\\InFormFuelsFeatu_SpatialJoin1"
    arcpy.analysis.SpatialJoin(target_features=InFormFuelsFeatureCsvExtract_Points, join_features=County, out_feature_class=InFormFuelsFeatu_SpatialJoin1, join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="GUID \"GUID\" true true false 8000 Text 0 0,First,#,C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points,GUID,0,8000;County_Name_sj \"County name\" true true false 50 Text 0 0,First,#,County,NAME,0,50", match_option="INTERSECT", search_radius="", distance_field_name="")

if __name__ == '__main__':
    # Global Environment settings
    with arcpy.EnvManager(scratchWorkspace=r"C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\BIA Crosswalk\BIA Crosswalk.gdb", workspace=r"C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\BIA Crosswalk\BIA Crosswalk.gdb"):
        CountyName()
