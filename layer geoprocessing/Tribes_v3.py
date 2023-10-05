# -*- coding: utf-8 -*-
"""
Generated by ArcGIS ModelBuilder on : 2023-10-03 13:35:26
"""
import arcpy

def Tribes():  # Tribes

    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = False

    InFormFuelsFeatureCsvExtract_Points = "InFormFuelsFeatureCsvExtract_Points"
    TribeName_allFields = "TribeName_allFields"

    # Process: Spatial Join (Spatial Join) (analysis)
    InFormFuelsFeatu_SpatialJoin2 = "C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\BIA Crosswalk\\BIA Crosswalk.gdb\\InFormFuelsFeatu_SpatialJoin2"
    arcpy.analysis.SpatialJoin(target_features=InFormFuelsFeatureCsvExtract_Points, join_features=TribeName_allFields, out_feature_class=InFormFuelsFeatu_SpatialJoin2, join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL", field_mapping="", match_option="INTERSECT", search_radius="", distance_field_name="")

if __name__ == '__main__':
    # Global Environment settings
    with arcpy.EnvManager(scratchWorkspace=r"C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\BIA Crosswalk\BIA Crosswalk.gdb", workspace=r"C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\BIA Crosswalk\BIA Crosswalk.gdb"):
        Tribes()