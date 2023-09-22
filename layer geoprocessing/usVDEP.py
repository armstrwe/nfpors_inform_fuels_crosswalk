# -*- coding: utf-8 -*-
"""
Generated by ArcGIS ModelBuilder on : 2023-09-22 07:20:53
"""
import arcpy

def usVDEP():  # usVDEP

    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = False

    InFormFuelsFeatureCsvExtract_Points = "C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\data input\\data.gdb\\InFormFuelsFeatureCsvExtract_Points"
    US_220VDEP = arcpy.Raster("US_220VDEP")

    # Process: Extract Values to Points (Extract Values to Points) (sa)
    Output_point_features = "C:\\Users\\warmstrong\\Documents\\work\\InFORM\\20230912 NFPORS InFORM Crosswalk Script\\BIA Crosswalk\\BIA Crosswalk.gdb\\Extract_InFormF1"
    arcpy.sa.ExtractValuesToPoints(in_point_features=InFormFuelsFeatureCsvExtract_Points, in_raster=US_220VDEP, out_point_features=Output_point_features, interpolate_values="NONE", add_attributes="VALUE_ONLY")

if __name__ == '__main__':
    # Global Environment settings
    with arcpy.EnvManager(scratchWorkspace=r"C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\BIA Crosswalk\BIA Crosswalk.gdb", workspace=r"C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\BIA Crosswalk\BIA Crosswalk.gdb"):
        usVDEP()
