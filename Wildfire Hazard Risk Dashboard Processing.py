import os
import sys
import arcpy 
import datetime

# Set the workspace
arcpy.env.workspace = r"C:\Users\jame9353\Documents\ArcGIS\Projects\Wildfire_Hazard_Risk_Dashboard\Wildfire_Hazard_Risk_Dashboard.gdb"

femaFC = r'C:\Users\warmstrong\Documents\work\20230810 Fuels Treatment and FEMA Wildfire Hazard Dashboard\data.gdb\FEMA_National_Risk_Index'

# list of wildfire risk index values
values_to_select = ["Very High", "Relatively High", "Relatively Moderate", "Relatively Low", "Very Low", "No Rating", "Insufficient Data"]

for value in values_to_select:

    # sql_query = f"{arcpy.AddFieldDelimiters(femaFC, 'WFIR_RISKR')} = {value}"
    sql_query = 'WFIR_RISKR' + " = " + "'" + value + "'"
   
    arcpy.SelectLayerByAttribute_management(femaFC, "NEW_SELECTION", sql_query)
    selected_count = int(arcpy.GetCount_management(femaFC))
    print(f"{selected_count} features selected for {value}.")


# # Use a search cursor to loop through the feature class
# with arcpy.da.SearchCursor(feature_class, ["WFIR_RISKR"]) as cursor:
#     for row in cursor:
#         # Get the value from the WFIR_RISKR field
#         value = row[0]

#         # Check if the value is in the list of values to select
#         if value in values_to_select:
#             # Select the feature
#             arcpy.SelectLayerByAttribute_management(feature_class, "ADD_TO_SELECTION", f"WFIR_RISKR = '{value}'")

# # If needed, you can export the selected features to a new feature class
# # arcpy.CopyFeatures_management(feature_class, 'output_feature_class')
