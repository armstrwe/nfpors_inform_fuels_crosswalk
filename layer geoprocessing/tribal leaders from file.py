
import arcpy 
import os 


tribal_leaders = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\Tribal_Leaders'

def get_bia_tribal_leaders_from_file(file_path):
    # We don't care about geometry
    query = "1=1"
    out_fields = "*"
    
    tribal_leaders = []

    with arcpy.da.SearchCursor(file_path, out_fields, where_clause=query) as cursor:
        for row in cursor:
            tribal_leaders.append(dict(zip(cursor.fields, row)))

    # print (tribal_leaders)
    return tribal_leaders

tribal_leaders_list = get_bia_tribal_leaders_from_file(tribal_leaders)

# Loop through the list and print each dictionary as a new line
for leader in tribal_leaders_list:
    print(leader)