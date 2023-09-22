import arcpy
import os, sys


# create dictionary of tribal leaders data from path
def get_bia_tribal_leaders_from_file(file_path):
    query = "1=1"
    out_fields = "*"
    tribal_leaders = []

    with arcpy.da.SearchCursor(file_path, out_fields, where_clause=query) as cursor:
        for row in cursor:
            attributes = dict(zip(cursor.fields, row))
            tribal_leaders.append(attributes)

    return tribal_leaders


# create dictionary of tribal land areas data from path
def get_bia_tribal_land_areas_from_file(file_path):
    query = "1=1"
    out_fields = ["*","SHAPE@"]
    tribal_land_areas = []

    with arcpy.da.SearchCursor(file_path, out_fields, where_clause=query) as cursor:
        for row in cursor:
            attributes = dict(zip(cursor.fields, row[:-1]))  # Exclude the last element, which is the geometry
            geometry = row[-1]  # Get the geometry from the last element
            tribal_land_areas.append({'attributes': attributes, 'geometry': geometry})

    return tribal_land_areas

def combine_tribes_and_tribal_leaders(results, config):
    
    # dictionary of tribal leaders data
    tribal_leaders = results[0]['value']

    # dictionary of tribal land areas data
    tribal_land_areas = results[1]['value']
    combined_tribes = []

    for tribe in tribal_land_areas:

        print (tribe)

    #     matching_tribe_leader = None
    #     for x in tribal_leaders:
    #         if x[config['tribeLeaders']['derivationCheckField']] == tribe['attributes'][config['tribe']['derivationCheckField']]:
    #             matching_tribe_leader = x
    #             break

    #     if matching_tribe_leader:
    #         unit = matching_tribe_leader[config['tribeLeaders']['derivationOutField']]
    #     else:
    #         unit = ""

    #     both = {**tribe['attributes'], 'BIA_UNIT': unit}
    #     new_object = {
    #         'geometry': tribe['geometry'],
    #         'attributes': both
    #     }
    #     combined_tribes.append(new_object)

    # return combined_tribes

# Example configuration
config = {
    "tribe": {
        "mapOutField": "*",
        "derivationOutField": "TRIBE_NAME",
        "derivationCheckField": "TRIBE_NAME",
        "wkid": 3857
    },
    "tribeLeaders": {
        "mapOutField": "*",
        "derivationOutField": "biaagency",
        "derivationCheckField": "tribefullname",
        "wkid": 3857
    },
}



TL_path = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\Tribal_Leaders'
Tribe_path = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\TribeName'

# Load tribal leadership data from the local file
tribal_leaders = get_bia_tribal_leaders_from_file(TL_path)


# for leader in tribal_leaders:
#     print(leader)
#sys.exit()


# Load tribal land areas from the local file with 'SHAPE' field
tribal_land_areas = get_bia_tribal_land_areas_from_file(Tribe_path)

# for land in tribal_land_areas:
#     print(land)
# sys.exit()



# Assuming you have a list `results` containing the results of promises, you can call the functions as follows:
result_1 = {"status": "fulfilled", "value": tribal_leaders}
result_2 = {"status": "fulfilled", "value": tribal_land_areas}

results = [result_1, result_2]

combined_tribes = combine_tribes_and_tribal_leaders(results, config)


# print(combined_tribes)


# Tribal leadership field to check agains
"tribefullname"

# Tribe NAme to check against
"NAME"