import os
import sys
import re
import time
import arcpy
import logging

inTable = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\data input\data.gdb\InFormFuelsFeatureCsvExtract_Points'

# Tribal Leaders
tribal_leaders = r'C:\Users\warmstrong\Documents\work\InFORM\20230912 NFPORS InFORM Crosswalk Script\spatial layers\data.gdb\Tribal_Leaders_table'


# Create a list of all of the unique tribes in the NFPORS data
# these will be any values in NFPORS "Units" that contain the word "Tribe or Tribes"
# These tribes will be used to calculate values for the 638 tribes
# The 638 tribes will have their location Unit set to the Tribal Leader BIA Agency
# The 638 tribes will have their Funding Unit set to the Tribal Leader BIA Agency
# Need to match the Tribe name from NFPORS to the Tribal Leader name








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






# Find all unique tribe Names in the NFPORS data

uniqueTribes = []
all_units = []

# using "Funding Unit" as the field to search for tribes, because it was populated with NFPORS Units
fields = ["FundingUnit"]

with arcpy.da.SearchCursor(inTable, fields) as cursor:
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

TribeLookup_list = [string.replace("Tribes", "").replace("Tribe", "").replace("&", "and") for string in uniqueTribes]
for t in TribeLookup_list:
     print (t)

for key, value in tribal_leaders_dict.items():
    for t in TribeLookup_list:
        # Remove spaces and hyphens from both the search string and the key
        t_cleaned = re.sub(r'[-\s]', '', t.rstrip())
        key_cleaned = re.sub(r'[-\s]', '', key.rstrip())
        
        t_pattern = re.compile(re.escape(t_cleaned), re.IGNORECASE)
        if t_pattern.search(key_cleaned):
            print(f"{t} is in {key}")