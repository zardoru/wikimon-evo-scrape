from ..digidb import DigiDB, display_stage, group_by_stage

namelist = open("luilist", "r").read().splitlines()
db = DigiDB()

# seek them in the database
digimon_list = db.digimon_from_namelist(namelist)

# group them by stage
stages = group_by_stage(digimon_list)

# print a markdown header plus list in that group
for stage in sorted(stages.keys()):
    print(f"## Stage {display_stage[stage]}")
    for digimon in stages[stage]:
        print(f"- [{digimon.name}](https://wikimon.net{digimon.url})")