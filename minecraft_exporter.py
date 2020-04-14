from prometheus_client import start_http_server, REGISTRY, Metric
import time
import requests
import json
import nbt
import re
import os
from mcrcon import MCRcon
from os import listdir
from os.path import isfile, join

class MinecraftCollector(object):
    def __init__(self):
        self.statsdirectory = "/data/world/stats"
        self.playerdirectory = "/data/world/playerdata"
        self.advancementsdirectory = "/data/world/advancements"
        self.betterquesting = "/data/world/betterquesting"
        self.map = dict()
        self.questsEnabled = False
        if os.path.isdir(self.betterquesting):
            self.questsEnabled = True

    def get_players(self):
        return [f[:-5] for f in listdir(self.statsdirectory) if isfile(join(self.statsdirectory, f))]

    def uuid_to_player(self,uuid):
        uuid = uuid.replace('-','')
        if uuid in self.map:
            return self.map[uuid]
        else:
            result = requests.get('https://api.mojang.com/user/profiles/'+uuid+'/names')
            self.map[uuid] = result.json()[-1]['name']
            return(result.json()[-1]['name'])

    def get_server_stats(self):
        if not all(x in os.environ for x in ['RCON_HOST','RCON_PASSWORD']):
            return []
        dim_tps          = Metric('dim_tps','TPS of a dimension',"counter")
        dim_ticktime     = Metric('dim_ticktime',"Time a Tick took in a Dimension","counter")
        overall_tps      = Metric('overall_tps','overall TPS',"counter")
        overall_ticktime = Metric('overall_ticktime',"overall Ticktime","counter")
        player_online    = Metric('player_online',"is 1 if player is online","counter")
        entities         = Metric('entities',"type and count of active entites", "counter")
        mcr = MCRcon(os.environ['RCON_HOST'],os.environ['RCON_PASSWORD'],port=int(os.environ['RCON_PORT']))
        mcr.connect()

        # dimensions
        resp = mcr.command("forge tps")
        dimtpsregex = re.compile("Dim\s*(-*\d*)\s\((.*?)\):\sMean tick time:\s(.*?) ms\. Mean TPS: (\d*\.\d*)")
        for dimid, dimname, meanticktime, meantps in dimtpsregex.findall(resp):
            dim_tps.add_sample('dim_tps',value=meantps,labels={'dimension_id':dimid,'dimension_name':dimname})
            dim_ticktime.add_sample('dim_ticktime',value=meanticktime,labels={'dimension_id':dimid,'dimension_name':dimname})
        overallregex = re.compile("Overall\s?: Mean tick time: (.*) ms. Mean TPS: (.*)")
        overall_tps.add_sample('overall_tps',value=overallregex.findall(resp)[0][1],labels={})
        overall_ticktime.add_sample('overall_ticktime',value=overallregex.findall(resp)[0][0],labels={})

        # dynmap
        if os.environ['DYNMAP_ENABLED'] == "True":
            dynmap_tile_render_statistics   = Metric('dynmap_tile_render_statistics','Tile Render Statistics reported by Dynmap',"counter")
            dynmap_chunk_loading_statistics_count = Metric('dynmap_chunk_loading_statistics_count','Chunk Loading Statistics reported by Dynmap',"counter")
            dynmap_chunk_loading_statistics_duration = Metric('dynmap_chunk_loading_statistics_duration','Chunk Loading Statistics reported by Dynmap',"counter")

            resp = mcr.command("dynmap stats")

            dynmaptilerenderregex = re.compile("  (.*?): processed=(\d*), rendered=(\d*), updated=(\d*)")
            for dim, processed, rendered, updated in dynmaptilerenderregex.findall(resp):
                dynmap_tile_render_statistics.add_sample('dynmap_tile_render_statistics',value=processed,labels={'type':'processed','file':dim})
                dynmap_tile_render_statistics.add_sample('dynmap_tile_render_statistics',value=rendered,labels={'type':'rendered','file':dim})
                dynmap_tile_render_statistics.add_sample('dynmap_tile_render_statistics',value=updated,labels={'type':'updated','file':dim})

            dynmapchunkloadingregex = re.compile("Chunks processed: (.*?): count=(\d*), (\d*.\d*)")
            for state, count, duration_per_chunk in dynmapchunkloadingregex.findall(resp):
                dynmap_chunk_loading_statistics_count.add_sample('dynmap_chunk_loading_statistics',value=count,labels={'type': state})
                dynmap_chunk_loading_statistics_duration.add_sample('dynmap_chunk_loading_duration',value=duration_per_chunk,labels={'type': state})



        # entites
        resp = mcr.command("forge entity list")
        entityregex = re.compile("(\d+): (.*?:.*?)\s")
        for entitycount, entityname in entityregex.findall(resp):
            entities.add_sample('entities',value=entitycount,labels={'entity':entityname})

        # player
        resp = mcr.command("list")
        playerregex = re.compile("There are \d*.*20 players online: (.*)")
        if playerregex.findall(resp):
            for player in playerregex.findall(resp)[0].split(","):
                if player:
                    player_online.add_sample('player_online',value=1,labels={'player':player.lstrip()})

        return[dim_tps,dim_ticktime,overall_tps,overall_ticktime,player_online,entities,dynmap_tile_render_statistics,dynmap_chunk_loading_statistics_count,dynmap_chunk_loading_statistics_duration]

    def get_player_quests_finished(self,uuid):
        with open(self.betterquesting+"/QuestProgress.json") as json_file:
            data = json.load(json_file)
            json_file.close()
        counter = 0
        for _, value in data['questProgress:9'].items():
            for _, u in value['tasks:9']['0:10']['completeUsers:9'].items():
                if u == uuid:
                    counter +=1
        return counter

    def get_player_stats(self,uuid):
        with open(self.statsdirectory+"/"+uuid+".json") as json_file:
            data = json.load(json_file)
            json_file.close()
        nbtfile = nbt.nbt.NBTFile(self.playerdirectory+"/"+uuid+".dat",'rb')
        data["stat:XpTotal"]  = nbtfile.get("XpTotal").value
        data["stat:XpLevel"]  = nbtfile.get("XpLevel").value
        data["stat:Score"]    = nbtfile.get("Score").value
        data["stat:Health"]   = nbtfile.get("Health").value
        data["stat:foodLevel"]= nbtfile.get("foodLevel").value
        with open(self.advancementsdirectory+"/"+uuid+".json") as json_file:
            count = 0
            advancements = json.load(json_file)
            for key, value in advancements.items():
                if key in ("DataVersion"):
                  continue
                if value["done"] == True:
                    count += 1
        data["stat:advancements"] = count
        if self.questsEnabled:
            data["stat:questsFinished"] = self.get_player_quests_finished(uuid)
        return data

    def update_metrics_for_player(self,uuid):
        data = self.get_player_stats(uuid)
        name = self.uuid_to_player(uuid)
        blocks_mined        = Metric('blocks_mined','Blocks a Player mined',"counter")
        blocks_picked_up    = Metric('blocks_picked_up','Blocks a Player picked up',"counter")
        player_deaths       = Metric('player_deaths','How often a Player died',"counter")
        player_jumps        = Metric('player_jumps','How often a Player has jumped',"counter")
        cm_traveled         = Metric('cm_traveled','How many cm a Player traveled, whatever that means',"counter")
        player_xp_total     = Metric('player_xp_total',"How much total XP a player has","counter")
        player_current_level= Metric('player_current_level',"How much current XP a player has","counter")
        player_food_level   = Metric('player_food_level',"How much food the player currently has","counter")
        player_health       = Metric('player_health',"How much Health the player currently has","counter")
        player_score        = Metric('player_score',"The Score of the player","counter")
        entities_killed     = Metric('entities_killed',"Entities killed by player","counter")
        damage_taken        = Metric('damage_taken',"Damage Taken by Player","counter")
        damage_dealt        = Metric('damage_dealt',"Damage dealt by Player","counter")
        blocks_crafted      = Metric('blocks_crafted',"Items a Player crafted","counter")
        player_playtime     = Metric('player_playtime',"Time in Minutes a Player was online","counter")
        player_advancements = Metric('player_advancements', "Number of completed advances of a player","counter")
        player_slept        = Metric('player_slept',"Times a Player slept in a bed","counter")
        player_quests_finished = Metric('player_quests_finished', 'Number of quests a Player has finished', 'counter')
        player_used_crafting_table = Metric('player_used_crafting_table',"Times a Player used a Crafting Table","counter")
        categories = ["minecraft:killed_by", "minecraft:custom", "minecraft:mined", "minecraft:killed", "minecraft:picked_up", "minecraft:crafted"]
        for category in categories:
            for element in data.get("stats").get(category):
                if category == "minecraft:killed_by":
                    player_deaths.add_sample('player_deaths',value=data.get("stats").get(category).get(element),labels={'player':name,'cause':element})
                elif category == "minecraft:custom":
                    if element == "minecraft:damage_taken":
                        damage_taken.add_sample('damage_taken',value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:damage_dealt":
                        damage_dealt.add_sample('damage_dealt',value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:play_one_minute":
                        player_playtime.add_sample('player_playtime',value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:jump":
                        player_jumps.add_sample("player_jumps",value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:sleep_in_bed":
                        player_slept.add_sample('player_slept',value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:interact_with_crafting_table":
                        player_used_crafting_table.add_sample('player_used_crafting_table',value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:crouch_one_cm":
                        cm_traveled.add_sample("cm_traveled",value=data.get("stats").get(category).get(element),labels={'player':name,'method':"crouching"})
                    elif element == "minecraft:walk_one_cm":
                        cm_traveled.add_sample("cm_traveled",value=data.get("stats").get(category).get(element),labels={'player':name,'method':"walking"})
                    elif element == "minecraft:sprint_one_cm":
                        cm_traveled.add_sample("cm_traveled",value=data.get("stats").get(category).get(element),labels={'player':name,'method':"sprinting"})
                    elif element == "minecraft:walk_on_water_one_cm":
                        cm_traveled.add_sample("cm_traveled",value=data.get("stats").get(category).get(element),labels={'player':name,'method':"frost_walker"})
                    elif element == "minecraft:fall_one_cm":
                        cm_traveled.add_sample("cm_traveled",value=data.get("stats").get(category).get(element),labels={'player':name,'method':"falling"})
                    elif element == "minecraft:fly_one_cm":
                        cm_traveled.add_sample("cm_traveled",value=data.get("stats").get(category).get(element),labels={'player':name,'method':"flying"})
                elif category == "minecraft:mined":
                    blocks_mined.add_sample("blocks_mined",value=data.get("stats").get(category).get(element),labels={'player':name,'block':element})
                elif category == "minecraft:killed":
                    entities_killed.add_sample('entities_killed',value=data.get("stats").get(category).get(element),labels={'player':name,"entity":element})
                elif category == "minecraft:picked_up":
                    blocks_picked_up.add_sample("blocks_picked_up",value=data.get("stats").get(category).get(element),labels={'player':name,'block':element})
                elif category == "minecraft:crafted":
                    blocks_crafted.add_sample("blocks_crafted",value=data.get("stats").get(category).get(element),labels={'player':name,'block':element})
        player_xp_total.add_sample('player_xp_total',value=data.get("stat:XpTotal"),labels={'player':name})
        player_current_level.add_sample('player_current_level',value=data.get("stat:XpLevel"),labels={'player':name})
        player_score.add_sample('player_score',value=data.get("stat:Score"),labels={'player':name})
        player_health.add_sample('player_health',value=data.get("stat:Health"),labels={'player':name})
        player_food_level.add_sample('player_food_level',value=data.get("stat:foodLevel"),labels={'player':name})
        player_advancements.add_sample('player_advancements',value=data.get("stat:advancements"),labels={'player':name})
        if self.questsEnabled:
            player_quests_finished.add_sample('player_quests_finished',value=data.get("stat:questsFinished"),labels={'player':name})

        return [blocks_mined,blocks_picked_up,player_deaths,player_jumps,cm_traveled,player_xp_total,player_current_level,player_food_level,player_health,player_score,entities_killed,damage_taken,damage_dealt,blocks_crafted,player_playtime,player_advancements,player_slept,player_used_crafting_table,player_quests_finished]

    def collect(self):
        for player in self.get_players():
            for metric in self.update_metrics_for_player(player)+self.get_server_stats():
                yield metric


if __name__ == '__main__':
    if all(x in os.environ for x in ['RCON_HOST','RCON_PASSWORD']):
        print("RCON is enabled for "+ os.environ['RCON_HOST'])

    start_http_server(8000)
    REGISTRY.register(MinecraftCollector())
    print("Exporter started on Port 8000")
    while True:
        time.sleep(1)