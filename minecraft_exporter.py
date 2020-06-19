from prometheus_client import start_http_server, REGISTRY, CounterMetricFamily, Metric
import time
import requests
import json
import nbt
import re
import os
import schedule
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
        self.rcon = None
        if os.path.isdir(self.betterquesting):
            self.questsEnabled = True
        schedule.every().day.at("01:00").do(self.flush_playernamecache)

    def get_players(self):
        return [f[:-5] for f in listdir(self.statsdirectory) if isfile(join(self.statsdirectory, f))]

    def flush_playernamecache(self):
        print("flushing playername cache")
        self.map = dict()
        return

    def uuid_to_player(self,uuid):
        uuid = uuid.replace('-','')
        if uuid in self.map:
            return self.map[uuid]
        else:
            result = requests.get('https://api.mojang.com/user/profiles/'+uuid+'/names')
            self.map[uuid] = result.json()[-1]['name']
            return(result.json()[-1]['name'])

    def rcon_command(self,command):
        if self.rcon == None:
            self.rcon = MCRcon(os.environ['RCON_HOST'],os.environ['RCON_PASSWORD'],port=int(os.environ['RCON_PORT']))
            self.rcon.connect()
        try:
            response = self.rcon.command(command)
        except BrokenPipeError:
            print("Lost RCON Connection, trying to reconnect")
            self.rcon.connect()
            response = self.rcon.command(command)

        return response

    def get_server_stats(self):
        metrics = []
        if not all(x in os.environ for x in ['RCON_HOST','RCON_PASSWORD']):
            return []
        dim_tps          = CounterMetricFamily('dim_tps','TPS of a dimension')
        dim_ticktime     = CounterMetricFamily('dim_ticktime',"Time a Tick took in a Dimension")
        overall_tps      = CounterMetricFamily('overall_tps','overall TPS')
        overall_ticktime = CounterMetricFamily('overall_ticktime',"overall Ticktime")
        player_online    = CounterMetricFamily('player_online',"is 1 if player is online")
        entities         = CounterMetricFamily('entities',"type and count of active entites")

        metrics.extend([dim_tps,dim_ticktime,overall_tps,overall_ticktime,player_online,entities])


        if 'FORGE_SERVER' in os.environ and os.environ['FORGE_SERVER'] == "True":
            # dimensions
            resp = self.rcon_command("forge tps")
            dimtpsregex = re.compile("Dim\s*(-*\d*)\s\((.*?)\)\s:\sMean tick time:\s(.*?) ms\. Mean TPS: (\d*\.\d*)")
            for dimid, dimname, meanticktime, meantps in dimtpsregex.findall(resp):
                dim_tps.add_metric(value=meantps,labels={'dimension_id':dimid,'dimension_name':dimname})
                dim_ticktime.add_metric(value=meanticktime,labels={'dimension_id':dimid,'dimension_name':dimname})
            overallregex = re.compile("Overall\s?: Mean tick time: (.*) ms. Mean TPS: (.*)")
            overall_tps.add_metric(value=overallregex.findall(resp)[0][1],labels={})
            overall_ticktime.add_metric(value=overallregex.findall(resp)[0][0],labels={})

            # entites
            resp = self.rcon_command("forge entity list")
            entityregex = re.compile("(\d+): (.*?:.*?)\s")
            for entitycount, entityname in entityregex.findall(resp):
                entities.add_metric(value=entitycount,labels={'entity':entityname})

        # dynmap
        if 'DYNMAP_ENABLED' in os.environ and os.environ['DYNMAP_ENABLED'] == "True":
            dynmap_tile_render_statistics   = Metric('dynmap_tile_render_statistics','Tile Render Statistics reported by Dynmap',"counter")
            dynmap_chunk_loading_statistics_count = Metric('dynmap_chunk_loading_statistics_count','Chunk Loading Statistics reported by Dynmap',"counter")
            dynmap_chunk_loading_statistics_duration = Metric('dynmap_chunk_loading_statistics_duration','Chunk Loading Statistics reported by Dynmap',"counter")
            metrics.extend([dynmap_tile_render_statistics,dynmap_chunk_loading_statistics_count,dynmap_chunk_loading_statistics_duration])

            resp = self.rcon_command("dynmap stats")
            
            dynmaptilerenderregex = re.compile("  (.*?): processed=(\d*), rendered=(\d*), updated=(\d*)")
            for dim, processed, rendered, updated in dynmaptilerenderregex.findall(resp):
                dynmap_tile_render_statistics.add_sample('dynmap_tile_render_statistics',value=processed,labels={'type':'processed','file':dim})
                dynmap_tile_render_statistics.add_sample('dynmap_tile_render_statistics',value=rendered,labels={'type':'rendered','file':dim})
                dynmap_tile_render_statistics.add_sample('dynmap_tile_render_statistics',value=updated,labels={'type':'updated','file':dim})

            dynmapchunkloadingregex = re.compile("Chunks processed: (.*?): count=(\d*), (\d*.\d*)")
            for state, count, duration_per_chunk in dynmapchunkloadingregex.findall(resp):
                dynmap_chunk_loading_statistics_count.add_sample('dynmap_chunk_loading_statistics',value=count,labels={'type': state})
                dynmap_chunk_loading_statistics_duration.add_sample('dynmap_chunk_loading_duration',value=duration_per_chunk,labels={'type': state})

        # player
        resp = self.rcon_command("list")
        playerregex = re.compile("players online:(.*)")
        if playerregex.findall(resp):
            for player in playerregex.findall(resp)[0].split(","):
                if not player.isspace():
                    player_online.add_metric(value=1,labels={'player':player.lstrip()})

        return metrics

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
        blocks_mined        = CounterMetricFamily('blocks_mined','Blocks a Player mined',value=0, labels=[name])
        blocks_picked_up    = CounterMetricFamily('blocks_picked_up','Blocks a Player picked up',value=0, labels=[name])
        player_deaths       = CounterMetricFamily('player_deaths','How often a Player died',value=0, labels=[name])
        player_jumps        = CounterMetricFamily('player_jumps','How often a Player has jumped',value=0, labels=[name])
        cm_traveled         = CounterMetricFamily('cm_traveled','How many cm a Player traveled, whatever that means',value=0, labels=[name])
        player_xp_total     = CounterMetricFamily('player_xp_total',"How much total XP a player has",value=0, labels=[name])
        player_current_level= CounterMetricFamily('player_current_level',"How much current XP a player has",value=0, labels=[name])
        player_food_level   = CounterMetricFamily('player_food_level',"How much food the player currently has",value=0, labels=[name])
        player_health       = CounterMetricFamily('player_health',"How much Health the player currently has",value=0, labels=[name])
        player_score        = CounterMetricFamily('player_score',"The Score of the player",value=0, labels=[name])
        entities_killed     = CounterMetricFamily('entities_killed',"Entities killed by player",value=0, labels=[name])
        damage_taken        = CounterMetricFamily('damage_taken',"Damage Taken by Player",value=0, labels=[name])
        damage_dealt        = CounterMetricFamily('damage_dealt',"Damage dealt by Player",value=0, labels=[name])
        blocks_crafted      = CounterMetricFamily('blocks_crafted',"Items a Player crafted",value=0, labels=[name])
        player_playtime     = CounterMetricFamily('player_playtime',"Time in Minutes a Player was online",value=0, labels=[name])
        player_advancements = CounterMetricFamily('player_advancements', "Number of completed advances of a player",value=0, labels=[name])
        player_slept        = CounterMetricFamily('player_slept',"Times a Player slept in a bed",value=0, labels=[name])
        player_quests_finished = CounterMetricFamily('player_quests_finished', 'Number of quests a Player has finished', value=0, labels=[name])
        player_used_crafting_table = CounterMetricFamily('player_used_crafting_table',"Times a Player used a Crafting Table",value=0, labels=[name])
        categories = ["minecraft:killed_by", "minecraft:custom", "minecraft:mined", "minecraft:killed", "minecraft:picked_up", "minecraft:crafted"]
        print(data)
        for category in categories:
            print(category)
            print(categories)
            print(data.get("stats").get(category))
            if data.get("stats").get(category) is None:
                continue
            for element in data.get("stats").get(category):
                print("Element: " + element)
                if category == "minecraft:killed_by":
                    player_deaths.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name,'cause':element})
                elif category == "minecraft:custom":
                    if element == "minecraft:damage_taken":
                        damage_taken.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:damage_dealt":
                        damage_dealt.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:play_one_minute":
                        player_playtime.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:jump":
                        player_jumps.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:sleep_in_bed":
                        player_slept.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:interact_with_crafting_table":
                        player_used_crafting_table.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name})
                    elif element == "minecraft:crouch_one_cm":
                        cm_traveled.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name,'method':"crouching"})
                    elif element == "minecraft:walk_one_cm":
                        cm_traveled.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name,'method':"walking"})
                    elif element == "minecraft:sprint_one_cm":
                        cm_traveled.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name,'method':"sprinting"})
                    elif element == "minecraft:walk_on_water_one_cm":
                        cm_traveled.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name,'method':"frost_walker"})
                    elif element == "minecraft:fall_one_cm":
                        cm_traveled.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name,'method':"falling"})
                    elif element == "minecraft:fly_one_cm":
                        cm_traveled.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name,'method':"flying"})
                elif category == "minecraft:mined":
                    blocks_mined.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name,'block':element})
                elif category == "minecraft:killed":
                    entities_killed.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name,"entity":element})
                elif category == "minecraft:picked_up":
                    blocks_picked_up.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name,'block':element})
                elif category == "minecraft:crafted":
                    blocks_crafted.add_metric(value=data.get("stats").get(category).get(element),labels={'player':name,'block':element})
        player_xp_total.add_metric(value=data.get("stat:XpTotal"),labels={'player':name})
        player_current_level.add_metric(value=data.get("stat:XpLevel"),labels={'player':name})
        player_score.add_metric(value=data.get("stat:Score"),labels={'player':name})
        player_health.add_metric(value=data.get("stat:Health"),labels={'player':name})
        player_food_level.add_metric(value=data.get("stat:foodLevel"),labels={'player':name})
        player_advancements.add_metric(value=data.get("stat:advancements"),labels={'player':name})
        if self.questsEnabled:
            player_quests_finished.add_metric('player_quests_finished',value=data.get("stat:questsFinished"),labels={'player':name})

        return [blocks_mined,blocks_picked_up,player_deaths,player_jumps,cm_traveled,player_xp_total,player_current_level,player_food_level,player_health,player_score,entities_killed,damage_taken,damage_dealt,blocks_crafted,player_playtime,player_advancements,player_slept,player_used_crafting_table,player_quests_finished]

    def collect(self):
        for player in self.get_players():
            for metric in self.update_metrics_for_player(player):
                yield metric
        for metric in self.get_server_stats():
            yield metric


if __name__ == '__main__':
    if all(x in os.environ for x in ['RCON_HOST','RCON_PASSWORD']):
        print("RCON is enabled for "+ os.environ['RCON_HOST'])

    start_http_server(8000)
    REGISTRY.register(MinecraftCollector())
    print("Exporter started on Port 8000")
    while True:
        time.sleep(1)
        schedule.run_pending()
