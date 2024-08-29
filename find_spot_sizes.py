import os
import json

def getsize(filename):
    count = 0
    vol = 0
    for line in open(filename):
        path, size = line.strip().split('|')
        vol += int(size)
        count += 1
    return vol, count

# for each spot check if it has no fileset links
lastlogs_dir = "/datacentre/processing3/access_detector/last_logs" 
cache = {}
n = 0

for filename in os.listdir(lastlogs_dir):
    path = os.path.join(lastlogs_dir, filename)
    if filename.endswith("_links.txt"):
        n += 1
        spot = filename.replace("_links.txt", "")
        print(f" --- {n} --- {spot}")
        for line in open(path):
            if "/datacentre/" in line and not "nla_" in line:
                print(f" ++++ {spot} has sub spots ")
                break
        else:
            print(f"{spot} has no sub spots")
            lastlog_file = os.path.join(lastlogs_dir, f"{spot}.txt")
            cache[spot] = getsize(lastlog_file) 

json.dump(open("/tmp/lostlog_sizes.json", "w"), indent=2)

# if not then links to it can use this size as a cached size
