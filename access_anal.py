import json
import glob
import pandas as pd

eventlogs = glob.glob("/datacentre/processing3/access_detector/events/2*.events.txt")
eventlogs.sort()

cols = ["/badc/cmip5", "/badc/cmip6", "/neodc/sent", "/neodc/modis", "/badc", "/neodc", "/"]

sums = {}
mindate = "9999"
maxdate = "0000"

for el  in eventlogs:
    print(f"===== {el} =======")
    with open(el) as fh:
        for line in fh:
            msg = json.loads(line)
            date = msg["event_time"][:10]
            size = msg["size"]
            mindate = min(mindate, date)
            maxdate = max(maxdate, date)
            for col in cols:
                if msg["directory"].startswith(col): break
            if col not in sums: sums[col] = {}
            if date not in sums[col]: sums[col][date] = (0,0)
            sums[col][date] = (sums[col][date][0] + 1, sums[col][date][1] + size)
#            print(msg["directory"], col, date, size, mindate, maxdate, sums[col][date])
    print(mindate, maxdate)

dates = pd.date_range(start=mindate, end=maxdate)
size = pd.DataFrame(0, index=dates, columns=cols)
num = pd.DataFrame(0, index=dates, columns=cols)
for col in cols:
    for date in sums[col]:
        size.loc[date].at[col] = sums[col][date][1]
        num.loc[date].at[col] = sums[col][date][0]
        

print(size)
print(num)

size.to_json("sizes.json")
num.to_json("num.json")
