import json
import glob
import pandas as pd
import dask.bag as db
import sys

#N=3
#N = int(sys.argv[1])
#cols = []
#cols = json.load(open("cols.json"))
global N = 2
global cols = []
    

def grouper(x):
    #cols = ("/badc/cmip5", "/badc/cmip6", "/neodc/sent", "/neodc/modis", "/badc", "/neodc", "/")
    for c in cols:
        if x["directory"].startswith(c): return c
    else:
#        print(len(cols), cols[-3:])
        newcol = x["directory"].split("/")[:N]
#        print(newcol)
        newcol = "/".join(newcol)
        cols.append(newcol)

def binop(tot, x):
    return tot + 1

def combine(x, y): 
    return x+y

def main():

    lines = db.read_text("/datacentre/processing3/access_detector/events/2021-10-0*.events.txt")
    records = lines.map(json.loads)


    result = records.foldby(grouper, binop=binop, initial=0, combine=combine, combine_initial=0)
    x = result.compute()
    x.sort(key=lambda x:x[1])
    #print(list(map(lambda a: (a[0],a[1]*1e-12), x)))
    print(x)
    savecols = x[-50:]
    savecols = list(map(lambda a: a[0], savecols))
    json.dump(savecols, open("cols.json", "w"), indent=2)

if __name__ == "__main__":

    main()
    #cols = ["/badc/cmip5", "/badc/cmip6", "/neodc/sent", "/neodc/modis", "/badc", "/neodc", "/"]
#    cmip5 = records.filter(lambda x: x["directory"].startswith("/badc/cmip5"))
#    print(cmip5.take(5))
#    freq = cmip5.map(lambda x: x["directory"]).frequencies(sort=True).take(10)




