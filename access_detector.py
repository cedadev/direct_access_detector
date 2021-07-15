import json
import os
from stat import S_ISLNK, S_ISDIR
import urllib.request
import sys
import time

STORE = "/datacentre/processing3/access_detector/last_logs"
FILESETINFOURL = 'https://cedaarchiveapp.ceda.ac.uk/fileset/%s/info'
SPOTLISTURL = 'https://cedaarchiveapp.ceda.ac.uk/fileset/download_conf/'

class FileSetListing:

    def __init__(self, spot) -> None:
        self.spot = spot
        self.current = 0
        self.prev_fh = None
        self.next_fh = None
        self.filesetinfo = {}
        self.last_audit_time = 0 
        self.current_loc = None

    def store_file(self):
        return os.path.join(STORE, f"{self.spot}.txt")
    def store_tmp_file(self):
        return os.path.join(STORE, f"{self.spot}_____tmp.txt")
    def store_bak_file(self):
        return os.path.join(STORE, f"{self.spot}_____bak.txt")

    def last_done(self):
        if not os.path.exists(self.store_file()):
           return 0
        return os.path.getmtime(self.store_file())
        
    def get_filesetinfo(self):
        with urllib.request.urlopen(FILESETINFOURL % self.spot) as f:
            self.filesetinfo = json.loads(f.read())
            self.current_loc = self.filesetinfo["storage_path"]
            if "last_audit_endtime" in self.filesetinfo: 
                self.last_audit_time = self.filesetinfo["last_audit_endtime"]
      
    def __iter__(self):
        return self

    def __next__(self): 
        self.current += 1
        if self.current < self.high:
            return self.current
        raise StopIteration

    def forwardto(self, path):
        # forward to the first record of path or a record of something greater than path
        path_from_file, atime = self.readline() 
        while path > path_from_file:
            # print(f" - Skipping path = {path}   path_from_file = {path_from_file}")
            path_from_file, atime = self.readline() 
        return path_from_file, int(atime)

    def readline(self):
        line = self.prev_fh.readline()
        if line == '':
                return "~~~~~~~~", 0
        path_from_file, atime = line.strip().split("|")
        return path_from_file, int(atime)

    def open(self):
        self.open_prev()
        self.open_next()

    def open_prev(self):
        if not os.path.exists(self.store_file()):
            with open(self.store_file(), "w"):
                pass    
        self.prev_fh = open(self.store_file())

    def open_next(self):
        if os.path.exists(self.store_tmp_file()):
            os.unlink(self.store_tmp_file())
        self.next_fh = open(self.store_tmp_file(), "w")    

    def close(self):
        self.prev_fh.close()
        self.next_fh.close()
        os.rename(self.store_file(), self.store_bak_file())
        os.rename(self.store_tmp_file(), self.store_file())

    def add(self, path, atime):
        self.next_fh.write(f"{path}|{atime}\n")
   
    def find_access_events(self, directory=None):
        if directory is None:
            self.get_filesetinfo() 
            os.chdir(self.current_loc)
            self.open()
            directory = '.'
        contents = os.listdir(directory)
        contents.sort()

        for name in contents:
            path = os.path.join(directory, name)
            stat = os.lstat(path)
            atime = int(stat.st_atime)
            mtime = stat.st_mtime

            # skip links
            if S_ISLNK(stat.st_mode):
                #print("LINK") 
                continue
            # recurse dirs
            if S_ISDIR(stat.st_mode):
                #print("DIR") 
                self.find_access_events(path)
                continue

            # if the file is not in the file system but is on the list then it's removed.
            #  A removed file is not an access event and is not added to the next list.
            prev_path, prev_atime = self.forwardto(path)
            # print(path, atime, prev_path, prev_atime)

            if prev_path != path:
                # if the file is in the filesystem and not the list its new. 
                #  A new file is an access event only if the access time is 1 day post mtime.
                if atime > mtime + 24*3600 and self.last_audit_time == 0:
                    event_time = atime
                    directory = ?
                    name = ?
                    item_type = "FILE"
                    size = stat.st_size
                    event_type = "direct_access"
                    print(f"{path}:    access after new file creation event")
            else:
                # If the file is on the list and in the filesystem then 
                #  if the atime is different from the previous one (allowing for audit times, backups?) then 
                #    an access event has happened and the file goes on the next list.
                if atime > prev_atime and atime > self.last_audit_time:
                     print(f"{path}:   access event")

            #  The file goes on the next list.             
            self.add(path, atime)

        if directory == ".":
            self.close()


def get_spot_list():
    spots = []
    with urllib.request.urlopen(SPOTLISTURL) as f:
        for line in f:
            bits = line.decode().strip().split()
            if len(bits) == 2: spots.append(bits[0])
    return spots


def main():
    spots = get_spot_list()
    for spot in get_spot_list():
        print(spot)
        fslisting = FileSetListing(spot)
        if time.time() - fslisting.last_done() > 20*3600: 
            fslisting.find_access_events()


if __name__ == "__main__":
    main()
