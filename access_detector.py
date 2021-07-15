import json
import os
from stat import S_ISLNK, S_ISDIR
import urllib.request
import sys

STORE = "/datacentre/processing3/access_detector/last_logs"
FILESETINFOURL = 'https://cedaarchiveapp.ceda.ac.uk/fileset/%s/info'


class FileSetListing:

    def __init__(self, spot) -> None:
        self.spot = spot
        self.current = 0
        self.prev_fh = None
        self.next_fh = None
        self.filesetinfo = {}
        self.last_audit_time = 0 
        self.current_loc = None
        self.open_next()
        self.open_prev()

    def store_file(self):
        return os.path.join(STORE, f"{self.spot}.txt")
    def store_tmp_file(self):
        return os.path.join(STORE, f"{self.spot}_____tmp.txt")
    def store_bak_file(self):
        return os.path.join(STORE, f"{self.spot}_____bak.txt")
        
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
            directory = self.current_loc
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
                #  A new file is an access event if the access time is 1 day post mtime.
                if atime > mtime + 24*3600:
                    print(f"{path}:    access after new file creation event")
            else:
                # If the file is on the list and in the filesystem then 
                #  if the atime is different from the previous one (allowing for audit times, backups?) then 
                #    an access event has happened and the file goes on the next list.
                if atime != prev_atime and atime > self.last_audit_time:
                     print(f"{path}:   access event")

            #  The file goes on the next list.             
            self.add(path, atime)


def find_access_events(spot):
    # get the prev list for a spot
    
    # go down the list and the filesystem together one file at a time
    fslisting = FileSetListing(spot)
    fslisting.get_filesetinfo()
    fslisting.find_access_events()
    fslisting.close()

if __name__ == "__main__":
    spot = sys.argv[1]
    find_access_events(spot)
