import json
import os
from stat import S_ISLNK, S_ISDIR
import urllib.request
import sys
import time
import datetime

STORE = "/datacentre/processing3/access_detector/last_logs"
EVENTS = "/datacentre/processing3/access_detector/events"
FILESETINFOURL = 'https://cedaarchiveapp.ceda.ac.uk/fileset/%s/info'
SPOTLISTURL = 'https://cedaarchiveapp.ceda.ac.uk/fileset/download_conf/'
NDAYS = 7   

class FileSetListing:

    def __init__(self, spot) -> None:
        self.spot = spot
        self.current = 0
        self.prev_fh = None
        self.next_fh = None
        self.events_fh = None
        self.filesetinfo = {}
        self.last_audit_time = 0 
        self.current_loc = None

    def store_file(self):
        return os.path.join(STORE, f"{self.spot}.txt")
    def store_links_file(self):
        return os.path.join(STORE, f"{self.spot}_links.txt")
    def store_tmp_file(self):
        return os.path.join(STORE, f"{self.spot}_____tmp.txt")
    def store_bak_file(self):
        return os.path.join(STORE, f"{self.spot}_____bak.txt")
    def event_file(self):
        date = datetime.datetime.now().strftime("%Y-%m-%d-%H")
        return os.path.join(EVENTS, f"{date}.events.txt")

    def last_done(self):
        if not os.path.exists(self.store_file()):
           return 0
        return os.path.getmtime(self.store_file())
        
    def get_filesetinfo(self):
        with urllib.request.urlopen(FILESETINFOURL % self.spot) as f:
            self.filesetinfo = json.loads(f.read())
            print(self.filesetinfo)
            self.logical_path = self.filesetinfo["logical_path"]
            if os.path.exists(self.logical_path) and os.path.islink(self.logical_path):
                self.current_loc = os.readlink(self.logical_path)
            else:
                self.current_loc = None
            if "last_audit_starttime" in self.filesetinfo: 
                self.last_audit_time = self.filesetinfo["last_audit_starttime"]
            if "last_audit_endtime" in self.filesetinfo: 
                self.last_audit_time = self.filesetinfo["last_audit_endtime"]

    def audit_running(self):
        return "last_audit_starttime" in self.filesetinfo and not "last_audit_endtime" in self.filesetinfo

    def not_there(self):
        return self.current_loc is None
      
    def __iter__(self):
        return self

    def __next__(self): 
        self.current += 1
        if self.current < self.high:
            return self.current
        raise StopIteration

    def forwardto(self, path):
        # forward to the first record of path or a record of something greater than path
        path_from_file, atime = self._readline() 
        while path > path_from_file:
            # print(f" - Skipping path = {path}   path_from_file = {path_from_file}")
            path_from_file, atime = self._readline() 
        return path_from_file, int(atime)

    def _readline(self):
        line = self.prev_fh.readline()
        if line == '':
                return "~~~~~~~~", 0
        path_from_file, atime = line.strip().split("|")
        return path_from_file, int(atime)

    def open(self):
        self.open_prev()
        self.open_next()
        self.events_fh = open(self.event_file(), "a")
        self.links_fh = open(self.store_links_file(), "w")    

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
        self.events_fh.close()
        self.links_fh.close()
        os.rename(self.store_file(), self.store_bak_file())
        os.rename(self.store_tmp_file(), self.store_file())

    def add(self, path, atime):
        self.next_fh.write(f"{path}|{atime}\n")

    def addlink(self, path):
        target = os.readlink(path)
        self.links_fh.write(f"{path}|{target}\n")

    def make_event(self, path, atime, size):
        etime = datetime.datetime.fromtimestamp(atime).isoformat()
        path = path[2:] 
        path = os.path.join(self.filesetinfo["logical_path"], path)
        directory, name = os.path.split(path) 
        event = {"directory": directory, "name": name, "size": size, "event_time": etime, 
                 "item_type": "FILE", "event_type": "direct_access"}   
        self.events_fh.write(json.dumps(event) + "\n")
        self.events_fh.flush()

    def find_access_events(self, directory=None):
        # for top directory look upinfo and open files
        if directory is None:
            self.get_filesetinfo()
            if self.not_there(): return
            if self.audit_running(): return
            print(self.current_loc)
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
                self.addlink(path)
                continue

            # recurse dirs
            if S_ISDIR(stat.st_mode):
                self.find_access_events(path)
                continue

            # its a file from here on. The file goes on the next list.
            self.add(path, atime)

            # Not an access event if audit time after atime.  
            if atime < self.last_audit_time:
                continue

            # Move forward in the previous list to the current file or the file after where is would have been.
            prev_path, prev_atime = self.forwardto(path)

            # if the file is new and has been access at least a day after deposit then its an access event.
            new_file_accessed = prev_path != path and atime > mtime + 24*3600

            # if a privious file and has been accessed then its an access event  
            old_file_accessed = prev_path == path and atime > prev_atime

            if new_file_accessed or old_file_accessed:
                self.make_event(path, atime, stat.st_size)

        if directory == ".":
            self.close()


def get_spot_list():
    spots = []
    with urllib.request.urlopen(SPOTLISTURL) as f:
        for line in f:
            bits = line.decode().strip().split()
            if len(bits) == 2:
                spotname = bits[0] 
                spots.append(spotname)
    return spots


def main():
    spots = get_spot_list()
    for spot in get_spot_list():
        print(spot)
        fslisting = FileSetListing(spot)
        if time.time() - fslisting.last_done() > NDAYS * 24 * 3600: 
            fslisting.find_access_events()


if __name__ == "__main__":
    main()
