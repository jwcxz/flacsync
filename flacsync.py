#!/usr/bin/env python2

# flacsync
# Goes through a library of music and converts FLACs to MP3s, preserving as
# much tag information as it can.

# Uses a memory mapping to remember which things were already sync'd.


import argparse, hashlib, pickle, subprocess, sys, os, time, threading
from mutagen import mp3 as mutmp3
from mutagen import id3 as mutid3

CFGBASE = os.path.expanduser("~/.flacsync");
DBFILE = os.path.join(CFGBASE, "db");
CFGFILE = os.path.join(CFGBASE, "config");
LOGFILE = os.path.join(CFGBASE, "log");

FILETBL = "flacs";
NUMWORKERS = 8;
MUSICDIR = os.path.expanduser("~/media/music");
MP3DIR = "_mp3";

tagtable = { 'title': '--tt',
             'artist': '--ta',
             'album': '--tl',
             'year': '--ty',
             'date': '--ty',
             'comment': '--tc',
             'tracknumber': '--tn',
             'genre': '--tg' }


class SyncWorker(threading.Thread):
    dbg = "";

    def __init__(self, db, workers, workers_lk, flac, force):
        threading.Thread.__init__(self);

        self.db = db;
        self.flac = flac
        self.workers = workers;
        self.workers_lk = workers_lk;
        self.force = force;

        self.qresult = None;

    def run(self):
        try:
            flac_esc = self.flac.replace('"', '\\"').replace('$', '\\$').replace('`', '\\`');
            
            self.dbg = "-> %s: %s\n" %(self.name, self.flac);
            
            # check hash
            x = os.popen("md5sum \"%s\"" %(flac_esc));
            digest = x.read().split(' ')[0];
            x.close();

            if not self.force:
                if flac_esc in self.db:
                    dbdgst = self.db[flac_esc];
                else:
                    dbdgst = None

                if dbdgst != None and digest == dbdgst:
                    # don't need to sync this flac
                    self.dbg += "    already done\n"
                    print self.dbg;
                    self.stop();
                    return;
            
            # we need to sync this one
            # perform conversion
            self.dbg += "    converting...\n";
            r = self.transcode(flac_esc);

            # update hash or insert new row if the transcode was successful
            if r == None:
                self.db[flac_esc] = digest
                self.dbg += "    done!\n"

            print self.dbg;
            self.stop();
            return;
        except Exception as e:
            print "## %s (%s): Exception!" %(self.name, self.flac);
            f = open(LOGFILE, 'a');
            f.write("ERR: Exception! %s (%s)\n" %(self.name, self.flac));
            f.close();
            
            self.stop();
            return;

    def stop(self):
        self.workers_lk.acquire();
        self.workers.remove(self);
        self.workers_lk.release();

    def readquery(self, query):
        self.qresult = None;
        self.db.rdq.append( (self, query) );
        while self.qresult == None:
            time.sleep(0);

        return self.qresult;

    def writequery(self, query):
        self.db.wrq.append( (self, query) );

    def transcode(self, flac_esc):
        # convert flac to mp3
        _f = os.path.basename(self.flac);
        _b = os.path.dirname(self.flac);
        _d = os.path.join(_b, MP3DIR);

        if not os.path.exists(_d):
            os.mkdir(_d);
        elif not os.path.isdir(_d):
            os.remove(_d);
            os.mkdir(_d);

        mp3 = os.path.join(_d, _f[:-4]+'mp3');
        mp3_esc = mp3.replace('"', '\\"').replace('$', '\\$').replace('`', '\\`');

        if os.path.exists(mp3):
            os.remove(mp3);

        # extract available tags
        tags = {};
        x = os.popen("metaflac --export-tags-to=- \"%s\"" %(flac_esc));
        r = x.read();
        x.close();
        for l in r.split('\n'):
            if l != '':
                _ = l.split('=', 1);
                if len(_) == 2:
                    tags[_[0].lower()] = _[1];

        # convert tags to MP3speak
        tagopts = "";
        for tag in tags.keys():
            if tag in tagtable.keys():
                if tag == 'tracknumber':
                    tagopts += "%s %s " %(tagtable[tag], tags[tag]);
                else:
                    tagopts += "%s \"%s\" " %(tagtable[tag], tags[tag].replace('"', '\\"').replace('$', '\\$').replace('`', '\\`'));

        # transcode
        flac_log = os.path.join(CFGBASE, self.name+"_flac.log");
        lame_log = os.path.join(CFGBASE, self.name+"_lame.log");
        
        flac_cmd = "flac -s -cd \"%s\" 2>\"%s\"" \
                    %(flac_esc, flac_log);

        lame_cmd = "lame --silent -V 0 --add-id3v2 %s - \"%s\" 2>\"%s\"" \
                    %(tagopts, mp3_esc, lame_log);

        x = os.popen("%s | %s" %(flac_cmd, lame_cmd));
        ret = x.close();
        
        if ret:
            self.dbg += "    ## ERROR: command returned %d\n" %(ret);
            f = open(LOGFILE, 'a');
            flaclog = open(flac_log, 'r');
            lamelog = open(lame_log, 'r');
            f.write("ERR: %s returned %d\n" %(self.flac, ret));
            f.write("     flac: %s" %(flaclog.read()));
            f.write("     lame: %s" %(lamelog.read()));
            f.close();
            flaclog.close();
            lamelog.close();
            
            os.remove(flac_log);
            os.remove(lame_log);

            return ret;
        
        # otherwise search for pictures in the directory and apply them
        files = os.listdir(_b);
        files.reverse();    # probably will get better results this way
        m = mutmp3.MP3(mp3);
        tag = None;
        for fl in files:
            fll = fl.lower();
            if fll[-4:] in ['.jpg', '.png', 'jpeg']:
                if 'small' in fll:
                    continue;
                # found an image; now try to match it for a image type
                if 'front' in fll or 'cover' in fll:
                    tag = (3, 'Front cover');
                elif 'back' in fll or 'rear' in fll:
                    tag = (4, 'Back cover');
                elif 'folder' in fll:
                    tag = (5, 'Leaflet');
                elif 'cd' in fll:
                    tag = (6, 'Media');
                else:
                    tag = (0, 'Other');
                    
                if fll[-4:] in ['.jpg', 'jpeg']:
                    imtype = 'image/jpg';
                else:
                    imtype = 'image/png';
                    
                self.dbg += "    -> found image: %s (%d, %s)\n" %(fl, tag[0], tag[1]);
                imgf = open(os.path.join(_b, fl), 'rb')
                img = imgf.read();
                imgf.close();
                
                m.tags.add(mutid3.APIC(3, imtype, tag[0], tag[1], img));
                
        if tag != None:
            m.save();

        # remove temporary logs and finish
        os.remove(flac_log);
        os.remove(lame_log);

        return None;


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Converts FLACs to MP3s, preserving as much tag information as possible",
            prog="flacsync");

    parser.add_argument('-n', '--num-workers', action='store',
            dest='numworkers', default=NUMWORKERS, type=int, help='number of simultaneously working threads');
    parser.add_argument('-d', '--directory', action='store',
            dest='musicdir', default=MUSICDIR, help='directory to operate on');
    parser.add_argument('-m', '--mp3', action='store',
            dest='mp3dir', default=MP3DIR, help='directory to store MP3s in');
    parser.add_argument('-f', '--force', action='store_true',
            dest='force', default=False, help='convert all discovered tracks');
    
    
    args = parser.parse_args();
    MUSICDIR = os.path.expanduser(args.musicdir);
    NUMWORKERS = args.numworkers;
    MP3DIR = args.mp3dir;
    FORCE = args.force;
    
    # make sure path exists
    if not os.path.exists(MUSICDIR) or not os.path.isdir(MUSICDIR):
        print "## ERROR: music directory %s not found" %(MUSICDIR);
        sys.exit(1);
    
    if not os.path.exists(CFGBASE):
        os.mkdir(CFGBASE);
    elif not os.path.isdir(CFGBASE):
        os.remove(CFGBASE);
        os.mkdir(CFGBASE);
        
    # clear log file
    f = open(LOGFILE, 'a');
    f.write("---------------------------------------\n");
    f.close();

    # walk through the music directory looking for folders with FLACs
    print "-> searching for flacs...",
    _ = os.popen("find \"%s\" -iname \"*.flac\" -type f -print0" %(MUSICDIR));
    filelist = _.read();
    _.close();
    filelist = filelist.split('\0');
    print "done"

    print "-> sorting queue by mtime...",
    filequeue = []
    for track in filelist:
        if track != "": filequeue.append( (track, os.stat(track).st_mtime) );
    filequeue.sort(key=lambda _: _[1], reverse=True);
    print "done!"

    print "-> loading hash list"
    if not os.path.exists(DBFILE):
        flachashes = {}
    else:
        fh_fl = open(DBFILE, 'r');
        flachashes = pickle.load(fh_fl);
        fh_fl.close();
    print "done"

    print "-> performing sync with %d workers on %d tracks" %(NUMWORKERS, len(filequeue)-1)
    workers = [];
    workers_lk = threading.Semaphore();
    i = 0
    while i < len(filequeue):
        if len(workers) < NUMWORKERS:
            workers_lk.acquire();
            workers.append( SyncWorker(flachashes, workers, workers_lk, filequeue[i][0], FORCE) );
            workers[-1].start();
            workers_lk.release();
            i += 1
        else:
            time.sleep(0);

    while len(workers) > 0:
        time.sleep(0);


    print "-> saving hash list"
    fh_fl = open(DBFILE, 'w');
    pickle.dump(flachashes, fh_fl);
    fh_fl.close();
    print "done"


    print "## done"
