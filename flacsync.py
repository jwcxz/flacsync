#!/usr/bin/env python2

# flacsync
# Goes through a library of music and converts FLACs to MP3s, preserving as
# much tag information as it can.

# Keeps an sqlite database of file hashes to prevent needless resyncing


import sqlite3 as sql
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

tagtable = { 'title': '--tt',
             'artist': '--ta',
             'album': '--tl',
             'year': '--ty',
             'date': '--ty',
             'comment': '--tc',
             'tracknumber': '--tn',
             'genre': '--tg' }


class DBWorker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self);
        self.wrq = [];
        self.rdq = [];
        self.enabled = True;

    def run(self):
        self.cxn = sql.connect(DBFILE);

        # build filename and dir hash tables
        self.cxn.execute("create table if not exists %s (filename text, hash varchar(40), primary key (filename));" %(FILETBL));

        while self.enabled:
            # continually poll the queues
            if len(self.rdq) > 0:
                wrkr, query = self.rdq[0];
                self.rdq.pop(0);
                r = self.cxn.execute(query);
                _ = [];
                for row in r:
                    _.append(row);
                wrkr.qresult = _;

            if len(self.wrq) > 0:
                wrkr, query = self.wrq[0];
                self.wrq.pop(0);
                self.cxn.execute(query);
                self.cxn.commit();
                wrkr.qresult = True;
            
            time.sleep(0);

    def stop(self):
        self.enabled = False;


class SyncWorker(threading.Thread):
    dbg = "";

    def __init__(self, db, workers, workers_lk, flac):
        threading.Thread.__init__(self);

        self.db = db;
        self.flac = flac
        self.workers = workers;
        self.workers_lk = workers_lk;

        self.qresult = None;

    def run(self):
        try:
            flac_esc = self.flac.replace('"', '\\"').replace('$', '\\$').replace('`', '\\`');
            
            self.dbg = "-> %s: %s\n" %(self.name, self.flac);

            # check hash
            x = os.popen("sha1sum \"%s\"" %(flac_esc));
            digest = x.read().split(' ')[0];
            x.close();

            dbdgst = None
            r = self.readquery("select hash from %s where filename=\"%s\";" %(FILETBL, flac_esc));
            for row in r:
                dbdgst = r[0][0];

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
                if dbdgst != None:
                    self.writequery("update %s set hash=\"%s\" where filename=\"%s\";" %(FILETBL, digest, flac_esc));
                else:
                    self.writequery("insert into %s (filename, hash) values (\"%s\", \"%s\");" %(FILETBL, flac_esc, digest));

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
        _d = os.path.join(_b, ".mp3");

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

        lame_cmd = "lame --silent --cbr -b 320 --add-id3v2 %s - \"%s\" 2>\"%s\"" \
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
            epilog="Runs a raw command when no switches are given; brings up a color chooser if rgb arguments are required but not given", 
            prog="ACRIS Client");

    parser.add_argument('-n', '--num-workers', action='store',
            dest='numworkers', default=8, type=int, help='number of simultaneously working threads');
    parser.add_argument('-d', '--directory', action='store',
            dest='musicdir', default='~/media/music', help='directory to operate on');
    
    args = parser.parse_args();
    MUSICDIR = os.path.expanduser(args.musicdir);
    NUMWORKERS = args.numworkers;
    
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
    filequeue = _.read();
    _.close();
    filequeue = filequeue.split('\0');
    print "done"

    # create and start database thread
    print "-> starting database thread...",
    dbthread = DBWorker();
    dbthread.start();
    print "done"

    print "-> performing sync with %d workers on %d tracks" %(NUMWORKERS, len(filequeue)-1)
    workers = [];
    workers_lk = threading.Semaphore();
    i = 0
    # last entry in filequeue is just '', so ignore it
    while i < len(filequeue)-1:
        if len(workers) < NUMWORKERS:
            workers_lk.acquire();
            workers.append( SyncWorker(dbthread, workers, workers_lk, filequeue[i]) );
            workers[-1].start();
            workers_lk.release();
            i += 1
        else:
            time.sleep(0);

    while len(workers) > 0:
        time.sleep(0);

    print "## done"
    dbthread.stop();
