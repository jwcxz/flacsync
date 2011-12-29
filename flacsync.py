#!/usr/bin/env python2

# flacsync
# Goes through a library of music and converts FLACs to MP3s, preserving as
# much tag information as it can.

# Keeps an sqlite database of file hashes to prevent needless resyncing


import sqlite3 as sql
import hashlib, pickle, subprocess, sys, os, time, threading

CFGBASE = os.path.expanduser("~/.flacsync");
DBFILE = os.path.join(CFGBASE, "db");
CFGFILE = os.path.join(CFGBASE, "config");

FILETBL = "flacs";
NUMWORKERS = 4;
MUSICDIR = "~/mtest/";

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
        self.cxn.execute("create table if not exists "+FILETBL+" (filename text, hash varchar(40), primary key (filename));");

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
        # do shit
        self.dbg = "-> " + str(self.name) + ": " + self.flac + "\n";

        # check hash
        x = os.popen('sha1sum "'+self.flac.replace('"', '\\"')+'"');
        digest = x.read().split(' ')[0];

        dbdgst = None
        r = self.readquery('select hash from '+FILETBL+' where filename="'+self.flac.replace('"', '\\"')+'";');
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
        self.transcode();

        # update hash or insert new row
        if dbdgst != None:
            self.writequery('update '+FILETBL+' set hash="'+digest+'" where filename="'+self.flac.replace('"', '\\"')+'";');
        else:
            self.writequery('insert into '+FILETBL+' (filename, hash) values ("'+self.flac.replace('"', '\\"')+'", "'+digest+'");');

        print self.dbg;
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

    def transcode(self):
        # convert flac to mp3
        _f = os.path.basename(self.flac);
        _d = os.path.join(os.path.dirname(self.flac), ".mp3");

        if not os.path.exists(_d):
            os.mkdir(_d);
        elif not os.path.isdir(_d):
            os.remove(_d);
            os.mkdir(_d);

        mp3 = os.path.join(_d, _f[:-4]+'mp3');

        if os.path.exists(mp3):
            os.remove(mp3);

        # extract available tags
        tags = {};
        x = os.popen('metaflac --export-tags-to=- "'+self.flac.replace('"', '\\"')+'"');
        r = x.read();
        for l in r.split('\n'):
            if l != '':
                _ = l.split('=', 1);
                tags[_[0].lower()] = _[1];

        # apply tags
        self.dbg += "   " + str(tags);
        tagopts = "";
        for tag in tags.keys():
            if tag in tagtable.keys():
                if tag == 'tracknumber':
                    tagopts += tagtable[tag] + ' ' + tags[tag] + ' ';
                else:
                    tagopts += tagtable[tag] + ' "' + tags[tag] + '" ';

        # decode flac
        #pipe = subprocess.Popen('flac -cd "'+self.flac.replace('"', '\\"')+'"', shell=True, stdout=subprocess.PIPE).stdout;
        #f = open('blah', 'w');
        #f.write(pipe.read());
        #f.close();
        #print '             flac -cd "'+self.flac.replace('"', '\\"')+'"'

        # encode mp3
        #print '             lame --cbr -b 320 --add-id3v2 '+tagopts+' - "'+mp3.replace('"', '\\"')+'"'
        #pipe = subprocess.Popen('lame --cbr -b 320 --add-id3v2 '+tagopts+' - "'+mp3.replace('"', '\\"')+'"', shell=True, stdin=subprocess.PIPE).stdin;
        
        os.popen('flac -cd "'+self.flac.replace('"', '\\"')+'" | lame --cbr -b 320 --add-id3v2 '+tagopts+' - "'+mp3.replace('"', '\\"')+'"');

        return;


if __name__ == "__main__":
    # make sure path exists
    if not os.path.exists(CFGBASE):
        os.mkdir(CFGBASE);
    elif not os.path.isdir(CFGBASE):
        os.remove(CFGBASE);
        os.mkdir(CFGBASE);

    # walk through the music directory looking for folders with FLACs
    print "-> searching for flacs...",
    _ = os.popen('find '+MUSICDIR+' -iname "*.flac" -print0');
    filequeue = _.read();
    _.close();
    filequeue = filequeue.split('\0');
    print "done"

    # create and start database thread
    print "-> starting database thread...",
    dbthread = DBWorker();
    dbthread.start();
    print "done"

    print "-> performing sync"
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
