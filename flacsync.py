#!/usr/bin/env python2

# flacsync
# Goes through a library of music and converts FLACs to MP3s, preserving as
# much tag information as it can.

# Uses a memory mapping to remember which things were already sync'd.

# Uses multiprocessing to actually perform simultaneous processing, though the
# program is probably i/o limited anyways.


import argparse, hashlib, pickle, subprocess, sys, os, tempfile, time
from multiprocessing import Pool
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



def process_track(trackdata):
    flac = trackdata[0];
    dbg = "";

    try:
        flac_esc = flac.replace('"', '\\"').replace('$', '\\$').replace('`', '\\`');
        
        dbg += "-> %s\n" %(flac);
        
        # check hash
        x = open(flac, 'r');
        # the first 4096 bytes should be more than enough to hold important
        # header info
        xd = x.read(4096); 
        x.close();
        digest = hashlib.md5(xd).hexdigest();

        if not FORCE:
            if flac_esc in FLACHASHES:
                dbdgst = FLACHASHES[flac_esc];
            else:
                dbdgst = None

            if dbdgst != None and digest == dbdgst:
                # don't need to sync this flac
                dbg += "    already done\n";
                print dbg;
                return None;
        
        # we need to sync this one
        # perform conversion
        dbg += "    converting...\n";
        r, rdbg = transcode(flac, flac_esc);

        dbg += rdbg;

        # update hash or insert new row if the transcode was successful
        if r == None:
            dbg += "    done!\n"

        print dbg;
        return (flac_esc, digest);
    except Exception as e:
        print "## %s %r: Exception!" %(flac, e);
        f = open(LOGFILE, 'a');
        f.write("ERR: Exception! %s %r\n" %(flac, e));
        f.close();
        return None;


def transcode(flac, flac_esc):
    # convert flac to mp3
    _f = os.path.basename(flac);
    _b = os.path.dirname(flac);
    _d = os.path.join(_b, MP3DIR);

    dbg = "";

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
    flac_log = tempfile.NamedTemporaryFile(dir=CFGBASE);
    lame_log = tempfile.NamedTemporaryFile(dir=CFGBASE);
    
    flac_cmd = "flac -s -cd \"%s\" 2>\"%s\"" \
                %(flac_esc, flac_log.name);

    lame_cmd = "lame --silent -V 0 --add-id3v2 %s - \"%s\" 2>\"%s\"" \
                %(tagopts, mp3_esc, lame_log.name);

    x = os.popen("%s | %s" %(flac_cmd, lame_cmd));
    ret = x.close();
    
    if ret:
        dbg += "    ## ERROR: command returned %d\n" %(ret);
        f = open(LOGFILE, 'a');
        f.write("ERR: %s returned %d\n" %(flac, ret));
        f.write("     flac: %s" %(flac_log.read()));
        f.write("     lame: %s" %(lame_log.read()));
        f.close();
        flac_log.close();
        lame_log.close();

        return (ret, dbg);
    
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
                
            dbg += "    -> found image: %s (%d, %s)\n" %(fl, tag[0], tag[1]);
            imgf = open(os.path.join(_b, fl), 'rb')
            img = imgf.read();
            imgf.close();
            
            m.tags.add(mutid3.APIC(3, imtype, tag[0], tag[1], img));
            
    if tag != None:
        m.save();

    return (None, dbg);



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
        FLACHASHES = {}
    else:
        fh_fl = open(DBFILE, 'r');
        FLACHASHES = pickle.load(fh_fl);
        fh_fl.close();
    print "done"

    print "-> performing sync with %d workers on %d tracks" %(NUMWORKERS, len(filequeue));
    pool = Pool(processes=NUMWORKERS);
    result = pool.map_async(process_track, filequeue)
    new_hashes = result.get();

    for r in new_hashes:
        if r != None:
            FLACHASHES[r[0]] = r[1];


    print "-> saving hash list"
    fh_fl = open(DBFILE, 'w');
    pickle.dump(FLACHASHES, fh_fl);
    fh_fl.close();
    print "done"


    print "## done"
