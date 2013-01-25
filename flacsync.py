#!/usr/bin/env python2

# flacsync
# Goes through a library of music and converts FLACs to MP3s, preserving as
# much tag information as it can.

# Uses a memory mapping to remember which things were already sync'd.

# Uses multiprocessing to actually perform simultaneous processing, though the
# program is probably i/o limited anyways.


import argparse, ConfigParser, hashlib, pickle, subprocess, sys, os, tempfile, time
from multiprocessing import Pool
from mutagen import flac as mut_flac
from mutagen import mp3 as mut_mp3
from mutagen import id3 as mut_id3


class Config:
    tagtable = {
            'album'         : mut_id3.TALB,
            'comment'       : mut_id3.COMM,
            'encoded-by'    : mut_id3.TENC,
            'performer'     : mut_id3.TOPE,
            'copyright'     : mut_id3.TCOP,
            'artist'        : mut_id3.TPE1,
            'license'       : mut_id3.WXXX,
            'title'         : mut_id3.TIT2,
            'genre'         : mut_id3.TCON,
            'albumartist'   : mut_id3.TPE2,
            'composer'      : mut_id3.TCOM,
            'date'          : mut_id3.TDRC,
            'tracknumber'   : mut_id3.TRCK,
            'discnumber'    : mut_id3.TPOS,
            };

    def __init__(self):
        self.fsdir = "~/.flacsync"

        self.config = os.path.expanduser(os.path.join(self.fsdir, "config"));

        self.numworkers = 8;
        self.musicdir = os.path.expanduser("~/media/music");
        self.mp3dir = "_mp3";
        self.force = False;

        self.dbdata = {};
        self.update_filevars();


    def update_filevars(self):
        self.dbfile = os.path.expanduser(os.path.join(self.fsdir, "db"));
        self.logfile = os.path.expanduser(os.path.join(self.fsdir, "log"));


    def apply_cfgfile(self, cfg):
        cfg = os.path.expanduser(cfg);

        if not os.path.exists(cfg) or os.path.isdir(cfg):
            return;

        cfgp = configparser.ConfigParser();
        cfgp.read([self.config]);

        for new in cfgp.items('flacsync'):
            key, val = new;

            if   key == "numworkers": self.numworkers = int(val);
            elif key == "musicdir":   self.musicdir = os.path.expanduser(val);
            elif key == "mp3dir":     self.mp3dir = val;
            elif key == "fsdir":      self.fsdir = os.path.expanduser(val);

            elif key == "force" and val.lower() == "true":
                self.force = True;


        self.update_filevars();


    def apply_args(self, args):
        if args.numworkers: self.numworkers = args.numworkers;
        if args.mp3dir: self.mp3dir = args.mp3dir;
        if args.force: self.force = args.force;
        if args.musicdir: self.musicdir = os.path.expanduser(args.musicdir);
        if args.fsdir: self.fsdir = os.path.expanduser(args.fsdir);

        self.update_filevars();


    def prepare_dirs(self):
        self.fsdir = os.path.expanduser(self.fsdir);

        if not os.path.exists(self.fsdir):
            os.mkdir(self.fsdir);
        elif not os.path.isdir(self.fsdir):
            os.remove(self.fsdir);
            os.mkdir(self.fsdir);

        # make sure path exists
        if not os.path.exists(self.musicdir) or not os.path.isdir(self.musicdir):
            print "## ERROR: music directory %s not found" %(self.musicdir);
            sys.exit(1);


    def load_dbdata(self):
        if not os.path.exists(self.dbfile):
            self.dbdata = {}
        else:
            fh_fl = open(self.dbfile, 'r');
            self.dbdata = pickle.load(fh_fl);
            fh_fl.close();


    def save_dbdata(self):
        fh_fl = open(cfg.dbfile, 'w');
        pickle.dump(self.dbdata, fh_fl);
        fh_fl.close();


def process_track(trackdata):
    flac, mtime, _num, _total, cfg = trackdata;
    dbg = "";

    try:
        
        dbg += "-> [%d/%d] %s\n" %(_num, _total, flac);
        
        # check hash
        x = open(flac, 'r');
        # the first 4096 bytes should be more than enough to hold important
        # header info
        xd = x.read(4096); 
        x.close();
        digest = hashlib.md5(xd).hexdigest();

        if not cfg.force:
            if flac in cfg.dbdata:
                dbdgst = cfg.dbdata[flac];
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

        xcode, xcode_dbg = transcode(flac, cfg);
        dbg += xcode_dbg;

        # update hash or insert new row if the transcode was successful
        if xcode == None:
            dbg += "    done!\n"

        print dbg;
        return (flac, digest);

    except Exception as e:
        print "## %s %r: Exception!" %(flac, e);
        f = open(cfg.logfile, 'a');
        f.write("ERR: Exception! %s %r\n" %(flac, e));
        f.close();
        return None;


def transcode(flac, cfg):
    # convert flac to mp3
    _f = os.path.basename(flac);
    _b = os.path.dirname(flac);
    _d = os.path.join(_b, cfg.mp3dir);

    dbg = "";

    if not os.path.exists(_d):
        os.mkdir(_d);
    elif not os.path.isdir(_d):
        os.remove(_d);
        os.mkdir(_d);

    mp3 = os.path.join(_d, _f[:-4]+'mp3');

    if os.path.exists(mp3):
        os.remove(mp3);


    # transcode
    flac_log = tempfile.NamedTemporaryFile(dir=cfg.fsdir);
    lame_log = tempfile.NamedTemporaryFile(dir=cfg.fsdir);

    flac_cmd = ['flac', '-s', '-c', '-d', flac];
    lame_cmd = ['lame', '--silent', '-V', '0', '-', mp3];

    flac_proc = subprocess.Popen(flac_cmd, stdout=subprocess.PIPE, stderr=flac_log);
    lame_proc = subprocess.Popen(lame_cmd, stdin=flac_proc.stdout, stderr=lame_log);
    flac_proc.stdout.close();
    lame_proc.wait();
    ret = lame_proc.returncode;

    if ret:
        dbg += "    ## ERROR: command returned %d\n" %(ret);

        flac_log.seek(0);
        lame_log.seek(0);

        f = open(cfg.logfile, 'a');
        f.write("ERR: %s returned %d\n" %(flac, ret));
        f.write("     flac: %s" %(flac_log.read()));
        f.write("     lame: %s" %(lame_log.read()));
        f.close();
        flac_log.close();
        lame_log.close();

        return (ret, dbg);
    
    flac_log.close();
    lame_log.close();


    # copy tags
    ff = mut_flac.FLAC(flac);
    mm = mut_mp3.MP3(mp3);

    if not mm.tags:
        mm.tags = mut_id3.ID3();

    for tag in cfg.tagtable.keys():
        if ff.has_key(tag):
            id3tag = cfg.tagtable[tag];

            if tag == 'comment':
                if ff.has_key('description'):
                    desc = ff['description'];
                else: 
                    desc = u'';

                mm.tags.add( id3tag(encoding=3, lang='XXX', desc=desc, text=ff['comment']) );


            elif tag == 'tracknumber':
                if ff.has_key('tracktotal'):
                    trck = u"%s/%s" % (ff[tag][0], ff['tracktotal'][0]);
                else:
                    trck = ff[tag];

                mm.tags.add( id3tag(encoding=3, text=trck) );


            elif tag == 'license':
                mm.tags.add( id3tag(encoding=0, desc=u'', text=ff[tag]) );


            else:
                mm.tags.add( id3tag(encoding=3, text=ff[tag]) );

        elif tag == 'albumartist':
            mm.tags.add( cfg.tagtable[tag](encoding=3, text=u'VA') );


    # copy photos
    files = os.listdir(_b);
    files.reverse();    # probably will get better results this way
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
            
            mm.tags.add(mut_id3.APIC(3, imtype, tag[0], tag[1], img));
            

    mm.save();
    return (None, dbg);



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Converts FLACs to MP3s, preserving as much tag information as possible",
            epilog="By default, flacsync stores state in ~/.flacsync and can read from ~/.flacsync/config",
            prog="flacsync");

    cfg = Config();

    parser.add_argument('-c', '--config', action='store',
            dest='config', default=cfg.config,
            help='configuration file');


    parser.add_argument('-n', '--num-workers', action='store',
            dest='numworkers', default=None, type=int, 
            help='number of simultaneously working threads');

    parser.add_argument('-d', '--directory', action='store',
            dest='musicdir', default=None,
            help='directory to operate on');

    parser.add_argument('-m', '--mp3', action='store',
            dest='mp3dir', default=None,
            help='directory to store MP3s in');

    parser.add_argument('-f', '--force', action='store_true',
            dest='force', default=False,
            help='convert all discovered tracks');

    parser.add_argument('-D', '--db-directory', action='store',
            dest='fsdir', default=None,
            help='database and logfile location');

    
    args = parser.parse_args();

    # set configuration based on file
    cfg.apply_cfgfile(args.config);

    # override those values with arguments
    cfg.apply_args(args);

    cfg.prepare_dirs();


    # clear log file
    f = open(cfg.logfile, 'a');
    f.write("---------------------------------------\n");
    f.close();

    # walk through the music directory looking for folders with FLACs
    print "-> searching for flacs...",
    _ = ['find', cfg.musicdir, '-iname', "*.flac", '-type', 'f', '-print0'];
    filelist = subprocess.check_output(_);
    filelist = filelist.split('\0');
    print "done"

    print "-> sorting queue by mtime...",
    filequeue = []
    _total = len(filelist) - 1; # XXX: stupid hack to remove ""'s contribution
    _num = 0;

    for track in filelist:
        _num += 1;
        if track != "":
            filequeue.append( (track, os.stat(track).st_mtime, _num, _total, cfg) );

    filequeue.sort(key=lambda _: _[1], reverse=True);
    print "done!"

    print "-> loading hash list"
    cfg.load_dbdata();
    print "done"

    print "-> performing sync with %d workers on %d tracks" %(cfg.numworkers, len(filequeue));
    pool = Pool(processes=cfg.numworkers);
    result = pool.map_async(process_track, filequeue)
    new_hashes = result.get();

    for r in new_hashes:
        if r != None:
            cfg.dbdata[r[0]] = r[1];


    print "-> saving hash list"
    cfg.save_dbdata();
    print "done"


    print "## done"
