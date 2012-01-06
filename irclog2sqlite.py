#!/usr/bin/env python

import os
import re
import sqlite3
import sys
import time

def time2str(t):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t))

def log_opened(channel, opened_at):
    cursor.execute("insert into chunks (channel, opened_at, imported_from, \
		   imported_at, imported_by) values (?, ?, ?, ?, ?)",
		   [channel, time2str(opened_at), filename, imported_at, imported_by])
    return cursor.lastrowid

def log_closed(chunk_id, closed_at):
    if opened_at > closed_at:
        raise Exception("closed before open: opened at " + time2str(opened_at) +
			", closed at " + time2str(closed_at))
    # Make sure the new chunk doesn't overlap with the existing ones.
    cursor.execute("select opened_at, closed_at, imported_from, imported_at, \
		    imported_by from chunks where channel = ? and \
		    opened_at <= ? and closed_at >= ?",
		    [channel, time2str(opened_at), time2str(closed_at)])
    result = cursor.fetchone()
    if result:
        raise Exception("time range between " + time2str(opened_at) + \
			" and " + time2str(closed_at) + \
			" overlaps with log opened at " + result[0] + \
			", closed at " + result[1] + \
			", imported from " + result[2] + \
			" at " + result[3] + " by " + result[4])

    cursor.execute("update chunks set closed_at = ? where id = ?",
		    [time2str(closed_at), chunk_id])
    if cursor.rowcount != 1:
        raise Exception("weird number of rows affected: was " + \
			str(cursor.rowcount) + ", should be 1")

if len(sys.argv) != 3:
    print "usage: " + sys.argv[0] + " irclog-path db-path"
    sys.exit(1)

# We cannot use codecs.open() here instead, as it seems to split lines
# on invalid UTF-8 sequences.
filename = os.path.abspath(sys.argv[1])
f = open(filename, "r")
db = sqlite3.connect(sys.argv[2])
cursor = db.cursor()
prev_entry_time = 0
prev_channel = ""
lineno = 1
imported_by = os.getlogin() + "@" + os.uname()[1]
imported_at = time.ctime()
opened = False

try:
    cursor.execute("create table chunks (id integer primary key autoincrement, \
		    channel text not null, opened_at timestamp not null, \
		    closed_at timestamp, imported_from text not null, \
		    imported_at timestamp not null, imported_by text not null, \
		    unique (channel, opened_at, closed_at))")
    cursor.execute("create table entries (chunk_id integer, \
		    time timestamp not null, seq integer not null, \
		    line text not null, \
		    foreign key(chunk_id) references chunks(id))")
except:
    pass

try:
    while True:
        line = f.readline()
        if not line: # EOF
            break

        lineno += 1

        line = line.rstrip()
        if not line: # Empty line.
            continue

        if line.startswith("--- Log opened"):
            if opened:
                #raise Exception("log already opened at " + time2str(opened_at))
                print "missing close for a log opened at " + \
		      time2str(opened_at) + "; closing at " + \
		      time2str(prev_entry_time)
                log_closed(chunk_id, prev_entry_time)
                del chunk_id

            timestr = " ".join(line.split()[3:])
            opened_at = time.mktime(time.strptime(timestr))

            # Subsequent calculations depend on hour and minute being 0.
            s = line.split()
            timestr = s[4] + " " + s[5] + " " + s[7]
            opened_date = time.mktime(time.strptime(timestr, "%b %d %Y"))
            prev_channel = ""
            opened = True
            continue

        if line.startswith("--- Day changed"):
            if not opened:
                raise Exception("changing day with log closed")
            s = line.split()
            timestr = s[4] + " " + s[5] + " " + s[6]
            opened_date = time.mktime(time.strptime(timestr, "%b %d %Y"))
            continue

        if line.startswith("--- Log closed"):
            if not opened:
                raise Exception("closing closed log")
            timestr = " ".join(line.split()[3:])
            closed_at = time.mktime(time.strptime(timestr))
            if (closed_at < prev_entry_time):
                raise Exception("message after log closing time; closed at " +
				time2str(closed_at) + ", last message " +
				time2str(prev_entry_time))
            log_closed(chunk_id, closed_at)
            del opened_at
            del opened_date
            del closed_at
            del chunk_id
            opened = False
            continue

        if not opened:
            raise Exception("log not opened")

        if re.search(r"^..... -!- .* has joined", line):
            (channel, dummy) = re.subn(r"^..... -!- .* has joined ", r"", line)
            channel = channel.lower()
            if not prev_channel:
                chunk_id = log_opened(channel, opened_at)
                prev_channel = channel
            elif channel != prev_channel:
                raise Exception("channel changed; was " + prev_channel +
				"; changed to " + channel)

        timestr = line.split()[0]
        t = time.strptime(timestr, "%H:%M")

        entry_time = opened_date + (t.tm_hour * 60 + t.tm_min) * 60 
        if entry_time < opened_at:
            # + 60, because opened_at is at second resolution, and entry_time
	    # is rounded down to minutes.
            if entry_time + 60 >= opened_at:
                entry_time = opened_at
            else:
                raise Exception("message before log opening time; opened at " +
				time2str(opened_at) + ", now " +
				time2str(entry_time))
        if entry_time < prev_entry_time:
            raise Exception("time going backwards; previously " +
			    time2str(prev_entry_time) + ", now " +
			    time2str(entry_time))
        if entry_time == prev_entry_time:
            entry_seq += 1
        else:
            entry_seq = 0
        prev_entry_time = entry_time
        entry_line = unicode(" ".join(line.split()[1:]), "utf8",
			     errors = "replace")
        cursor.execute("insert into entries (chunk_id, time, seq, line) \
			values (?, ?, ?, ?)", [chunk_id, time2str(entry_time),
			entry_seq, entry_line])

    if opened:
        print "missing close at EOF for a log opened at " + \
	      time2str(opened_at) + "; closing at " + time2str(prev_entry_time)
        log_closed(chunk_id, prev_entry_time)

except:
    print "\nProblematic line in " + filename + ", line " + str(lineno) + \
          ":\n" + line + "\n"
    db.rollback()
    raise

db.commit()

