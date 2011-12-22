#!/usr/bin/env python

# create table chunks (id int primary key, channel text, imported_from text, imported_at timestamp, imported_by text);
# create table entries (chunk_id int references chunks(id), time timestamp, seq integer, line text);

import calendar
import os
import re
import sqlite3
import sys
import time

def time2str(s):
	return time.strftime("%Y-%m-%d %H:%M", time.gmtime(s))

def create_chunk(ch):
	cursor.execute('insert into chunks (channel, imported_from, imported_at, imported_by) values (?, ?, ?, ?)', [ch, filename, imported_at, imported_by])
	return cursor.lastrowid

if len(sys.argv) != 3:
	print "usage: " + sys.argv[0] + " irclog-path db-path"
	sys.exit(1)

# We cannot use codecs.open() here instead, as it seems to split lines on invalid UTF-8 sequences.
filename = os.path.abspath(sys.argv[1])
f = open(filename, 'r')
db = sqlite3.connect(sys.argv[2])
cursor = db.cursor()
prev_entry_time = 0
prev_channel = ""
lineno = 1
imported_by = os.getlogin() + "@" + os.uname()[1]
imported_at = time.ctime()

try:
	cursor.execute('create table chunks (id integer primary key autoincrement, channel text not null, imported_from text not null, imported_at timestamp not null, imported_by text not null)')
	cursor.execute('create table entries (chunk_id integer, time timestamp not null, seq integer not null, line text not null, foreign key(chunk_id) references chunks(id))')
except:
	pass

while True:
	l = f.readline()
	if not l: # EOF
		break

	lineno += 1

	l = l.rstrip()
	if not l: # Empty line.
		continue;

	try:
		if l.startswith("--- Log opened"):
			# We are only interested in date here; subsequent calculations depend on hour and minute being 0.
			s = l.split()
			timestr = s[4] + " " + s[5] + " " + s[7]
			# The 'opened_at' variable contains log open time, in seconds since epoch.
			opened_at = calendar.timegm(time.strptime(timestr, "%b %d %Y"))
			continue

		if l.startswith("--- Day changed"):
			s = l.split()
			timestr = s[4] + " " + s[5] + " " + s[6]
			opened_at = calendar.timegm(time.strptime(timestr, "%b %d %Y"))
			continue

		if l.startswith("--- Log closed"):
			del opened_at
			del chunk_id
			prev_channel = ""
			continue

		if re.search(r"^..... -!- .* has joined", l):
			(channel, dummy) = re.subn(r"^..... -!- .* has joined ", r"", l)
			channel = channel.lower()
			if not prev_channel:
				chunk_id = create_chunk(channel)
			elif channel != prev_channel:
				raise Exception("channel changed; was " + prev_channel + "; changed to " + channel)
			prev_channel = channel

		timestr = l.split()[0]
		t = time.strptime(timestr, "%H:%M")

		entry_time = opened_at + (t.tm_hour * 60 + t.tm_min) * 60 
		if entry_time < prev_entry_time:
			raise Exception("time going backwards; previously " + time2str(prev_entry_time) + ", now " + time2str(entry_time))
		if entry_time == prev_entry_time:
			entry_seq += 1
		else:
			entry_seq = 0
		prev_entry_time = entry_time
		entry_line = unicode(" ".join(l.split()[1:]), 'utf8', errors = 'replace')
		cursor.execute('insert into entries (chunk_id, time, seq, line) values (?, ?, ?, ?)', [chunk_id, time2str(entry_time), entry_seq, entry_line])
	except:
		print "\nProblematic line in " + filename + ", line " + str(lineno) + ":\n" + l + "\n"
		db.rollback()
		raise

db.commit()

