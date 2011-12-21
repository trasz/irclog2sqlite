#!/usr/bin/env python

# create table irclogs (channel text, time timestamp, seq integer, line text);

import calendar
import re
import sqlite3
import sys
import time

if len(sys.argv) != 3:
	print "usage: " + sys.argv[0] + " irclog-path db-path"
	sys.exit(1)

# We cannot use codecs.open() here instead, as it seems to split lines on invalid UTF-8 sequences.
filename = sys.argv[1]
f = open(filename, 'r')
db = sqlite3.connect(sys.argv[2])
cursor = db.cursor()
prev_msgtime = 0
prev_msgchan = ""
lineno = 1

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
			opened_at = calendar.timegm(time.strptime(timestr, "%b %d %Y"))
			# The 'opened_at' variable contains log open time, in seconds since epoch.
			continue

		if l.startswith("--- Day changed"):
			s = l.split()
			timestr = s[4] + " " + s[5] + " " + s[6]
			opened_at = calendar.timegm(time.strptime(timestr, "%b %d %Y"))
			continue

		if l.startswith("--- Log closed"):
			del opened_at
			continue

		# 12:21 -!- jot [jceel@apcoh.org] has joined #gusta-muzyczne-milosnikow-bsd-i-open-source
		if re.search(r"^..... -!- .* has joined", l):
			(msgchan, dummy) = re.subn(r"^..... -!- .* has joined ", r"", l)
			msgchan = msgchan.lower()
			if prev_msgchan and msgchan != prev_msgchan:
				raise Exception("channel changed; was " + prev_msgchan + "; changed to " + msgchan)
			prev_msgchan = msgchan

		timestr = l.split()[0]
		t = time.strptime(timestr, "%H:%M")

		msgtime = opened_at + (t.tm_hour * 60 + t.tm_min) * 60 
		if msgtime < prev_msgtime:
			raise Exception("time going backwards; previously " + time.strftime("%Y-%m-%d %H:%M", time.gmtime(prev_msgtime)) + ", now " + time.strftime("%Y-%m-%d %H:%M", time.gmtime(msgtime)))
		if msgtime == prev_msgtime:
			msgseq += 1
		else:
			msgseq = 0
		prev_msgtime = msgtime
		msgstr = " ".join(l.split()[1:])
		#print time.strftime("%Y-%m-%d %H:%M", time.gmtime(msgtime)), msgseq, msgstr
		cursor.execute('insert into irclogs (channel, time, seq, line) values (?, ?, ?, ?)', [msgchan, time.strftime("%Y-%m-%d %H:%M", time.gmtime(msgtime)), msgseq, unicode(msgstr, 'utf8', errors = 'replace')]);
	except:
		print "Problematic line in " + filename + ", line " + str(lineno) + ": " + l
		raise

db.commit()

