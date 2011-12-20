#!/usr/bin/env python

import calendar
import sqlite3
import sys
import time

if len(sys.argv) != 3:
	print "usage: " + sys.argv[0] + " file-name db-name"
	sys.exit(1)

f = open(sys.argv[1], 'r')
db = sqlite3.connect(sys.argv[2])
cursor = db.cursor()

while True:
	l = f.readline()
	if not l: # EOF
		break

	l = l.rstrip()
	if not l: # Empty line.
		continue;

	if l.startswith("--- Log opened"):
		# We are only interested in date here.
		s = l.split()
		timestr = s[4] + " " + s[5] + " " + s[7]
		opened_at = calendar.timegm(time.strptime(timestr, "%b %d %Y"))
		prev_msgtime = 0
		# The 'opened_at' variable contains log open time, in seconds since epoch.
		continue

	if l.startswith("--- Day changed"):
		s = l.split()
		timestr = s[4] + " " + s[5] + " " + s[6]
		opened_at = calendar.timegm(time.strptime(timestr, "%b %d %Y"))
		prev_msgtime = 0
		continue

	if l.startswith("--- Log closed"):
		del opened_at
		continue

	timestr = l.split()[0]
	t = time.strptime(timestr, "%H:%M")

	msgtime = opened_at + (t.tm_hour * 60 + t.tm_min) * 60 
	if msgtime < prev_msgtime:
		print "ERROR: time going backwards; previously ", time.strftime("%Y-%m-%d %H:%M", time.gmtime(prev_msgtime)), ", now ", time.strftime("%Y-%m-%d %H:%M", time.gmtime(msgtime))
		print "at line '", l, "'"
		exit(10)
	if msgtime == prev_msgtime:
		msgseq += 1
	else:
		msgseq = 0
	prev_msgtime = msgtime
	msgstr = " ".join(l.split()[1:])
	#print time.strftime("%Y-%m-%d %H:%M", time.gmtime(msgtime)), msgseq, msgstr
	cursor.execute('insert into irclogs values (?, ?, ?)', [time.strftime("%Y-%m-%d %H:%M", time.gmtime(msgtime)), msgseq, utf(msgstr)]);

db.commit()

