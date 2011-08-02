#!/usr/bin/env python

#############################################################################
# Author  : Jerome ODIER
# Email   : jerome.odier@cern.ch
#
# Version : 1.0 (2011)
#
#
# This file is part of HIGGS_HACKER.
#
#############################################################################

FROM = 'jerome.odier@cern.ch'
TO = 'jerome.odier@cern.ch'		#,elodie.tiouchichine@cern.ch

#############################################################################

import os, sys, time, socket, commands, threading

#############################################################################

try:
	import smtplib

except ImportError:
	print('Warning: could not import "smtplib" !')

#############################################################################

try:
	import sqlite3

except ImportError:
	print('Warning: could not import "sqlite3" !')

#############################################################################

try:
	import MySQLdb

except ImportError:
	print('Warning: could not import "MySQLdb" !')

#############################################################################

def SQLBuildDic(array1, array2):

	if array1 is None:
		return []

	L = []
	M = []

	for N in array1:
		L.append(      N[0]     )

	for N in array2:
		M.append(dict(zip(L, N)))

	return M


#############################################################################

def multi_split(s, splitters):
	for splitter in splitters:
		s = s.replace(splitter, '|')

	return s.split('|')

#############################################################################
# TEmail								    #
#############################################################################

from email.MIMEText import MIMEText

#############################################################################

class TEmail:

	#####################################################################

	def send(self, SUBJECT, BODY):

		try:
			#####################################################

			server = smtplib.SMTP()

			server.connect('smtp.cern.ch', '587')
			server.ehlo()
			server.starttls()
			server.ehlo()

			#####################################################

			server.login('\152\157\144\151\145\162', '\130\153\63\155\147\147\62\65\66')

			#####################################################

			data = MIMEText(BODY)

			data['From'] = FROM
			data[ 'To' ] =  TO
			data['Subject'] = SUBJECT

			server.sendmail(FROM, multi_split(TO, [',', ';']), data.as_string())

			#####################################################

			server.quit()

			#####################################################

		except (socket.error, smtplib.SMTPException), errmsg:
			sys.stderr.write('%s\n' % str(errmsg))

#############################################################################
# TSQLiteAbstract							    #
#############################################################################

class TSQLiteAbstract:

	#####################################################################

	def open(self, _host, _port, _user, _pswd, _name):
		self.host = _host
		self.port = _port
		self.user = _user
		self.pswd = _pswd
		self.name = _name

		try:
			self.db = sqlite3.connect(_host)
			self.db.text_factory = str
			self.cursor = self.db.cursor()

		except sqlite3.Error, errmsg:
			raise Exception(str(errmsg))

	#####################################################################

	def execute(self, query):
		result = []

		try:
			self.cursor.execute(query)

			result = SQLBuildDic(self.cursor.description, self.cursor.fetchall())

		except sqlite3.Error, errmsg:
			sys.stderr.write('%s\n' % str(errmsg))

		return result

	#####################################################################

	def commit(self):
		self.db.commit()

	#####################################################################

	def close(self):
		self.cursor.close()
		self.db.close()

#############################################################################
# TMySQLAbstract							    #
#############################################################################

class TMySQLAbstract:

	#####################################################################

	def open(self, _host, _port, _user, _pswd, _name):
		self.host = _host
		self.port = _port
		self.user = _user
		self.pswd = _pswd
		self.name = _name

		try:
			self.db = MySQLdb.connect(host = _host, port = _port, user = _user, passwd = _pswd, db = _name)
			self.db.text_factory = str
			self.cursor = self.db.cursor()

		except MySQLdb.Error, errmsg:
			raise Exception(str(errmsg))

	#####################################################################

	def execute(self, query):
		result = []

		try:
			self.cursor.execute(query)

			result = SQLBuildDic(self.cursor.description, self.cursor.fetchall())

		except sqlite3.Error, errmsg:
			sys.stderr.write('%s\n' % str(errmsg))

		return result

	#####################################################################

	def commit(self):
		self.db.commit()

	#####################################################################

	def close(self):
		self.cursor.close()
		self.db.close()

#############################################################################
# THiggsHackerAbstract							    #
#############################################################################

from pandatools import Client
from pandatools import PsubUtils

#############################################################################

class THiggsHackerAbstract:

	#####################################################################

	def __init__(self):
		self.engine = None

		self.mailBox = TEmail()

		self.gridPass, self.gridVoms = PsubUtils.checkGridProxy('', False, False)

	#####################################################################
	# LOG								    #
	#####################################################################

	def error(self, message):
		sys.stderr.write('error: %s\n' % str(message))

		self.mailBox.send('Higgs Hacker - Error', message)

	#####################################################################

	def success(self, message):
		sys.stdout.write('success: %s\n' % str(message))

		self.mailBox.send('Higgs Hacker - Success', message)

	#####################################################################
	# SQL								    #
	#####################################################################

	def open(self, host, port, name):
		user = ''
		pswd = ''

		if host[:9] == 'sqlite://':
			self.engine = TSQLiteAbstract()
			host = host[9:]
		if host[:8] == 'mysql://':
			self.engine = TMySQLAbstract()
			host = host[8:]

		if host.find('@') != -1:
			user, host = host.split('@', 1)

			if user.find(':') != -1:
				user, pswd = user.split(':', 1)

		try:
			self.engine.open(host, port, user, pswd, name)

		except AttributeError:
			raise Exception('bad protocol')

	#####################################################################

	def execute(self, query):
		result = []

		try:
			result = self.engine.execute(query)

		except AttributeError:
			raise Exception('DB not opened')

		return result

	#####################################################################

	def commit(self):
		try:
			self.engine.commit()

		except AttributeError:
			raise Exception('DB not opened')

	#####################################################################

	def close(self):
		try:
			self.engine.close()

			self.engine = None

		except AttributeError:
			raise Exception('DB not opened')

	#####################################################################
	# DQ2								    #
	#####################################################################

	def dq2_ls(self, pattern):
		status, output = commands.getstatusoutput('dq2-ls %s' % pattern)

		if status != 0:
			print('Could not execute \'dq2-ls %s\' !' % pattern)
			sys.exit(status)

		return output.split('\n')

	#####################################################################

	def dq2_date(self, pattern):
		status, output = commands.getstatusoutput('dq2-ls -f %s' % pattern)

		if status != 0:
			print('Could not execute \'dq2-ls -f %s\' !' % pattern)
			sys.exit(status)

		try:
			date = output.split('\n')[-1].split(':', 1)[+1].strip()

			date = time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S'))

		except:
			date = time.mktime(((((((((((((time.localtime())))))))))))))

		return int(date)

	#####################################################################

	def dq2_run(self, pattern):
		L = pattern.split('.')

		if len(L) > 1:
			return int(L[1])
		else:
			return int(0x00)

	#####################################################################
	# PANDA								    #
	#####################################################################

	def getJobIDs(self, timestamp):
		date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timestamp))

		status, output = Client.getJobIDsInTimeRange(date)

		if status != 0:
			print 'Error: panda !'
			sys.exit(status)

		output.sort()

		return output

	#####################################################################

	def jobIsFinished(self, ID):
		L = Client.getPandIDsWithJobID(ID)[1]

		if L is None:
			result = False
		else:
			result = True

			for item in L:
				if L[item][0] != 'finished':
					result = False
					pass

		return result

	#####################################################################

	def jobIsFailed(self, ID):
		L = Client.getPandIDsWithJobID(ID)[1]

		if L is None:
			result = False
		else:
			result = False

			for item in L:
				if L[item][0] != 'failed':
					result = True
					pass

		return result

	#####################################################################

	def jobIsCancelled(self, ID):
		L = Client.getPandIDsWithJobID(ID)[1]

		if L is None:
			result = False
		else:
			result = False

			for item in L:
				if L[item][0] != 'cancelled':
					result = True
					pass

		return result

#############################################################################

