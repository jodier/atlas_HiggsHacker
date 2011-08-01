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

try:
	import sqlite3

except ImportError:
	print('Warning: could not import "sqlite3" !')

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

	def __init__(self):
		try:
			self.server = smtplib.SMTP()

			self.server.connect('smtp.cern.ch', '587')
			self.server.ehlo()
			self.server.starttls()
			self.server.ehlo()
			self.server.login('jodier', '\130\153\63\155\147\147\62\65\66')

		except (socket.error, smtplib.SMTPException), errmsg:
			self.server = None

			sys.stderr.write('%s\n' % str(errmsg))

	#####################################################################

	def __del__(self):
		if not self.server is None:
			self.server.quit()

	#####################################################################

	def send(self, SUBJECT, BODY):

		if not self.server is None:
			data = MIMEText(BODY)

			data['From'] = FROM
			data['To'] = TO
			data['Subject'] = SUBJECT

			try:
				self.server.sendmail(FROM, multi_split(TO, [',', ';']), data.as_string())

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
# THiggsHacker								    #
#############################################################################

from pandatools import Client
from pandatools import PsubUtils

#############################################################################

class THiggsHacker:

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

	def create(self):
		self.execute('''
			CREATE TABLE "DataSet" (
				"in_ds" VARCHAR(256) NOT NULL PRIMARY KEY,
				"out1_ds" VARCHAR(256) NOT NULL,
				"out2_ds" VARCHAR(256) NOT NULL,
				"state1" VARCHAR(16) NOT NULL,
				"state2" VARCHAR(16) NOT NULL,
				"jobID1" INT NOT NULL,
				"jobID2" INT NOT NULL,
				"date" INT NOT NULL,
				"run" INT NOT NULL
			);
		''')

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
			print 'Could not execute \'dq2-ls\' !'
			sys.exit(status)

		return output.split('\n')

	#####################################################################

	def dq2_date(self, pattern):
		status, output = commands.getstatusoutput('dq2-ls -f %s' % pattern)

		if status != 0:
			print 'Error: dq2 !'
			sys.exit(status)

		try:
			date = output.split('\n')[-1].split(':', 1)[+1].strip()

			date = time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S'))

		except:
			date = time.mktime(	    time.localtime()	    )

		return int(date)

	#####################################################################

	def dq2_run(self, pattern):
		L = pattern.split('.')
		print(L)
		if len(L) > 1:
			return int(L[1])
		else:
			return int(0x00)

	#####################################################################
	# PANDA								    #
	#####################################################################

	def getJobIDs(self, timestamp):
		date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))

		status, output = Client.getJobIDsInTimeRange(date)

		if status != 0:
			print 'Error: panda !'
			sys.exit(status)

		output.sort()

		return output

	#####################################################################

	def jobIsFinished(self, ID):

		L = Client.getPandIDsWithJobID(ID)[1]

		if len(L) > 0:
			result = True

			for item in L:
				if item[0] != 'finished':
					result = False
					pass
		else:
			result = False

		return result

	#####################################################################

	def jobIsFailed(self, ID):

		L = Client.getPandIDsWithJobID(ID)[1]

		if len(L) > 0:
			result = False

			for item in L:
				if item[0] == 'failed':
					result = True
					break
		else:
			result = False

		return result

	#####################################################################

	def jobIsCancelled(self, ID):

		L = Client.getPandIDsWithJobID(ID)[1]

		if len(L) > 0:
			result = False

			for item in L:
				if item[0] == 'cancelled':
					result = True
					break
		else:
			result = False

		return result

	#####################################################################
	# CRON 1							    #
	#####################################################################

	def cron1(self, in_pattern, out1_pattern, out2_pattern):
		print('#############################################################################')
		print(time.strftime('%a, %d %b %Y %H:%M:%S', time.localtime()))
		print('#############################################################################')

		datasets = self.dq2_ls(in_pattern)

		if len(datasets) > 0:
			datasets = [datasets[0]]

		for in_ds in datasets:

			values = self.execute('SELECT * FROM DataSet WHERE in_ds = "%s"' % in_ds)

			if len(values) == 0:
				date = self.dq2_date(in_ds)
				run = self.dq2_run(in_ds)

				timestamp = time.time()
	
				out1_ds = out1_pattern % (os.getlogin(), run, timestamp)
				out2_ds = out2_pattern % (os.getlogin(), run, timestamp)

				print('\033[32min_ds\033[0m: %s' % in_ds)
				print('\033[36mout1_ds\033[0m: %s' % out1_ds)
				print('\033[36mout2_ds\033[0m: %s' % out2_ds)

				#############################################
				# TIMESTAMP				    #
				#############################################

				timestamp = time.time()

				#############################################
				# PANDA 1				    #
				#############################################

				status, output = commands.getstatusoutput('./tools/uD3PD.sh %s %s' % (in_ds, out1_ds))

				if status != 0:
					self.error('Could not launch \'uD3PD.sh\' !\n%d' % output)
					sys.exit(status)

				#############################################
				# PANDA 2				    #
				#############################################

				jobIDs = self.getJobIDs(timestamp)

				if len(jobIDs) == 1:
					jobID1 = int(jobIDs[0])
					jobID2 = int(0x0000000)
					self.execute('INSERT INTO DataSet (in_ds, out1_ds, out2_ds, state1, state2, jobID1, jobID2, date, run) VALUES ("%s", "%s", "%s", "??????", "??????", "%d", "%d", "%d", "%d");' % (in_ds, out1_ds, out2_ds, jobID1, jobID2, date, run))
				else:
					jobID1 = int(0x0000000)
					jobID2 = int(0x0000000)
					self.execute('INSERT INTO DataSet (in_ds, out1_ds, out2_ds, state1, state2, jobID1, jobID2, date, run) VALUES ("%s", "%s", "%s", "FAILED", "FAILED", "%d", "%d", "%d", "%d");' % (in_ds, out1_ds, out2_ds, jobID1, jobID2, date, run))

					self.error('Could not start \'pathena\' !\n%s' % output)

				#############################################

		higgsHacker.commit()

	#####################################################################
	# CRON 2							    #
	#####################################################################

	def cron2(self):

		values = self.execute('SELECT * FROM DataSet WHERE state1 = "??????" AND state2 = "??????"')

		for value in values:

			jobID1 = value['jobID1']

			if self.jobIsFinished(jobID1) != False:
				#############################################
				# TIMESTAMP				    #
				#############################################

				timestamp = time.time()

				#############################################
				# PANDA 1				    #
				#############################################

				status, output = commands.getstatusoutput('./tools/higgs_analysis.sh %s %s' % (value['out1_ds'], value['out2_ds']))

				if status != 0:
					self.error('Could not launch \'higgs_analysis.sh\' !\n%d' % output)
					sys.exit(status)

				#############################################
				# PANDA 2				    #
				#############################################

				jobIDs = self.getJobIDs(timestamp)

				if len(jobIDs) == 1:
					jobID2 = int(jobIDs[0])
					self.execute('UPDATE DataSet SET state1 = "SUCCESS", state2 = "??????", jobID2 = "%d" WHERE jobID1 = "%d";' % (jobID2, jobID1))
				else:
					jobID2 = int(0x0000000)
					self.execute('UPDATE DataSet SET state1 = "SUCCESS", state2 = "FAILED", jobID2 = "%d" WHERE jobID1 = "%d";' % (jobID2, jobID1))

					self.error('Could not start \'prun\' job !\n%s' % output)

				#############################################

			else:
				if self.jobIsFailed(jobID1) != False:
					self.execute('UPDATE DataSet SET state1 = "FAILED", state2 = "FAILED" WHERE jobID1 = "%d";' % jobID1)
					self.error('Job \'%d\' is \'failed\' !\nhttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%20Odier\n' % (jobID1, jobID1))
					continue

				if self.jobIsCancelled(jobID1) != False:
					self.execute('UPDATE DataSet SET state1 = "CANCELLED", state2 = "FAILED" WHERE jobID1 = "%d";' % jobID1)
					self.error('Job \'%d\' is \'cancelled\' !\nhttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%20Odier\n' % (jobID1, jobID1))
					continue

		higgsHacker.commit()

	#####################################################################
	# CRON 3							    #
	#####################################################################

	def cron3(self):

		values = self.execute('SELECT * FROM DataSet WHERE state1 = "SUCCESS" AND state2 = "??????"')

		for value in values:

			jobID2 = value['jobID2']

			if self.jobIsFinished(jobID2) == False:
				#############################################
				# SQL					    #
				#############################################

				self.execute('UPDATE DataSet SET state2 = "SUCCESS" WHERE jobID2 = "%d";' % jobID2)

				#############################################
				# LOG					    #
				#############################################

				self.success('''New run available: %08d :-)
http://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%20Odier
http://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%20Odier

''' % (value['run'], value['jobID1'], value['jobID2']))

				#############################################

			else:
				if self.jobIsFailed(jobID2) != False:
					self.execute('UPDATE DataSet SET state2 = "FAILED" WHERE jobID2 = "%d";' % jobID2)
					self.error('Job \'%d\' is \'failed\' !\nhttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%20Odier\n' % (jobID2, jobID2))
					continue

				if self.jobIsCancelled(jobID2) != False:
					self.execute('UPDATE DataSet SET state2 = "FAILED" WHERE jobID2 = "%d";' % jobID2)
					self.error('Job \'%d\' is \'cancelled\' !\nhttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%20Odier\n' % (jobID2, jobID2))
					continue

		higgsHacker.commit()

#############################################################################
# HIGGS HACKER								    # 
#############################################################################

dbHost = 'sqlite://test.db'
dbPort = 3306
dbName = 'HiggsHunter'

#############################################################################

from optparse import OptionParser

#############################################################################

if __name__ == '__main__':
	parser = OptionParser('usage: %prog [options] dbhost dbport dbname')

	parser.add_option('-a', '--authors',
			action='store_true', dest='authors', help='show the authors of this application')
	parser.add_option('-v', '--version',
			action='store_true', dest='version', help='show the version of this application')

	(options, args) = parser.parse_args()

	if options.authors:
		print 'Jerome ODIER'
		sys.exit()

	if options.version:
		print 'higgs_hacker-1.0'
		sys.exit()

	#####################################################################

	higgsHacker = THiggsHacker()

	#####################################################################

	if   len(args) == 0:
		higgsHacker.open(dbHost, dbPort, dbName)
	elif len(args) == 1:
		higgsHacker.open(args[0], dbPort, dbName)
	elif len(args) == 2:
		higgsHacker.open(args[0], int(args[1]), dbName)
	elif len(args) == 3:
		higgsHacker.open(args[0], int(args[1]), args[2])

	higgsHacker.create()

	if not os.path.isdir('root'):
		os.mkdir('root')

	if not os.path.isdir('plot'):
		os.mkdir('plot')

	#####################################################################

	try:
		while True:
			higgsHacker.cron1('data11_7TeV.*.physics_Egamma.merge.DAOD_2LHSG2.*_p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.2LuD3PD-%d.p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.2Lhiggs-analysis-%d.p600/')
			higgsHacker.cron1('data11_7TeV.*.physics_Egamma.merge.DAOD_HSG2.*_p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.uD3PD-%d.p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.higgs-analysis-%d.p600/')
#			higgsHacker.cron1('data11_7TeV.*.physics_Muons.merge.DAOD_2LHSG2.*_p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.2LuD3PD-%d.p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.2Lhiggs-analysis-%d.p600/')
#			higgsHacker.cron1('data11_7TeV.*.physics_Muons.merge.DAOD_HSG2.*_p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.uD3PD-%d.p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.higgs-analysis-%d.p600/')

#			time.sleep(600)

			higgsHacker.cron2()

#			time.sleep(600)

			higgsHacker.cron3()

			time.sleep(600)

	except KeyboardInterrupt:
		higgsHacker.commit()
		higgsHacker.close()

		print('Bye')

	#####################################################################

