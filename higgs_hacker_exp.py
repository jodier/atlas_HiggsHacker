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

import os, sys, time, commands, threading, higgs_hacker_core

#############################################################################
# THiggsHacker								    #
#############################################################################

class THiggsHacker(higgs_hacker_core.THiggsHackerAbstract):

	#####################################################################
	# SQL								    #
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
	# CRON 1							    #
	#####################################################################

	def cron1(self, in_pattern, out1_pattern, out2_pattern):
		print('#############################################################################')
		print(time.strftime('%a, %d %b %Y %H:%M:%S', time.localtime()))
		print('#############################################################################')

		datasets = self.dq2_ls(in_pattern)

#		if len(datasets) > 0:
#			datasets = [datasets[0]]

		for in_ds in datasets:

			values = self.execute('SELECT * FROM DataSet WHERE in_ds = "%s"' % in_ds)

			if len(values) == 0:
				date = self.dq2_date(in_ds)
				run = self.dq2_run(in_ds)

				timestamp = int(time.time())
	
				out1_ds = out1_pattern % (os.getlogin(), run, timestamp)
				out2_ds = out2_pattern % (os.getlogin(), run, timestamp)

				print('\033[32min_ds\033[0m: %s' % in_ds)
				print('\033[36mout1_ds\033[0m: %s' % out1_ds)
				print('\033[36mout2_ds\033[0m: %s' % out2_ds)

				#############################################
				# TIMESTAMP				    #
				#############################################

				timestamp = int(time.time())

				#############################################
				# PANDA 1				    #
				#############################################

				status, output = commands.getstatusoutput('./tools/uD3PD.sh %s %s' % (in_ds, out1_ds))

				if status != 0:
					self.error('Could not launch \'uD3PD.sh\' !\n%d' % output)
					continue

				print(output)

				#############################################
				# PANDA 2				    #
				#############################################

				jobIDs = self.getJobIDs(timestamp)

				if len(jobIDs) > 0:
					jobID1 = int(jobIDs[-1])
					jobID2 = int(0x00000000)
					self.execute('INSERT INTO DataSet (in_ds, out1_ds, out2_ds, state1, state2, jobID1, jobID2, date, run) VALUES ("%s", "%s", "%s", "??????", "??????", "%d", "%d", "%d", "%d");' % (in_ds, out1_ds, out2_ds, jobID1, jobID2, date, run))
				else:
					jobID1 = int(0x00000000)
					jobID2 = int(0x00000000)
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

				timestamp = int(time.time())

				#############################################
				# PANDA 1				    #
				#############################################

				status, output = commands.getstatusoutput('./tools/higgs_analysis.sh %s %s' % (value['out1_ds'], value['out2_ds']))

				if status != 0:
					self.error('Could not launch \'higgs_analysis.sh\' !\n%d' % output)
					continue

				print(output)

				#############################################
				# PANDA 2				    #
				#############################################

				jobIDs = self.getJobIDs(timestamp)

				if len(jobIDs) > 0:
					jobID2 = int(jobIDs[-1])
					self.execute('UPDATE DataSet SET state1 = "SUCCESS", state2 = "??????", jobID2 = "%d" WHERE jobID1 = "%d";' % (jobID2, jobID1))
				else:
					jobID2 = int(0x00000000)
					self.execute('UPDATE DataSet SET state1 = "SUCCESS", state2 = "FAILED", jobID2 = "%d" WHERE jobID1 = "%d";' % (jobID2, jobID1))

					self.error('Could not start \'prun\' job !\n%s' % output)

				#############################################

			else:
				if self.jobIsFailed(jobID1) != False:
					self.execute('UPDATE DataSet SET state1 = "FAILED", state2 = "FAILED" WHERE jobID1 = "%d";' % jobID1)
					self.error('New run available: %08d :-(\nJob \'%d\' is \'failed\' !\nhttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%%20Odier\n' % (value['run'], jobID1, jobID1))
					continue

				if self.jobIsCancelled(jobID1) != False:
					self.execute('UPDATE DataSet SET state1 = "CANCELLED", state2 = "FAILED" WHERE jobID1 = "%d";' % jobID1)
					self.error('New run available: %08d :-(\nJob \'%d\' is \'cancelled\' !\nhttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%%20Odier\n' % (value['run'], jobID1, jobID1))
					continue

		higgsHacker.commit()

	#####################################################################
	# CRON 3							    #
	#####################################################################

	def cron3(self):

		values = self.execute('SELECT * FROM DataSet WHERE state1 = "SUCCESS" AND state2 = "??????"')

		for value in values:

			jobID1 = value['jobID1']
			jobID2 = value['jobID2']

			if self.jobIsFinished(jobID2) != False:
				#############################################
				# SQL					    #
				#############################################

				self.execute('UPDATE DataSet SET state2 = "SUCCESS" WHERE jobID2 = "%d";' % jobID2)

				#############################################
				# LOG					    #
				#############################################

				self.success('''New run available: %08d :-)
Job \'%d\' is \'success\' !
Job \'%d\' is \'success\' !
http://panda.cern.ch:25980/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%%20Odier
http://panda.cern.ch:25980/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%%20Odier
''' % (value['run'], jobID1, jobID2, jobID1, jobID2))

				#############################################

			else:
				if self.jobIsFailed(jobID2) != False:
					self.execute('UPDATE DataSet SET state2 = "FAILED" WHERE jobID2 = "%d";' % jobID2)
					self.error('New run available: %08d :-(\nJob \'%d\' is \'failed   \' !\nhttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%%20Odier\n' % (value['run'], jobID2))
					continue

				if self.jobIsCancelled(jobID2) != False:
					self.execute('UPDATE DataSet SET state2 = "FAILED" WHERE jobID2 = "%d";' % jobID2)
					self.error('New run available: %08d :-(\nJob \'%d\' is \'cancelled\' !\nhttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%%20Odier\n' % (value['run'], jobID2))
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
#			higgsHacker.cron1('data11_7TeV.*.physics_Egamma.merge.DAOD_HSG2.*_p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.uD3PD-%d.p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.higgs-analysis-%d.p600/')
#			higgsHacker.cron1('data11_7TeV.*.physics_Muons.merge.DAOD_2LHSG2.*_p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.2LuD3PD-%d.p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.2Lhiggs-analysis-%d.p600/')
#			higgsHacker.cron1('data11_7TeV.*.physics_Muons.merge.DAOD_HSG2.*_p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.uD3PD-%d.p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.higgs-analysis-%d.p600/')

			time.sleep(30)

			higgsHacker.cron2()

			time.sleep(30)

			higgsHacker.cron3()

			time.sleep(30)

	except KeyboardInterrupt:
		higgsHacker.commit()
		higgsHacker.close()

		print('Bye')

	#####################################################################

