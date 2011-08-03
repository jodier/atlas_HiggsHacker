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
				"out_ds" VARCHAR(256) NOT NULL,
				"state" VARCHAR(16) NOT NULL,
				"jobID" INT NOT NULL,
				"date" INT NOT NULL,
				"run" INT NOT NULL
			);
		''')

	#####################################################################
	# CRON 1							    #
	#####################################################################

	def cron1(self, in_pattern, out_pattern):
		print('#############################################################################')
		print(time.strftime('%a, %d %b %Y %H:%M:%S', time.localtime()))
		print('#############################################################################')

		datasets = self.dq2_ls(in_pattern)

		for in_ds in datasets:

			values = self.execute('SELECT * FROM DataSet WHERE in_ds = "%s"' % in_ds)

			if len(values) == 0:
				date = self.dq2_date(in_ds)
				run = self.dq2_run(in_ds)

				out_ds = out_pattern % (os.getlogin(), run, int(time.time()))

				print('\033[32min_ds\033[0m: %s' % in_ds)
				print('\033[36mout_ds\033[0m: %s' % out_ds)

				#############################################
				# TIMESTAMP				    #
				#############################################

				timestamp = int(time.time())

				#############################################
				# PANDA 1				    #
				#############################################

				status, output = commands.getstatusoutput('./tools/higgs_analysis_std.sh %s %s' % (in_ds, out_ds))

				if status != 0:
					self.error('Could not launch \'higgs_analysis.sh\' !\n%d' % output)
					continue

				print(output)

				#############################################
				# PANDA 2				    #
				#############################################

				jobIDs = self.getJobIDs(timestamp)

				if len(jobIDs) > 0:
					jobID = int(jobIDs[-1])
					self.execute('INSERT INTO DataSet (in_ds, out_ds, state, jobID, date, run) VALUES ("%s", "%s", "??????", "%d", "%d", "%d");' % (in_ds, out_ds, jobID, date, run))
				else:
					jobID = int(0x00000000)
					self.execute('INSERT INTO DataSet (in_ds, out_ds, state, jobID, date, run) VALUES ("%s", "%s", "FAILED", "%d", "%d", "%d");' % (in_ds, out_ds, jobID, date, run))

					self.error('Could not start \'pathena\' job !\n%s' % output)

				#############################################

		higgsHacker.commit()

	#####################################################################
	# CRON 2							    #
	#####################################################################

	def cron2(self):

		values = self.execute('SELECT * FROM DataSet WHERE state = "??????"')

		for value in values:

			jobID = value['jobID']

			if self.jobIsFinished(jobID) != False:
				#############################################
				# SQL					    #
				#############################################

				self.execute('UPDATE DataSet SET state = "SUCCESS" WHERE jobID = "%d";' % jobID)

				#############################################
				# LOG					    #
				#############################################

				self.success('New run available: %08d :-)\nJob \'%d\' is \'success\' !\nhttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%%20Odier\n' % (value['run'], jobID, jobID))

				#############################################

			else:
				if self.jobIsFailed(jobID) != False:
					self.execute('UPDATE DataSet SET state = "FAILED" WHERE jobID = "%d";' % jobID)
					self.error('New run available: %08d :-(\nJob \'%d\' is \'failed   \' !\nhttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%%20Odier\n' % (value['run'], jobID, jobID))
					continue

				if self.jobIsCancelled(jobID) != False:
					self.execute('UPDATE DataSet SET state = "FAILED" WHERE jobID = "%d";' % jobID)
					self.error('New run available: %08d :-(\nJob \'%d\' is \'cancelled\' !\nhttp://panda.cern.ch/server/pandamon/query?job=*&jobsetID=%d&user=Jerome%%20Odier\n' % (value['run'], jobID, jobID))
					continue

		higgsHacker.commit()

#############################################################################
# HIGGS HACKER								    # 
#############################################################################

dbHost = 'sqlite://test_std.db'
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

	if not os.path.isdir('root_std'):
		os.mkdir('root_std')

	if not os.path.isdir('plot_std'):
		os.mkdir('plot_std')

	#####################################################################

	try:
		while True:
			higgsHacker.cron1('data11_7TeV.*.physics_Egamma.merge.NTUP_2LHSG2.*_p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.2Lhiggs-analysis_std-%d.p600/')
#			higgsHacker.cron1('data11_7TeV.*.physics_Egamma.merge.NTUP_HSG2.*_p600/'  , 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.higgs-analysis-%d_std.p600/')
#			higgsHacker.cron1('data11_7TeV.*.physics_Muons.merge.NTUP_2LHSG2.*_p600/' , 'user.%s.data11_7TeV.%08d.physics_Muons.merge.2Lhiggs-analysis-%d_std.p600/')
#			higgsHacker.cron1('data11_7TeV.*.physics_Muons.merge.NTUP_HSG2.*_p600/'   , 'user.%s.data11_7TeV.%08d.physics_Muons.merge.higgs-analysis-%d_std.p600/')

			time.sleep(30)

			higgsHacker.cron2()

			time.sleep(30)

	except KeyboardInterrupt:
		higgsHacker.commit()
		higgsHacker.close()

		print('Bye')

	#####################################################################

