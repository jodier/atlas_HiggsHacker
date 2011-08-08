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

import higgs_hacker_core
import higgs_hacker_conf

import sys, time, commands, threading

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
				"in_type" VARCHAR(64) NOT NULL,
				"in_ds" VARCHAR(256) NOT NULL PRIMARY KEY,
				"out1_ds" VARCHAR(256) NOT NULL,
				"out2_ds" VARCHAR(256) NOT NULL,
				"state1" VARCHAR(16) NOT NULL,
				"state2" VARCHAR(16) NOT NULL,
				"get" INT2 NOT NULL,
				"jobID1" INT NOT NULL,
				"jobID2" INT NOT NULL,
				"date" INT NOT NULL,
				"run" INT NOT NULL
			);
		''')

	#####################################################################
	# CRON 1							    #
	#####################################################################

	def cron1(self, in_type, in_pattern, out1_pattern, out2_pattern):
		print('#############################################################################')
		print('%s - %s' % (in_type, time.strftime('%a, %d %b %Y %H:%M:%S', time.localtime())))
		print('#############################################################################')

		datasets = self.dq2_ls(in_pattern)

#		if len(datasets) > 0:
#			datasets = [datasets[0]]

		for in_ds in datasets:

			if self.dq2_complete(in_ds) != False:

				values = self.execute('SELECT * FROM DataSet WHERE in_ds = "%s"' % in_ds)

				date = self.dq2_date(in_ds)
				run = self.dq2_run(in_ds)

				if run >= 178044 and run <= 186755 and len(values) == 0:

					timestamp = int(time.time())
	
					out1_ds = out1_pattern % (higgs_hacker_conf.GRID_USER, run, timestamp)
					out2_ds = out2_pattern % (higgs_hacker_conf.GRID_USER, run, timestamp)

					print('\033[32min_ds\033[0m: %s' % in_ds)
					print('\033[36mout1_ds\033[0m: %s' % out1_ds)
					print('\033[36mout2_ds\033[0m: %s' % out2_ds)

					#####################################
					# TIMESTAMP			    #
					#####################################

					timestamp = int(time.time())

					#####################################
					# PANDA 1			    #
					#####################################

					status, output = commands.getstatusoutput('./tools/uD3PD.sh %s %s %s %s' % (in_ds, out1_ds, higgs_hacker_conf.PATH_UD3PD, higgs_hacker_conf.OPTION_UD3PD))

					if status != 0:
						self.error('Could not launch \'uD3PD.sh\' !\n%d' % output)
						continue

					print(output)

					#####################################
					# PANDA 2			    #
					#####################################

					jobIDs = self.getJobIDs(timestamp)

					if len(jobIDs) > 0:
						jobID1 = int(jobIDs[-1])
						jobID2 = int(0x00000000)
						self.execute('INSERT INTO DataSet (in_type, in_ds, out1_ds, out2_ds, state1, state2, get, jobID1, jobID2, date, run) VALUES ("%s", "%s", "%s", "%s", "??????", "??????", "0", "%d", "%d", "%d", "%d");' % (in_type, in_ds, out1_ds, out2_ds, jobID1, jobID2, date, run))
					else:
						jobID1 = int(0x00000000)
						jobID2 = int(0x00000000)
						self.execute('INSERT INTO DataSet (in_type, in_ds, out1_ds, out2_ds, state1, state2, get, jobID1, jobID2, date, run) VALUES ("%s", "%s", "%s", "%s", "FAILED", "FAILED", "0", "%d", "%d", "%d", "%d");' % (in_type, in_ds, out1_ds, out2_ds, jobID1, jobID2, date, run))

						self.error('Could not start \'pathena\' !\n%s' % output)

					#####################################

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

				status, output = commands.getstatusoutput('./tools/higgs_analysis_exp.sh %s %s %s %s' % (value['out1_ds'], value['out2_ds'], higgs_hacker_conf.PATH_HIGGS_ANALYSIS, higgs_hacker_conf.OPTION_HIGGS_ANALYSIS))

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

dbHost = 'sqlite://test_exp.db'
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

	#####################################################################

	try:
		i = 0

		while True:
			if i % 40 == 0:
				higgsHacker.checkGridProxy()

			higgsHacker.cron1('egamma_4L', 'data11_7TeV.*.physics_Egamma.merge.DAOD_HSG2.f*_m*_p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.uD3PD-%d.p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.higgs-analysis-%d.p600/')
#			higgsHacker.cron1('egamma_2L', 'data11_7TeV.*.physics_Egamma.merge.DAOD_2LHSG2.f*_m*_p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.2LuD3PD-%d.p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.2Lhiggs-analysis-%d.p600/')
			higgsHacker.cron1('muon_4L', 'data11_7TeV.*.physics_Muons.merge.DAOD_HSG2.f*_m*_p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.uD3PD-%d.p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.higgs-analysis-%d.p600/')
#			higgsHacker.cron1('muon_2L', 'data11_7TeV.*.physics_Muons.merge.DAOD_2LHSG2.f*_m*_p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.2LuD3PD-%d.p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.2Lhiggs-analysis-%d.p600/')

			time.sleep(30)

			higgsHacker.cron2()

			time.sleep(30)

			higgsHacker.cron3()

			time.sleep(30)

			i += 1

	except KeyboardInterrupt:
		higgsHacker.commit()
		higgsHacker.close()

		print('Bye')

	#####################################################################

