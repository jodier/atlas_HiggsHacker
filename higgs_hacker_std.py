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
				"out_ds" VARCHAR(256) NOT NULL,
				"state" VARCHAR(16) NOT NULL,
				"get" INT2 NOT NULL,
				"jobID" INT NOT NULL,
				"date" INT NOT NULL,
				"run" INT NOT NULL
			);
		''')

	#####################################################################
	# CRON 1							    #
	#####################################################################

	def cron1(self, in_type, in_pattern, out_pattern):
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

				if run >= 178044 and run <= 186755 && len(values) == 0:

					out_ds = out_pattern % (higgs_hacker_conf.GRID_USER, run, int(time.time()))

					print('\033[32min_ds\033[0m: %s' % in_ds)
					print('\033[36mout_ds\033[0m: %s' % out_ds)

					#####################################
					# TIMESTAMP			    #
					#####################################

					timestamp = int(time.time())

					#####################################
					# PANDA 1			    #
					#####################################

					status, output = commands.getstatusoutput('./tools/higgs_analysis_std.sh %s %s %s %s' % (in_ds, out_ds, higgs_hacker_conf.PATH_HIGGS_ANALYSIS, higgs_hacker_conf.OPTION_HIGGS_ANALYSIS))

					if status != 0:
						self.error('Could not launch \'higgs_analysis.sh\' !\n%d' % output)
						continue

					print(output)

					#####################################
					# PANDA 2			    #
					#####################################

					jobIDs = self.getJobIDs(timestamp)

					if len(jobIDs) > 0:
						jobID = int(jobIDs[-1])
						self.execute('INSERT INTO DataSet (in_type, in_ds, out_ds, state, get, jobID, date, run) VALUES ("%s", "%s", "%s", "??????", "0", "%d", "%d", "%d");' % (in_type, in_ds, out_ds, jobID, date, run))
					else:
						jobID = int(0x00000000)
						self.execute('INSERT INTO DataSet (in_type, in_ds, out_ds, state, get, jobID, date, run) VALUES ("%s", "%s", "%s", "FAILED", "0", "%d", "%d", "%d");' % (in_type, in_ds, out_ds, jobID, date, run))

						self.error('Could not start \'pathena\' job !\n%s' % output)

					#####################################

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

	#####################################################################

	try:
		i = 0

		while True:
			if i % 40 == 0:
				higgsHacker.checkGridProxy()

			higgsHacker.cron1('egamma_4L', 'data11_7TeV.*.physics_Egamma.merge.NTUP_HSG2.f*_m*_p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.higgs-analysis-%d_std.p600/')
#			higgsHacker.cron1('egamma_2L', 'data11_7TeV.*.physics_Egamma.merge.NTUP_2LHSG2.f*_m*_p600/', 'user.%s.data11_7TeV.%08d.physics_Egamma.merge.2Lhiggs-analysis_std-%d.p600/')
			higgsHacker.cron1('muon_4L', 'data11_7TeV.*.physics_Muons.merge.NTUP_HSG2.f*_m*_p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.higgs-analysis-%d_std.p600/')
#			higgsHacker.cron1('muon_2L', 'data11_7TeV.*.physics_Muons.merge.NTUP_2LHSG2.f*_m*_p600/', 'user.%s.data11_7TeV.%08d.physics_Muons.merge.2Lhiggs-analysis-%d_std.p600/')

			time.sleep(30)

			higgsHacker.cron2()

			time.sleep(30)

			i += 1

	except KeyboardInterrupt:
		higgsHacker.commit()
		higgsHacker.close()

		print('Bye')

	#####################################################################

