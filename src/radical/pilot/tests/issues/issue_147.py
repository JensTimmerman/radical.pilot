""" (Compute) Unit tests
"""
import os
import sys
import radical.pilot
import unittest

import uuid
from copy import deepcopy
from radical.pilot.db import Session
from pymongo import MongoClient

# RADICAL_PILOT_DBURL defines the MongoDB server URL and has the format
# mongodb://host:port/db_name

RP_DBENV = os.environ.get("RADICAL_PILOT_DBURL")
if not RP_DBENV:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

RP_DBURL = ru.Url (RP_DBENV)
if not (RP_DBURL.path and len(RP_DBURL.path) > 1):
    RP_DBURL=ru.generate_id ('rp_test.')

DBURL      = ru.URL(RP_DBURL)
DBURL.path = None
DBURL      = str(DBURL)

DBNAME     = RP_DBURL.path.lstrip('/')


#-----------------------------------------------------------------------------
#
class TestIssue147(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        client = MongoClient(DBURL)
        client.drop_database(DBNAME)

    def tearDown(self):
        # clean up after ourselves 
        client = MongoClient(DBURL)
        client.drop_database(DBNAME)

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    #-------------------------------------------------------------------------
    #
    def test__issue_147_part_1(self):
        """ https://github.com/radical-cybertools/radical.pilot/issues/163
        """
        session = radical.pilot.Session(database_url=DBURL)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores    = 1
        cpd.runtime  = 1
        cpd.sandbox  = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup  = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        um = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION
        )
        um.add_pilots(pilot)

        cudesc = radical.pilot.ComputeUnitDescription()
        cudesc.cores      = 1
        cudesc.executable = "/bin/sleep"
        cudesc.arguments  = ['1']

        cu = um.submit_units(cudesc)
        um.wait_units(timeout=5*60)

        session_id = session.uid

        session.close(cleanup=False)

        # NOW LET'S TRY TO RECONNECT
        session = radical.pilot.Session(database_url=DBURL, uid=session_id)

        for pm_id in session.list_pilot_managers():
            pm = session.get_pilot_managers(pm_id)
            assert len(pm.list_pilots()) == 1

        for um_id in session.list_unit_managers():
            um = session.get_unit_managers(um_id)
            assert len(um.list_units()) == 1

        # YUP. SEEMS TO WORK.

        session.close()

