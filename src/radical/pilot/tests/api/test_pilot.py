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
class TestPilot(unittest.TestCase):
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
    def test__pilot_wait(self):
        """ Test if we can wait for different pilot states.
        """
        session = radical.pilot.Session(database_url=DBURL)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        assert pilot is not None
        assert pilot.started is None
        assert pilot.finished is None

        pilot.wait(state=[radical.pilot.ACTIVE, radical.pilot.FAILED], timeout=5*60)
        assert pilot.submitted is not None
        assert pilot.state == radical.pilot.ACTIVE
        assert pilot.started is not None
        assert pilot.log is not None
        assert pilot.sandbox == "file://localhost%s/pilot-%s/" % (cpd.sandbox, pilot.uid)

        # the pilot should finish after it has reached run_time

        pilot.wait(timeout=5*60)
        assert pilot.state == radical.pilot.DONE
        assert pilot.finished is not None

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__pilot_errors(self):
        """ Test if pilot errors are raised properly.
        """
        session = radical.pilot.Session(database_url=DBURL)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/non-/existing/directory..."
        cpd.cleanup = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)
        pilot.wait(timeout=5*60)
        assert pilot.state == radical.pilot.FAILED, "State is '%s' instead of 'Failed'." % pilot.state

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 100000000000  # This should fail - at least in 2014 ;-)
        cpd.runtime = 1
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)
        pilot.wait(timeout=5*60)
        assert pilot.state == radical.pilot.FAILED, ("state should be %s and not %s" % (radical.pilot.FAILED, pilot.state))

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__pilot_cancel(self):
        """ Test if we can cancel a pilot.
        """
        session = radical.pilot.Session(database_url=DBURL)

        pm = radical.pilot.PilotManager(session=session)

        cpd = radical.pilot.ComputePilotDescription()
        cpd.resource = "local.localhost"
        cpd.cores = 1
        cpd.runtime = 1
        cpd.sandbox = "/tmp/radical.pilot.sandbox.unittests"
        cpd.cleanup = True

        pilot = pm.submit_pilots(pilot_descriptions=cpd)

        assert pilot is not None
        assert pilot.started is None
        assert pilot.finished is None

        pilot.wait(state=[radical.pilot.ACTIVE, radical.pilot.FAILED], timeout=5*60)
        assert pilot.submitted is not None
        assert pilot.state == radical.pilot.ACTIVE
        assert pilot.started is not None

        # the pilot should finish after it has reached run_time
        pilot.cancel()

        pilot.wait(timeout=5*60)
        assert pilot.state == radical.pilot.CANCELED
        assert pilot.finished is not None

        session.close()
