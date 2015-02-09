"""API layer tests
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
class Test_Session(unittest.TestCase):
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
    def test__session_create(self):
        """ Tests if creating a new session works as epxected.
        """
        for _ in range(1, 4):
            session = radical.pilot.Session(database_url=DBURL)
            
        client = MongoClient(DBURL)
        collections = client[DBNAME].collection_names()
        assert len(collections) == 4, "Wrong number of sessions in database"

        session.close()

    #-------------------------------------------------------------------------
    #
    def test__session_reconnect(self):
        """ Tests if reconnecting to an existing session works as epxected.
        """
        session_ids = []
        for _ in range(1, 4):
            session = radical.pilot.Session(database_url=DBURL)
            session_ids.append(session.uid)

        for sid in session_ids:
            session_r = radical.pilot.Session(database_url=DBURL, uid=sid)
            assert session_r.uid == sid, "Session IDs don't match"

        session.close()

