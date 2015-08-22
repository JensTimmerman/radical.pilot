#!/usr/bin/env python

import os
import sys
import json
import time
import pytest
import logging
import datetime
import radical.pilot as rp

logging.raiseExceptions = False

db_url     = "mongodb://ec2-54-221-194-147.compute-1.amazonaws.com:24242/"

json_data=open("../pytest_credentials.json")
CREDENTIALS = json.load(json_data)
json_data.close()

#-------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        sys.exit (1)

    if state in [rp.DONE, rp.FAILED, rp.CANCELED]:
        for cb in pilot.callback_history:
            print cb

#-------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):
    """ this callback is invoked on all unit state changes """

    if not unit :
        return

    global cb_counter
    cb_counter += 1

    print "[Callback]: ComputeUnit  '%s: %s' (on %s) state: %s." \
        % (unit.name, unit.uid, unit.pilot_id, state)

    if state == rp.FAILED:
        print "stderr: %s" % unit.stderr
        sys.exit (1)

    if state in [rp.DONE, rp.FAILED, rp.CANCELED]:
        for cb in unit.callback_history:
            print cb

#-------------------------------------------------------------------------------
#
@pytest.fixture(scope="module")
def setup_local_1(request):

    session1 = rp.Session(database_url=db_url, 
                         database_name='rp-testing')

    print "session id local_1: {0}".format(session1.uid)

    try:
        pmgr1 = rp.PilotManager(session=session1)

        print "pm id local_1: {0}".format(pmgr1.uid)

        umgr1 = rp.UnitManager (session=session1,
                               scheduler=rp.SCHED_DIRECT_SUBMISSION)

        pdesc1 = rp.ComputePilotDescription()
        pdesc1.resource = "local.localhost"
        pdesc1.runtime  = 30
        pdesc1.cores    = 16
        pdesc1.cleanup  = False

        pilot1 = pmgr1.submit_pilots(pdesc1)
        pilot1.register_callback(pilot_state_cb)

        umgr1.add_pilots(pilot1)

    except Exception as e:
        print 'test failed'
        raise

    def fin():
        print "finalizing..."
        pmgr1.cancel_pilots()       
        pmgr1.wait_pilots() 

        print 'closing session'
        session1.close()
        time.sleep(5)

    request.addfinalizer(fin)

    return session1, pilot1, pmgr1, umgr1

#-------------------------------------------------------------------------------
#
@pytest.fixture(scope="module")
def setup_local_2(request):

    session1 = rp.Session(database_url=db_url, 
                         database_name='rp-testing')

    print "session id local_2: {0}".format(session1.uid)

    try:
        pmgr1 = rp.PilotManager(session=session1)

        print "pm id local_2: {0}".format(pmgr1.uid)

        umgr1 = rp.UnitManager (session=session1,
                               scheduler=rp.SCHED_DIRECT_SUBMISSION)

        pdesc1 = rp.ComputePilotDescription()
        pdesc1.resource = "local.localhost"
        pdesc1.runtime  = 30
        pdesc1.cores    = 16
        pdesc1.cleanup  = False

        pilot1 = pmgr1.submit_pilots(pdesc1)
        pilot1.register_callback(pilot_state_cb)

        umgr1.add_pilots(pilot1)

    except Exception as e:
        print 'test failed'
        raise

    def fin():
        print "finalizing..."
        pmgr1.cancel_pilots()       
        pmgr1.wait_pilots() 

        print 'closing session'
        session1.close()
        time.sleep(5)

    request.addfinalizer(fin)

    return session1, pilot1, pmgr1, umgr1

#-------------------------------------------------------------------------------
#
@pytest.fixture(scope="module")
def setup_gordon(request):

    session1 = rp.Session(database_url=db_url, 
                         database_name='rp-testing')

    print "session id gordon: {0}".format(session1.uid)


    c = rp.Context('ssh')
    c.user_id = CREDENTIALS["xsede.gordon"]["user_id"]
    session1.add_context(c)

    try:
        pmgr1 = rp.PilotManager(session=session1)

        print "pm id gordon: {0}".format(pmgr1.uid)

        umgr1 = rp.UnitManager (session=session1,
                               scheduler=rp.SCHED_DIRECT_SUBMISSION)

        pdesc1 = rp.ComputePilotDescription()
        pdesc1.resource = "xsede.gordon"
        pdesc1.project  = CREDENTIALS["xsede.gordon"]["project"]
        pdesc1.runtime  = 30
        pdesc1.cores    = 16
        pdesc1.cleanup  = False

        pilot1 = pmgr1.submit_pilots(pdesc1)
        pilot1.register_callback(pilot_state_cb)

        umgr1.add_pilots(pilot1)

    except Exception as e:
        print 'test failed'
        raise

    def fin():
        print "finalizing..."
        pmgr1.cancel_pilots()       
        pmgr1.wait_pilots() 

        print 'closing session'
        session1.close()
        time.sleep(5)

    request.addfinalizer(fin)

    return session1, pilot1, pmgr1, umgr1

#-------------------------------------------------------------------------------
#
@pytest.fixture(scope="module")
def setup_comet(request):

    session2 = rp.Session(database_url=db_url, 
                         database_name='rp-testing')

    print "session id comet: {0}".format(session2.uid)

    c = rp.Context('ssh')
    c.user_id = CREDENTIALS["xsede.comet"]["user_id"]
    session2.add_context(c)

    try:
        pmgr2 = rp.PilotManager(session=session2)

        print "pm id gordon: {0}".format(pmgr2.uid)

        umgr2 = rp.UnitManager (session=session2,
                               scheduler=rp.SCHED_DIRECT_SUBMISSION)

        pdesc2 = rp.ComputePilotDescription()
        pdesc2.resource = "xsede.comet"
        pdesc2.project  = CREDENTIALS["xsede.comet"]["project"]
        pdesc2.runtime  = 30
        pdesc2.cores    = 24
        pdesc2.cleanup  = False

        pilot2 = pmgr2.submit_pilots(pdesc2)
        pilot2.register_callback(pilot_state_cb)

        umgr2.add_pilots(pilot2)

    except Exception as e:
        print 'test failed'
        raise

    def fin():
        print "finalizing..."
        pmgr2.cancel_pilots()       
        pmgr2.wait_pilots() 

        print 'closing session'
        session2.close()

    request.addfinalizer(fin)

    return session2, pilot2, pmgr2, umgr2

#-------------------------------------------------------------------------------
#
def test_one(setup_local_1, setup_local_2):

    session, pilot, pmgr, umgr = setup_local_1

    print "session id test: {0}".format(session.uid)

    compute_units = []
    for unit_count in range(0, 4):
        cu = rp.ComputeUnitDescription()
        cu.executable = "/bin/date"
        cu.cores = 1

        compute_units.append(cu)

    units = umgr.submit_units(compute_units)
    umgr.wait_units()

    # Wait for all compute units to finish.
    for unit in units:
        unit.wait()

    for unit in units:
        assert (unit.state == rp.DONE)

#-------------------------------------------------------------------------------
# add more generix tests below

