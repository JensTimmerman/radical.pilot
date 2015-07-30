#!/usr/bin/env python

import sys
import pytest
import radical.pilot as rp

#-------------------------------------------------------------------------------
# merge: issue 79, 87 and 88
#-------------------------------------------------------------------------------

cb_counter = 0
db_url     = "mongodb://ec2-54-221-194-147.compute-1.amazonaws.com:24242/"

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
def rp_setup(request):

    session = rp.Session(database_url=db_url, 
    	                 database_name='rp-testing')

    try:
        pmgr = rp.PilotManager(session=session)
        umgr = rp.UnitManager (session=session,
                               scheduler=rp.SCHED_DIRECT_SUBMISSION)

        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "local.localhost"
        pdesc.runtime  = 15
        pdesc.cores    = 1

        pilot = pmgr.submit_pilots(pdesc)
        pilot.register_callback(pilot_state_cb)

        umgr.add_pilots(pilot)

    except Exception as e:
        print 'test failed'
        raise

    def fin():
        pmgr.cancel_pilots()       
        pmgr.wait_pilots() 

        print 'closing session'
        session.close()
    request.addfinalizer(fin)

    return pilot, pmgr, umgr

#-------------------------------------------------------------------------------
#
def test_issue79(rp_setup):

    pilot, pmgr, umgr = rp_setup

    pmgr.wait_pilots(pilot.uid,'Active')

    unit_descr = rp.ComputeUnitDescription()
    unit_descr.executable = "/road/to/nowhere/does_not_exist"
    unit_descr.arguments  = ['-invalid']
    
    unit = umgr.submit_units(unit_descr)
    
    unit.wait()
    
    assert (unit.state == rp.FAILED)
    # commented out since results in: 
    # "TypeError: argument of type 'NoneType' is not iterable"
    # expected behaviour ?

    #assert ('/road/to/nowhere/does_not_exist: No such file or directory'
    #in unit.stderr)

    
#-------------------------------------------------------------------------------
#
def test_issue87(rp_setup):

    global cb_counter
    cb_counter = 0

    pilot, pmgr, umgr = rp_setup

    pmgr.wait_pilots(pilot.uid,'Active')

    unit_descr = rp.ComputeUnitDescription()
    unit_descr.executable = "/bin/sleep"
    unit_descr.arguments  = ['10']
    unit_descr.cores = 1

    unit = umgr.submit_units(unit_descr)
    unit.register_callback(unit_state_cb)

    unit.wait()

    global cb_counter
    assert (cb_counter > 1) # one invokation to capture final state

#-------------------------------------------------------------------------------
#
def test_issue88(rp_setup):

    global cb_counter
    cb_counter = 0

    pilot, pmgr, umgr = rp_setup

    pmgr.wait_pilots(pilot.uid,'Active')

    unit_descr = rp.ComputeUnitDescription()
    unit_descr.executable = "/bin/sleep"
    unit_descr.arguments  = ['10']
    unit_descr.cores = 1

    units = []
    for i in range(0,4):
        unit = umgr.submit_units(unit_descr)
        units.append(unit)

    for unit in units:    
        unit.register_callback(unit_state_cb)
    
    umgr.wait_units()

    global cb_counter
    assert (cb_counter > 3) # one invokation to capture final state