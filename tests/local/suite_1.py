#!/usr/bin/env python

import os
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
        pdesc.runtime  = 20
        pdesc.cores    = 1
        pdesc.cleanup  = True

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
@pytest.fixture(scope="module")
def rp_setup_short(request):

    session = rp.Session(database_url=db_url, 
                         database_name='rp-testing')

    try:
        pmgr = rp.PilotManager(session=session)
        umgr = rp.UnitManager (session=session,
                               scheduler=rp.SCHED_DIRECT_SUBMISSION)

        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "local.localhost"
        pdesc.runtime  = 1
        pdesc.cores    = 1
        pdesc.sandbox  = "/tmp/radical.pilot.sandbox.unittests"
        pdesc.cleanup  = True

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
    unit_descr.arguments  = ['5']
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


#-------------------------------------------------------------------------------
#
def test_issue114_3(rp_setup_short):

    pilot, pmgr, umgr = rp_setup_short

    state = pmgr.wait_pilots(state=[rp.ACTIVE, 
                                    rp.DONE, 
                                    rp.FAILED], 
                                    timeout=20*60)
    
    assert state       == [rp.ACTIVE], 'state      : %s' % state    
    assert pilot.state ==  rp.ACTIVE , 'pilot state: %s' % pilot.state 
    
    state = pmgr.wait_pilots(timeout=3*60)
    
    #print "pilot %s: %s / %s" % (pilot.uid, pilot.state, state)
    #for entry in pilot.state_history :
    #    print "      %s : %s" % (entry.timestamp, entry.state)
    #for log in pilot.log :
    #    print "      log : %s" % log
    
    assert state       == [rp.DONE], 'state      : %s' % state        
    assert pilot.state ==  rp.DONE , 'pilot state: %s' % pilot.state  

#-------------------------------------------------------------------------------
#
def test_issue133(rp_setup):

    pilot, pmgr, umgr = rp_setup

    compute_units = []
    for unit_count in range(0, 4):
        cu = rp.ComputeUnitDescription()
        cu.executable = "/bin/date"
        cu.cores = 1

        compute_units.append(cu)

    units = umgr.submit_units(compute_units)

    # Wait for all compute units to finish.
    for unit in units:
        unit.wait()

    for unit in units:
        #print "unit done: %s" % (unit.uid)
        assert (unit.state == rp.DONE)

#-------------------------------------------------------------------------------
#
def test_issue165(rp_setup):

    pilot, pmgr, umgr = rp_setup

    pmgr.register_callback(pilot_state_cb)
    umgr.register_callback(unit_state_cb)

    # prepare some input files for the compute units
    os.system ('hostname > file1.dat')
    os.system ('date     > file2.dat')

    cud = rp.ComputeUnitDescription()
    #---------------------------------------------------------------------------
    # Arguments are all treated as strings and don't need special quoting 
    # in the CUD.
    #---------------------------------------------------------------------------
    cud.executable = "/bin/bash"
    cud.arguments = ["-l", "-c", "cat ./file1.dat ./file2.dat > result.dat"]
    #---------------------------------------------------------------------------
    # In the backend, arguments containing spaces will get special treatment, 
    # so that they remain intact as strings.
    # This CUD will thus be executed as: 
    # /bin/bash -l -c "cat ./file1.dat ./file2.dat > result.dat"
    #---------------------------------------------------------------------------
    cud.input_staging  = ['file1.dat', 'file2.dat']
    cud.output_staging = ['result.dat']

    unit = umgr.submit_units(cud)
    unit.wait()

    assert (unit.state == rp.DONE)

    # delete the test data files
    os.system ('rm -f file1.dat')
    os.system ('rm -f file2.dat')
    os.system ('rm -f result.dat')
