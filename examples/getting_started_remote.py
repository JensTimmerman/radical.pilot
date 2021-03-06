#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp
import radical.utils as ru

dh = ru.DebugHelper ()

CNT      =     0
RUNTIME  =    10
SLEEP    =     1
CORES    =    26
UNITS    =    50
SCHED    = rp.SCHED_DIRECT_SUBMISSION

resources = {
        'local.localhost' : {
            'project'  : None,
            'queue'    : None,
            'schema'   : None
            },
        'home.test' : {
            'project'  : None,
            'queue'    : None,
            'schema'   : 'ssh'
            },

        'epsrc.archer' : {
            'project'  : 'e290',
            'queue'    : 'short',
            'schema'   : None
            },

        'lrz.supermuc' : {
            'project'  : 'e290',
            'queue'    : 'short',
            'schema'   : None
            },

        'xsede.stampede' : {
            'project'  : 'TG-CCR140028',
            'queue'    : 'development',
            'schema'   : None
            },

        'xsede.comet' : {
            'project'  : 'unc100',
            'queue'    : '',
            'schema'   : None
            },

        'xsede.gordon' : {
            'project'  : None,
            'queue'    : 'debug',
            'schema'   : None
            },

        'xsede.blacklight' : {
            'project'  : None,
            'queue'    : 'debug',
            'schema'   : 'gsissh'
            },

        'xsede.trestles' : {
            'project'  : 'TG-MCB090174' ,
            'queue'    : 'shared',
            'schema'   : None
            },

        'futuregrid.india' : {
            'project'  : None,
            'queue'    : None,
            'schema'   : None
            },

        'nersc.hopper' : {
                'project'  : None,
                'queue'    : 'debug',
                'schema'   : 'ssh'
                }
        }

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    if not unit:
        return

    global CNT

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if state in [rp.FAILED, rp.DONE, rp.CANCELED]:
        CNT += 1
        print "[Callback]: # %6d" % CNT


    if state == rp.FAILED:
        print "stderr: %s" % unit.stderr
        sys.exit(2)


#------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):

    print "[Callback]: wait_queue_size: %s." % wait_queue_size


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # we can optionally pass session name to RP
    if len(sys.argv) > 1:
        resource = sys.argv[1]
    else:
        resource = 'local.localhost'

    print 'running on %s' % resource

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()
    print "session id: %s" % session.uid

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        pmgr = rp.PilotManager(session=session)
        pmgr.register_callback(pilot_state_cb)

        pdesc = rp.ComputePilotDescription()
        pdesc.resource      = resource
        pdesc.cores         = CORES
        pdesc.project       = resources[resource]['project']
        pdesc.queue         = resources[resource]['queue']
        pdesc.runtime       = RUNTIME
        pdesc.cleanup       = False
        pdesc.access_schema = resources[resource]['schema']

        pilot = pmgr.submit_pilots(pdesc)

        input_sd_pilot = {
                'source': 'file:///etc/passwd',
                'target': 'staging:///f1',
                'action': rp.TRANSFER
                }
        pilot.stage_in (input_sd_pilot)

        umgr = rp.UnitManager(session=session, scheduler=SCHED)
        umgr.register_callback(unit_state_cb,      rp.UNIT_STATE)
        umgr.register_callback(wait_queue_size_cb, rp.WAIT_QUEUE_SIZE)
        umgr.add_pilots(pilot)

        input_sd_umgr   = {'source':'/etc/group',        'target': 'f2',                'action': rp.TRANSFER}
        input_sd_agent  = {'source':'staging:///f1',     'target': 'f1',                'action': rp.COPY}
        output_sd_agent = {'source':'f1',                'target': 'staging:///f1.bak', 'action': rp.COPY}
        output_sd_umgr  = {'source':'f2',                'target': 'f2.bak',            'action': rp.TRANSFER}

        cuds = list()
        for unit_count in range(0, UNITS):
            cud = rp.ComputeUnitDescription()
            cud.executable     = "wc"
            cud.arguments      = ["f1", "f2"]
            cud.cores          = 1
            cud.input_staging  = [ input_sd_umgr,  input_sd_agent]
            cud.output_staging = [output_sd_umgr, output_sd_agent]
            cuds.append(cud)

        units = umgr.submit_units(cuds)

        umgr.wait_units()

        for cu in units:
            print "* Task %s state %s, exit code: %s, started: %s, finished: %s" \
                % (cu.uid, cu.state, cu.exit_code, cu.start_time, cu.stop_time)

      # os.system ("radicalpilot-stats -m stat,plot -s %s > %s.stat" % (session.uid, session_name))


    except Exception as e:
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print "need to exit now: %s" % e

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print "closing session"
        session.close ()

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


#-------------------------------------------------------------------------------

