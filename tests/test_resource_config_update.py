import os
import sys
import radical.pilot

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!
#
# RADICAL-Pilot uses ssh to communicate with the remote resource. The 
# easiest way to make this work seamlessly is to set up ssh key-based
# authentication and add the key to your keychain so you won't be 
# prompted for a password. The following article explains how to set 
# this up on Linux:
#   http://www.cyberciti.biz/faq/ssh-password-less-login-with-dsa-publickey-authentication/


# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to http://docs.mongodb.org.
DBURL = os.getenv("RADICAL_PILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICAL_PILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

#------------------------------------------------------------------------------
#
def pilot_state_cb(pilot, state):
    """pilot_state_change_cb() is a callback function. It gets called very
    time a ComputePilot changes its state.
    """
    print "[Callback]: ComputePilot '{0}' state changed to {1}.".format(
        pilot.uid, state)

    if state == radical.pilot.states.FAILED:
        sys.exit(1)

#------------------------------------------------------------------------------
#
def unit_state_change_cb(unit, state):
    """unit_state_change_cb() is a callback function. It gets called very
    time a ComputeUnit changes its state.
    """
    print "[Callback]: ComputeUnit '{0}' state changed to {1}.".format(
        unit.uid, state)
    if state == radical.pilot.states.FAILED:
        print "            Log: %s" % unit.log[-1]

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
        # well as security crendetials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add an ssh identity to the session.
        cred = radical.pilot.SSHCredential()
        session.add_credential(cred)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session)

        # Get all configs,
        res = pmgr.list_resource_configs()
        # ... and the entry specific for stampede
        s = res['stampede.tacc.utexas.edu']
        print 'Default queue of stampede is: "%s".' % s['default_queue']

        # Build a new one based on Stampede's
        rc = radical.pilot.ResourceConfig(s)
        #rc.name = 'testing'

        # And set the queue to development to get a faster turnaround
        rc.default_queue = 'development'

        # Now add the entry back to the PM
        pmgr.add_resource_config(rc)

        # Get all configs,
        res = pmgr.list_resource_configs()
        # ... and the entry specific for stampede
        s = res['stampede.tacc.utexas.edu']
        #s = res['testing']
        print 'Default queue of stampede after change is: "%s".' % s['default_queue']

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # Define a 32-core on stamped that runs for 15 mintutes and 
        # uses $HOME/radical.pilot.sandbox as sandbox directoy. 
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource  = "stampede.tacc.utexas.edu"
        #pdesc.resource  = "testing"
        pdesc.runtime   = 15 # minutes
        pdesc.cores     = 16
        pdesc.cleanup   = True

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        compute_units = []

        for unit_count in range(0, 1):
            cu = radical.pilot.ComputeUnitDescription()
            cu.executable  = "/bin/date"
            cu.cores       = 1

            compute_units.append(cu)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_change_cb)

        # Add the previsouly created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(compute_units)

        # Wait for all compute units to reach a terminal state (DONE or FAILED).
        umgr.wait_units()

        if not isinstance(units, list):
            units = [units]
        for unit in units:
            print "* Task %s (executed @ %s) state: %s, exit code: %s, started: %s, finished: %s, output: %s" \
                % (unit.uid, unit.execution_locations, unit.state, unit.exit_code, unit.start_time, unit.stop_time,
                   unit.stdout)

        # Close automatically cancels the pilot(s).
        session.close(delete=False)
        sys.exit(0)

    except radical.pilot.PilotException, ex:
        # Catch all exceptions and exit with and error.
        print "Error during execution: %s" % ex
        sys.exit(1)
