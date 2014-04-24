import os
import sys
import radical.pilot
import saga

# DBURL defines the MongoDB server URL and has the format mongodb://host:port.
# For the installation of a MongoDB server, refer to the MongoDB website:
# http://docs.mongodb.org/manual/installation/
DBURL = os.getenv("RADICALPILOT_DBURL")
if DBURL is None:
    print "ERROR: RADICALPILOT_DBURL (MongoDB server URL) is not defined."
    sys.exit(1)

# RCONF points to the resource configuration files. Read more about resource
# configuration files at http://saga-pilot.readthedocs.org/en/latest/machconf.html
RCONF = ["https://raw.github.com/radical-cybertools/radical.pilot/master/configs/xsede.json",
          "https://raw.github.com/radical-cybertools/radical.pilot/master/configs/futuregrid.json"]

INDIA_STAGING = '///N/u/marksant/staging_area/'
INDIA_HOST = 'india.futuregrid.org'

SHARED_INPUT_FILE = 'shared_input_file.txt'

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsualtes the MongoDB connection(s) as
        # well as security crendetials.
        session = radical.pilot.Session(database_url=DBURL)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = radical.pilot.PilotManager(session=session, resource_configurations=RCONF)

        # Define a 32-core on stamped that runs for 15 mintutes and
        # uses $HOME/radical.pilot.sandbox as sandbox directoy.
        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource = "india.futuregrid.org"
        pdesc.runtime = 15 # minutes
        pdesc.cores = 8

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Define and create staging directory for the intermediate data on the remote machine
        remote_dir_url = saga.Url()
        remote_dir_url.scheme = 'sftp'
        remote_dir_url.host = INDIA_HOST
        remote_dir_url.path = INDIA_STAGING
        remote_dir = saga.filesystem.Directory(remote_dir_url, saga.filesystem.CREATE)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = radical.pilot.UnitManager(
            session=session,
            scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

        # Add the previsouly created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)

        # Configure the staging directive for input data
        sd_input = radical.pilot.StagingDirectives()
        sd_input.source = 'input_file.txt'
        sd_input.target = 'input_file.txt' # Could be left empty if filename is same
        sd_input.action = radical.pilot.StagingDirectives.TRANSFER
        sd_input.moment = radical.pilot.StagingDirectives.BEGIN

        # Configure the staging directive for intermediate data
        sd_inter_out = radical.pilot.StagingDirectives()
        sd_inter_out.source = 'intermediate_file.txt'
        sd_inter_out.target = INDIA_STAGING
        sd_inter_out.action = radical.pilot.StagingDirectives.COPY
        sd_inter_out.moment = radical.pilot.StagingDirectives.END_SUCCESS

        # Task 1: Sort the input file and output to intermediate file
        cud1 = radical.pilot.ComputeUnitDescription()
        cud1.executable = '/usr/bin/sort'
        cud1.arguments = 'input_file.txt > intermediate_file.txt'.split()
        cud1.staging_directives = [sd_input, sd_inter_out]
        cud1.cores = 1

        # Submit the first task for execution.
        umgr.submit_units(cud1)

        # Wait for the compute unit to finish.
        umgr.wait_units()

        # Configure the staging directive for input intermediate data
        sd_inter_in = radical.pilot.StagingDirectives()
        sd_inter_in.source = INDIA_STAGING
        sd_inter_in.target = 'intermediate_file.txt'
        sd_inter_in.action = radical.pilot.StagingDirectives.LINK
        sd_inter_in.moment = radical.pilot.StagingDirectives.BEGIN

        # Configure the staging directive for output data
        sd_output = radical.pilot.StagingDirectives()
        sd_output.source = 'output_file.txt'
        sd_output.target = 'output_file.txt' # Could be left out if same as source
        sd_output.action = radical.pilot.StagingDirectives.TRANSFER
        sd_output.moment = radical.pilot.StagingDirectives.END

        # Task 2: Take the first line of the sort intermediate file and write to output
        cud2 = radical.pilot.ComputeUnitDescription()
        cud2.executable = '/usr/bin/head'
        cud2.arguments = ('-n1 intermediate_file.txt > output_file.txt').split()
        cud2.staging_directives = [sd_inter_in, sd_output]
        cud2.cores = 1

        # Submit the second CU for execution.
        umgr.submit_units(cud2)

        # Wait for the compute unit to finish.
        umgr.wait_units()

        session.close()

    except radical.pilot.PilotException, ex:
        print "Error: %s" % ex
