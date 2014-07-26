#!/usr/bin/env python

"""
.. module::   radical.pilot.agent
   :platform: Unix
   :synopsis: A multi-core agent for RADICAL-Pilot.

.. moduleauthor:: Mark Santcroos <mark.santcroos@rutgers.edu>
"""

__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import os.path as op
import sys
import time
import Queue
import signal
import gridfs
import pymongo
import optparse
import logging
import datetime
import hostlist
import traceback
import threading 
import subprocess
import multiprocessing
import radical.utils        as ru
import saga.utils.pty_shell as sups

from   bson.objectid import ObjectId


# ==============================================================================
#
# CONSTANTS
#
# ==============================================================================
OK                          = "OK"
FAIL                        = "FAIL"
RETRY                       = "RETRY"

FREE                        = '-'
BUSY                        = '#'

LAUNCH_METHOD_LOCAL         = 'LOCAL'
LAUNCH_METHOD_SSH           = 'SSH'
LAUNCH_METHOD_MPIRUN        = 'MPIRUN'
LAUNCH_METHOD_MPIEXEC       = 'MPIEXEC'
LAUNCH_METHOD_APRUN         = 'APRUN'
LAUNCH_METHOD_IBRUN         = 'IBRUN'
LAUNCH_METHOD_POE           = 'POE'

MULTI_NODE_LAUNCH_METHODS   =  [LAUNCH_METHOD_MPIRUN,
                                LAUNCH_METHOD_MPIEXEC, 
                                LAUNCH_METHOD_POE,
                                LAUNCH_METHOD_APRUN,
                                LAUNCH_METHOD_IBRUN]

CONT_SLOTS_LAUNCH_METHODS   =  [LAUNCH_METHOD_IBRUN]

LRMS_NAME_TORQUE            = 'TORQUE'
LRMS_NAME_PBSPRO            = 'PBSPRO'
LRMS_NAME_SLURM             = 'SLURM'
LRMS_NAME_SGE               = 'SGE'
LRMS_NAME_LSF               = 'LSF'
LRMS_NAME_LOADL             = 'LOADL'
LRMS_NAME_FORK              = 'FORK'

COMMAND_CANCEL_PILOT        = "Cancel_Pilot"
COMMAND_CANCEL_COMPUTE_UNIT = "Cancel_Compute_Unit"
COMMAND_LAUNCH_COMPUTE_UNIT = "Launch_Compute_Unit"
COMMAND_KEEP_ALIVE          = "Keep_Alive"
COMMAND_FIELD               = "commands"
COMMAND_TYPE                = "type"
COMMAND_ARG                 = "arg"

# Common  States
DONE                        = 'Done'
CANCELED                    = 'Canceled'
FAILED                      = 'Failed'

# ComputePilot States
PENDING_LAUNCH              = 'PendingLaunch'
LAUNCHING                   = 'Launching'
PENDING_ACTIVE              = 'PendingActive'
ACTIVE                      = 'Active'

# ComputeUnit States
NEW                         = 'New'
STATE_X                     = 'Scheduled'
PENDING_INPUT_TRANSFER      = 'PendingInputTransfer'
TRANSFERRING_INPUT          = 'TransferringInput'

PENDING_EXECUTION           = 'PendingExecution'
SCHEDULING                  = 'Scheduling'
EXECUTING                   = 'Executing'

PENDING_OUTPUT_TRANSFER     = 'PendingOutputTransfer'
TRANSFERRING_OUTPUT         = 'TransferringOutput'

UNKNOWN                     = 'Unknown'


# ==============================================================================
#
# Helper Methods 
#
# ------------------------------------------------------------------------------
#
def parse_commandline () :

    parser = optparse.OptionParser()

    parser.add_option('-c', '--cores',
                      metavar='CORES',
                      dest='cores',
                      type='int',
                      help='Specifies the number of cores to allocate.')

    parser.add_option('-d', '--debug',
                      metavar='DEBUG',
                      dest='debug_level',
                      type='int',
                      help='The DEBUG level for the agent.')

    parser.add_option('-j', '--task-launch-method',
                      metavar='METHOD',
                      dest='task_launch_method',
                      help='Specifies the task launch method.')

    parser.add_option('-k', '--mpi-launch-method',
                      metavar='METHOD',
                      dest='mpi_launch_method',
                      help='Specifies the MPI launch method.')

    parser.add_option('-l', '--lrms',
                      metavar='LRMS',
                      dest='lrms',
                      help='Specifies the LRMS type.')

    parser.add_option('-m', '--mongodb-url',
                      metavar='URL',
                      dest='mongodb_url',
                      help='Specifies the MongoDB Url.')

    parser.add_option('-n', '--database-name',
                      metavar='URL',
                      dest='database_name',
                      help='Specifies the MongoDB database name.')

    parser.add_option('-p', '--pilot-id',
                      metavar='PID',
                      dest='pilot_id',
                      help='Specifies the Pilot ID.')

    parser.add_option('-s', '--session-id',
                      metavar='SID',
                      dest='session_id',
                      help='Specifies the Session ID.')

    parser.add_option('-t', '--runtime',
                      metavar='RUNTIME',
                      dest='runtime',
                      help='Specifies the agent runtime in minutes.')

    parser.add_option('-v', '--version',
                      metavar='VERSION ',
                      dest='package_version',
                      help='The RADICAL-Pilot package version.')

    parser.add_option('-w', '--workdir',
                      metavar='DIRECTORY',
                      dest='workdir',
                      help='agent working directory. [default: %default]',
                      default='.')

    # parse the whole shebang
    (options, args) = parser.parse_args()

    if  args :
        parser.error ("undefined arguments. " \
                      "Try --help for help.")

    if  not options.mongodb_url :
        parser.error ("You must define MongoDB URL (-m/--mongodb-url). " \
                      "Try --help for help.")

    if  not options.database_name :
        parser.error ("You must define a database name (-n/--database-name). " \
                      "Try --help for help.")

    if  not options.session_id :
        parser.error ("You must define a session id (-s/--session-id). " \
                      "Try --help for help.")

    if  not options.pilot_id :
        parser.error ("You must define a pilot id (-p/--pilot-id). " \
                      "Try --help for help.")

    if  not options.cores :
        parser.error ("You must define the number of cores (-c/--cores). " \
                      "Try --help for help.")

    if  not options.runtime :
        parser.error ("You must define the agent runtime (-t/--runtime). " \
                      "Try --help for help.")

    if  not options.package_version :
        parser.error ("You must pass the RADICAL-Pilot version (-v/--version). " \
                              "Try --help for help.")

    if  not options.debug_level :
        parser.error ("You must pass the DEBUG level (-d/--debug). " \
                      "Try --help for help.")

    if  not options.lrms :
        parser.error ("You must pass the LRMS (-l/--lrms). " \
                      "Try --help for help.")

    return options


# ------------------------------------------------------------------------------
#
def string_to_state (state_str) :

    state_str = state_str.strip ()

    if state_str.lower () == 'done'      : return DONE
    if state_str.lower () == 'failed'    : return FAILED
    if state_str.lower () == 'canceled'  : return CANCELED

    return UNKNOWN


# ------------------------------------------------------------------------------
#
def pilot_FAILED (mongo_p, pilot_uid, logger, message) :

    logger.error (message)      

    mongo_p.update ({"_id"  : ObjectId(pilot_uid)}, 
                    {"$set" : {"state"       : 'Failed',
                               "capability"  : 0},
                     "$push": {"log"         : message,
                               "statehistory": {"state"    : 'Failed', 
                                                "timestamp": timestamp()}}
                    })

    sys.exit (1)


# ------------------------------------------------------------------------------
#
def pilot_CANCELED (mongo_p, pilot_uid, logger, message) :

    logger.info (message)

    mongo_p.update ({"_id"   : ObjectId(pilot_uid)}, 
                    {"$set"  : {"state"        : 'Canceled',
                                "capability"   : 0},
                     "$push" : {"log"          : message,
                                "statehistory" : {"state"     : 'Canceled', 
                                                  "timestamp" : timestamp()}}
                    })

    sys.exit (0)


# ------------------------------------------------------------------------------
#
def pilot_DONE (mongo_p, pilot_uid, logger):
    
    """Updates the state of one or more pilots.
    """

    logger.info ("Pilot Done")

    mongo_p.update({"_id"   : ObjectId(pilot_uid)}, 
                   {"$set"  : {"state"        : 'Done',
                               "capability"   : 0},
                    "$push" : {"statehistory" : {"state"     : 'Done', 
                                                 "timestamp" : timestamp()}}
                   })

    sys.exit (0)


# ------------------------------------------------------------------------------
#
def log_raise (logger, exception, message, *args) :

    logger.error    (traceback.format_exc())
    logger.error    (message, args)
    raise exception (message % args)


# ------------------------------------------------------------------------------
#
def timestamp () :

    return datetime.datetime.utcnow()


# ==============================================================================
#
# Launch Methods 
#
# ==============================================================================
#
class LaunchMethod (object) :

    # --------------------------------------------------------------------------
    def __init__ (self, launch_method, logger) :

        self.name   = launch_method
        self.logger = logger
        self.impl   = {LAUNCH_METHOD_LOCAL   : LaunchMethodLocal  (logger),
                       LAUNCH_METHOD_SSH     : LaunchMethodSSH    (logger),
                       LAUNCH_METHOD_MPIRUN  : LaunchMethodMPIRUN (logger),
                       LAUNCH_METHOD_MPIEXEC : LaunchMethodMPIEXEC(logger),
                       LAUNCH_METHOD_APRUN   : LaunchMethodAPRUN  (logger),
                       LAUNCH_METHOD_IBRUN   : LaunchMethodIBRUN  (logger),
                       LAUNCH_METHOD_POE     : LaunchMethodPOE    (logger),
                      }.get (launch_method, None)

        if  not self.impl :
            log_raise (self.logger, RuntimeError, 
                       "Unknown LaunchMethod '%s'", launch_method)


    # --------------------------------------------------------------------------
    def command (self, lrms, slots, cores) :

        return self.impl.command (slots, lrms, cores)


# ==============================================================================
#
class LaunchMethodBase (object) :

    # --------------------------------------------------------------------------
    def __init__ (self, logger) :

        self.logger = logger


    # --------------------------------------------------------------------------
    def command (self, lrms, slots, cores) :

        raise NotImplementedError ("undefined launch method")


# ==============================================================================
#
class LaunchMethodLocal (LaunchMethodBase) :

    # --------------------------------------------------------------------------
    def __init__ (self, logger) :

        LaunchMethodBase.__init__ (self, logger)

        self.cmd = ''


    # --------------------------------------------------------------------------
    def command (self, lrms, slots, cores) :

        return self.cmd


# ==============================================================================
#
class LaunchMethodSSH (LaunchMethodBase) :

    # --------------------------------------------------------------------------
    def __init__ (self, logger) :

        LaunchMethodBase.__init__ (self, logger)

        self.cmd = ru.which ('ssh')

        if  not self.cmd :
            log_raise (self.logger, RuntimeError, 
                       "Could not find 'ssh' in path")

        # Some MPI environments (e.g. SGE) put a link to rsh as "ssh" into 
        # the path.  We try to detect that and then use different arguments.
        if  op.islink (self.cmd) and \
            op.basename (op.realpath (self.cmd)) == 'rsh' :
            self.logger.info ('"ssh" is a link to "rsh".')
        else :
            self.cmd += ' -o StrictHostKeyChecking=no'


    # --------------------------------------------------------------------------
    def command (self, lrms, slots, cores) :

        # Get the host of the first entry in the acquired slot
        host = slots[0].split(':')[0] 
        cmd  = "%s %s" % (self.cmd, host)

        return cmd


# ==============================================================================
#
class LaunchMethodMPIRUN (LaunchMethodBase) :

    # --------------------------------------------------------------------------
    def __init__ (self, logger) :

        LaunchMethodBase.__init__ (self, logger)

        self.cmd = ru.which ('mpirun')

        # fallback for MacOS
        if  not self.cmd :
            self.cmd = ru.which ('mpirun-openmpi-mp')

        if  not self.cmd :
            log_raise (self.logger, RuntimeError, 
                       "Could not find 'mpirun' in path")


    # --------------------------------------------------------------------------
    def command (self, lrms, slots, cores) :

        # Construct the hosts_string
        hosts_string = ''
        for slot in slots:
            host = slot.split(':')[0]
            hosts_string += '%s,' % host

        cmd = "%s -np %s -host %s" % (self.cmd, cores, hosts_string)

        return cmd


# ==============================================================================
#
class LaunchMethodMPIEXEC (LaunchMethodBase) :

    # --------------------------------------------------------------------------
    def __init__ (self, logger) :

        LaunchMethodBase.__init__ (self, logger)

        self.cmd = ru.which ('mpiexec')

        if  not self.cmd :
            log_raise (self.logger, RuntimeError, 
                       "Could not find 'mpiexec' in path")


    # --------------------------------------------------------------------------
    def command (self, lrms, slots, cores) :

        # Construct the hosts_string
        hosts_string = ''
        for slot in slots:
            host = slot.split(':')[0]
            hosts_string += '%s,' % host

        cmd = "%s -n %s -hosts %s" % (self.cmd, cores, hosts_string)

        return cmd


# ==============================================================================
#
class LaunchMethodAPRUN (LaunchMethodBase) :

    # --------------------------------------------------------------------------
    def __init__ (self, logger) :

        LaunchMethodBase.__init__ (self, logger)

        self.cmd = ru.which ('aprun')

        if  not self.cmd :
            log_raise (self.logger, RuntimeError, 
                       "Could not find 'aprun' in path")


    # --------------------------------------------------------------------------
    def command (self, lrms, slots, cores) :

        cmd = "%s -n %s" % (self.cmd, cores)

        return cmd


# ==============================================================================
#
class LaunchMethodIBRUN (LaunchMethodBase) :

    # --------------------------------------------------------------------------
    def __init__ (self, logger) :

        LaunchMethodBase.__init__ (self, logger)

        self.cmd = ru.which ('ibrun')

        if  not self.cmd :
            log_raise (self.logger, RuntimeError, 
                       "Could not find 'ibrun' in path")


    # --------------------------------------------------------------------------
    def command (self, lrms, slots, cores) :

        # NOTE: Don't think that with IBRUN it is possible to have
        # processes != cores ...

        # Get the host and the core part from first slot
        [first_slot_host, first_slot_core] = slots[0].split(':')

        # Find the entry in the the all_slots list based on the host
        slot_entry = (slot for slot in lrms.slot_list \
                           if slot["node"] == first_slot_host).next()

        # Transform it into an index in to the all_slots list
        all_slots_slot_index = lrms.slot_list.index (slot_entry)

        # TODO: This assumes all hosts have the same number of cores
        offset = all_slots_slot_index * lrms.cores_per_node + int(first_slot_core)
        cmd    = "%s -n %s -o %d" % (self.cmd, cores, offset)

        return cmd



# ==============================================================================
#
class LaunchMethodPOE (LaunchMethodBase) :

    # --------------------------------------------------------------------------
    def __init__ (self, logger) :

        LaunchMethodBase.__init__ (self, logger)

        self.cmd = ru.which ('poe')

        if  not self.cmd :
            log_raise (self.logger, RuntimeError, 
                       "Could not find 'poe' in path")


    # --------------------------------------------------------------------------
    def command (self, lrms, slots, cores) :

        # Count slots per host in provided slots description.
        hosts = dict()
        for slot in slots :
            host = slot.split(':')[0]
            if  host not in hosts:
                hosts[host]  = 1
            else:
                hosts[host] += 1

        # Create string with format: "hostname slotnum ..."
        hosts_string = ''
        for host in hosts:
            hosts_string += '%s %d ' % (host, hosts[host])

        # Override the LSB_MCPU_HOSTS env variable as this is set by
        # default to the size of the whole pilot.
        cmd = 'LSB_MCPU_HOSTS="%s" %s' % (hosts_string, self.cmd)

        return cmd



# ==============================================================================
#
# LRMS
#
# ==============================================================================
#
class LRMS (object) :

    # --------------------------------------------------------------------------
    def __init__ (self, lrms, requested_cores, logger) :

        self.name            = lrms
        self.logger          = logger
        self.slots           = list()
        self.node_list       = list()
        self.cores_per_node  = None

        self.impl = {LRMS_NAME_TORQUE : LRMS_TORQUE (requested_cores, logger),
                     LRMS_NAME_PBSPRO : LRMS_PBSPRO (requested_cores, logger),
                     LRMS_NAME_SLURM  : LRMS_SLURM  (requested_cores, logger),
                     LRMS_NAME_SGE    : LRMS_SGE    (requested_cores, logger),
                     LRMS_NAME_LSF    : LRMS_LSF    (requested_cores, logger),
                     LRMS_NAME_LOADL  : LRMS_LOADL  (requested_cores, logger),
                     LRMS_NAME_FORK   : LRMS_FORK   (requested_cores, logger)
                    }.get (lrms, None)

        if  not self.impl :
            log_raise (self.logger, RuntimeError, "Unknown LRMS '%s'", lrms)

        self.node_list      = self.impl.node_list
        self.cores_per_node = self.impl.cores_per_node

        # Slots represent the internal process management structure, as follows:
        # [
        #   {'node': 'node1', 'cores': [p_1, p_2, p_3, ... , p_cores_per_node]},
        #   {'node': 'node2', 'cores': [p_1, p_2, p_3. ... , p_cores_per_node]
        # ]
        # We put it in a list because we care about (and make use of) the order.
        # Slots are either BUSY or FREE.

        self.slots = list()
        for node in self.node_list :
            
            # FIXME: use real core numbers for non-exclusive host reservations
            self.slots.append ({ 'node' : node,
                                 'cores': [FREE for _ in range (0, self.cores_per_node)] 
                               })


# ==============================================================================
#
class LRMS_Base (object) :

    # --------------------------------------------------------------------------
    def __init__ (self, requested_cores, logger) :

        self.requested_cores = requested_cores
        self.logger          = logger
        self.node_list       = list()
        self.cores_per_node  = None
        self.target_is_macos = False

        self.configure ()


    # --------------------------------------------------------------------------
    def configure (self) :

        log_raise (self.logger, NotImplementedError, "Unknonwn LRMS")


# ==============================================================================
# 
class LRMS_FORK (LRMS_Base) :

    # --------------------------------------------------------------------------
    def __init__ (self, requested_cores, logger) :

        LRMS_Base.__init__ (self, requested_cores, logger)


    # --------------------------------------------------------------------------
    def configure (self) :

        detected_cpus = multiprocessing.cpu_count()
        selected_cpus = min(detected_cpus, self.requested_cores)

        self.logger.info ("Detected %d cores on localhost, using %d." \
                       % (detected_cpus, selected_cpus))

        self.node_list      = ["localhost"]
        self.cores_per_node = selected_cpus


# ==============================================================================
# 
class LRMS_TORQUE (LRMS_Base) :

    # --------------------------------------------------------------------------
    def __init__ (self, requested_cores, logger) :

        LRMS_Base.__init__ (self, requested_cores, logger)


    # --------------------------------------------------------------------------
    def configure (self) :

        torque_nodefile = os.environ.get('PBS_NODEFILE', None)
        if  torque_nodefile is None:
            log_raise (self.logger, Exception, "$PBS_NODEFILE not set!")

        # Parse PBS the nodefile
        torque_nodes = [line.strip() for line in open(torque_nodefile)]
        self.logger.info ("Found Torque PBS_NODEFILE %s: %s" \
                       % (torque_nodefile, torque_nodes))

        # Number of nodes involved in allocation
        torque_num_nodes = os.environ.get('PBS_NUM_NODES', None)
        if  torque_num_nodes is None:
            self.logger.warning("$PBS_NUM_NODES not set! (old Torque version?)")
        else :
            torque_num_nodes = int(torque_num_nodes)

        # Number of cores (processors) per node
        torque_cores_per_node = os.environ.get('PBS_NUM_PPN', None)
        if  torque_cores_per_node is None :
            self.logger.warning("$PBS_NUM_PPN not set! (old Torque version?)")
        else : 
            torque_cores_per_node = int(torque_cores_per_node)

        # Number of entries in nodefile should be PBS_NUM_NODES * PBS_NUM_PPN
        torque_nodes_length = len(torque_nodes)
        if  torque_num_nodes and torque_cores_per_node and \
            torque_nodes_length != torque_num_nodes * torque_cores_per_node :
            log_raise (self.logger, Exception, 
                       "Number of entries in $PBS_NODEFILE (%s) does " \
                       "not match $PBS_NUM_NODES*$PBS_NUM_PPN (%s*%s)", \
                       (torque_nodes_length, torque_nodes, torque_cores_per_node))

        # only unique node names
        torque_node_list        = list(set(torque_nodes))
        torque_node_list_length = len(torque_node_list)

        self.logger.debug ("Node list: %s(%d)" 
                        % (torque_node_list, torque_node_list_length))

        self.node_list = torque_node_list
        if  torque_num_nodes and torque_cores_per_node:
            # Modern style Torque
            self.cores_per_node = torque_cores_per_node
        else:
            # Old style Torque (Should we just use this for all versions?)
            self.cores_per_node = torque_nodes_length / torque_node_list_length


# ==============================================================================
# 
class LRMS_PBSPRO (LRMS_Base) :

    # --------------------------------------------------------------------------
    def __init__ (self, requested_cores, logger) :

        LRMS_Base.__init__ (self, requested_cores, logger)


    # --------------------------------------------------------------------------
    def configure (self) :

        # TODO: $NCPUS?!?! = 1 on archer

        pbspro_nodefile = os.environ.get('PBS_NODEFILE', None)
        if  pbspro_nodefile is None:
            log_raise (self.logger, Exception, "$PBS_NODEFILE not set!")

        # Number of Processors per Node
        pbspro_num_ppn = os.environ.get('NUM_PPN', None)
        if  pbspro_num_ppn is None:
            log_raise (self.logger, Exception, "$NUM_PPN not set!")

        # Number of Nodes allocated
        pbspro_node_count = os.environ.get('NODE_COUNT', None)
        if  pbspro_node_count is None :
            log_raise (self.logger, Exception, "$NODE_COUNT not set!")

        # Number of Parallel Environments
        pbspro_num_pes = os.environ.get('NUM_PES', None)
        if  pbspro_num_pes :
            log_raise (self.logger, Exception, "$NUM_PES not set!")

        self.logger.info("Found PBSPro $PBS_NODEFILE %s." % pbspro_nodefile)

        # Dont need to parse the content of nodefile for PBSPRO, only the length
        # is interesting, as there are only duplicate entries in it.
        pbspro_nodes_length = len([line.strip() for line in open(pbspro_nodefile)])
        pbspro_num_ppn      = int(pbspro_num_ppn)
        pbspro_node_count   = int(pbspro_node_count)
        pbspro_num_pes      = int(pbspro_num_pes)
        pbspro_vnodes       = self._parse_pbspro_vnodes()

        # Verify that $NUM_PES == $NODE_COUNT * $NUM_PPN == len($PBS_NODEFILE)
        if  not (pbspro_node_count * pbspro_num_ppn \
                == pbspro_num_pes \
                == pbspro_nodes_length ) :
            self.logger.warning("NUM_PES != NODE_COUNT * NUM_PPN != len($PBS_NODEFILE)")

        self.node_list      = pbspro_vnodes
        self.cores_per_node = pbspro_num_ppn


    # --------------------------------------------------------------------------
    def _parse_pbspro_vnodes(self):

        # PBS Job ID
        pbspro_jobid = os.environ.get('PBS_JOBID')
        if  pbspro_jobid is None :
            msg = "$PBS_JOBID not set!"
            log_raise (self.logger, Exception, msg)

        # Get the output of qstat -f for this job
        output = subprocess.check_output(["qstat", "-f", pbspro_jobid])

        # Get the (multiline) 'exec_vnode' entry
        vnodes_str = ''
        for line in output.splitlines():
            # Detect start of entry
            if  'exec_vnode = ' in line:
                vnodes_str += line.strip()
            elif vnodes_str:
                # Find continuing lines
                if  " = " not in line:
                    vnodes_str += line.strip()
                else:
                    break

        # Get the RHS of the entry
        elems = vnodes_str.split('=',1)[1].strip()
        self.logger.debug("input: %s" % elems)

        nodes_list = list()

        # Break up the individual node partitions into vnode slices
        while True:

            idx   = elems.find(')+(')
            nodes_list.append(elems[1:idx])
            elems = elems[idx+2:]

            if  idx < 0:
                break

        vnodes_list = list()
        cpus_list   = list()

        # Split out the slices into vnode name and cpu count
        for node_str in nodes_list:
            slices = node_str.split('+')
            for _slice in slices:
                vnode, cpus = _slice.split(':')
                cpus = int(cpus.split('=')[1])
                vnodes_list.append(vnode)
                cpus_list.append(cpus)
                self.logger.debug('vnode: %s cpus: %s' % (vnode, cpus))

        self.logger.debug("vnodes: %s" % vnodes_list)
        self.logger.debug("cpus: %s" % cpus_list)

        cpus_list = list(set(cpus_list))
        min_cpus = int(min(cpus_list))

        if  len(cpus_list) > 1:
            self.logger.debug ("Detected vnodes of different sizes: %s, " \
                               "the minimal is: %d." % (cpus_list, min_cpus))

        node_list = list()
        for vnode in vnodes_list:
            # strip the last _0 of the vnodes to get the node name
            node_list.append(vnode.rsplit('_', 1)[0])

        # only unique node names
        node_list = list(set(node_list))
        self.logger.debug("Node list: %s" % node_list)

        # Return the list of node names
        return node_list


# ==============================================================================
# 
class LRMS_SLURM (LRMS_Base) :

    # --------------------------------------------------------------------------
    def __init__ (self, requested_cores, logger) :

        LRMS_Base.__init__ (self, requested_cores, logger)


    # --------------------------------------------------------------------------
    def configure (self) :

        # Parse SLURM nodefile environment variable
        slurm_nodelist = os.environ.get('SLURM_NODELIST', None)
        if  slurm_nodelist is None:
            log_raise (self.logger, Exception, "$SLURM_NODELIST not set!")

        # $SLURM_NPROCS = Total number of processes in the current job
        slurm_nprocs_str = os.environ.get('SLURM_NPROCS', None)
        if  slurm_nprocs_str is None:
            log_raise (self.logger, Exception, "$SLURM_NPROCS not set!")

        # $SLURM_NNODES = Total number of nodes in the job's resource allocation
        slurm_nnodes_str = os.environ.get('SLURM_NNODES', None)
        if  slurm_nnodes_str is None:
            log_raise (self.logger, Exception, "$SLURM_NNODES not set!")

        # $SLURM_CPUS_ON_NODE = Count of processors available on this node.
        slurm_cpus_on_node_str = os.environ.get('SLURM_CPUS_ON_NODE', None)
        if  slurm_cpus_on_node_str is None:
            log_raise (self.logger, Exception, "$SLURM_NNODES not set!")

        slurm_nodes        = hostlist.expand_hostlist(slurm_nodelist)
        slurm_nprocs       = int(slurm_nprocs_str)
        slurm_nnodes       = int(slurm_nnodes_str)
        slurm_cpus_on_node = int(slurm_cpus_on_node_str)

        self.logger.info ("Found SLURM_NODELIST %s. Expanded to: %s" \
                       % (slurm_nodelist, slurm_nodes))

        # Verify that $SLURM_NPROCS == $SLURM_NNODES * $SLURM_CPUS_ON_NODE
        if  slurm_nnodes * slurm_cpus_on_node != slurm_nprocs:
            self.logger.error ("$SLURM_NPROCS(%d) != $SLURM_NNODES(%d) * $SLURM_CPUS_ON_NODE(%d)" % \
                              (slurm_nnodes, slurm_cpus_on_node, slurm_nprocs))

        # Verify that $SLURM_NNODES == len($SLURM_NODELIST)
        if  slurm_nnodes != len(slurm_nodes):
            self.logger.error ("$SLURM_NNODES(%d) != len($SLURM_NODELIST)(%d)" % \
                              (slurm_nnodes, len(slurm_nodes)))

        self.node_list      = slurm_nodes
        self.cores_per_node = slurm_cpus_on_node


# ==============================================================================
# 
class LRMS_SGE (LRMS_Base) :

    # --------------------------------------------------------------------------
    def __init__ (self, requested_cores, logger) :

        LRMS_Base.__init__ (self, requested_cores, logger)


    # --------------------------------------------------------------------------
    def configure (self) :

        sge_hostfile = os.environ.get('PE_HOSTFILE', None)
        if  sge_hostfile is None:
            log_raise (self.logger, Exception, "$PE_HOSTFILE not set!")

        # SGE core configuration might be different than what multiprocessing
        # announces 
        # Alternative: "qconf -sq all.q|awk '/^slots *[0-9]+$/{print $2}'"

        # Parse SGE hostfile for nodes, keep only unique nodes
        sge_node_list = [line.split()[0] for line in open(sge_hostfile)]
        sge_nodes     = list(set(sge_node_list))
        self.logger.info ("Found PE_HOSTFILE %s. Expanded to: %s" \
                       % (sge_hostfile, sge_nodes))

        # Parse SGE hostfile for cores
        sge_cores_count_list = [int(line.split()[1]) for line in open(sge_hostfile)]
        sge_core_counts      = list(set(sge_cores_count_list))
        sge_cores_per_node   = min(sge_core_counts)

        self.logger.info ("Found unique core counts: %s Using: %d" \
                       % (sge_core_counts, sge_cores_per_node))

        self.node_list       = sge_nodes
        self.cores_per_node  = sge_cores_per_node


# ==============================================================================
# 
class LRMS_LSF (LRMS_Base) :

    # --------------------------------------------------------------------------
    def __init__ (self, requested_cores, logger) :

        LRMS_Base.__init__ (self, requested_cores, logger)


    # --------------------------------------------------------------------------
    def configure (self) :

        lsf_hostfile = os.environ.get('LSB_DJOB_HOSTFILE', None)
        if  lsf_hostfile is None:
            log_raise (self.logger, Exception, "$LSB_DJOB_HOSTFILE not set!")

        lsb_mcpu_hosts = os.environ.get('LSB_MCPU_HOSTS', None)
        if  lsb_mcpu_hosts is None:
            log_raise (self.logger, Exception, "$LSB_MCPU_HOSTS not set!")

        # parse LSF hostfile
        # format:
        # <hostnameX>
        # <hostnameX>
        # <hostnameY>
        # <hostnameY>
        #
        # There are in total "-n" entries (number of tasks) and "-R" entries 
        # per host (tasks per host).  (That results in "-n" / "-R" unique hosts)
        #
        lsf_nodes     = [line.strip() for line in open(lsf_hostfile)]
        lsf_node_list = list(set(lsf_nodes))

        # Grab the core (slot) count from the environment
        # Format: hostX N hostY N hostZ N
        lsf_cores_count_list = map(int, lsb_mcpu_hosts.split()[1::2])
        lsf_core_counts      = list(set(lsf_cores_count_list))
        lsf_cores_per_node   = min(lsf_core_counts)

        self.logger.info ("Found LSB_DJOB_HOSTFILE %s. Expanded to: %s" \
                       % (lsf_hostfile, lsf_nodes))
        self.logger.info ("Found unique core counts: %s Using: %d" \
                       % (lsf_core_counts, lsf_cores_per_node))

        self.node_list       = lsf_node_list
        self.cores_per_node  = lsf_cores_per_node


# ==============================================================================
# 
class LRMS_LOADL (LRMS_Base) :

    # --------------------------------------------------------------------------
    def __init__ (self, requested_cores, logger) :

        LRMS_Base.__init__ (self, requested_cores, logger)


    # --------------------------------------------------------------------------
    def configure (self) :

        #LOADL_HOSTFILE
        loadl_hostfile = os.environ.get('LOADL_HOSTFILE', None)
        if  loadl_hostfile is None:
            log_raise (self.logger, Exception, "$LOADL_HOSTFILE not set!")

        #LOADL_TOTAL_TASKS
        loadl_total_tasks_str = os.environ.get('LOADL_TOTAL_TASKS', None)
        if  loadl_total_tasks_str is None:
            log_raise (self.logger, Exception, "$LOADL_TOTAL_TASKS not set!")

        loadl_total_tasks   = int(loadl_total_tasks_str)
        loadl_nodes         = [line.strip() for line in open(loadl_hostfile)]
        loadl_node_list     = list(set(loadl_nodes))
        # Assume: 
        # cores_per_node    = lenght(nodefile) / len(unique_nodes_in_nodefile)
        loadl_cpus_per_node = len(loadl_nodes) / len(loadl_node_list)

        self.logger.info ("Found LOADL_HOSTFILE %s. Expanded to: %s" \
                       % (loadl_hostfile, loadl_nodes))

        # Verify that $LLOAD_TOTAL_TASKS == len($LOADL_HOSTFILE)
        if  loadl_total_tasks != len(loadl_nodes):
            self.logger.error ("$LLOAD_TOTAL_TASKS(%d) != len($LOADL_HOSTFILE)(%d)" % \
                              (loadl_total_tasks, len(loadl_nodes)))

        self.node_list      = loadl_node_list
        self.cores_per_node = loadl_cpus_per_node


# ==============================================================================
#
# Execution Environment
#
# ==============================================================================
#
class ExecutionEnvironment(object):

    """DOC """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, logger, lrms, requested_cores, 
                 task_launch_method, mpi_launch_method):

        self.logger = logger

        # Configure nodes and number of cores available, and 
        # task spawning mechanism
        self.lrms               = LRMS         (lrms, requested_cores, logger)
        self.task_launch_method = LaunchMethod (task_launch_method,    logger)
        self.mpi_launch_method  = LaunchMethod (mpi_launch_method,     logger)

        logger.info ("Discovered execution environment: %s" \
                  % (self.lrms.node_list))
        logger.info ("Discovered task launch method: '%s' and MPI launch method: '%s'." \
                  % (task_launch_method, mpi_launch_method))

        # For now assume that all nodes have equal amount of cores
        cores_avail = len(self.lrms.node_list) * self.lrms.cores_per_node
        if  cores_avail < int(requested_cores):
            log_raise (self.logger, RuntimeError, 
                                   "Not enough cores available (%s) to " \
                                   "satisfy allocation request (%s).", \
                                   (cores_avail, requested_cores))
            

# ==============================================================================
#
# Task
#
# ==============================================================================
#
class Task (object):

    # --------------------------------------------------------------------------
    def __init__(self, uid, description):

        self.logger       = None
        self._description = None

        # static task properties
        self.uid            = uid

        self.executable     = description["executable"]
        self.arguments      = description["arguments"]
        self.environment    = description["environment"]
        self.cores          = description["cores"]
        self.mpi            = description["mpi"]
        self.pre_exec       = description["pre_exec"]
        self.post_exec      = description["post_exec"]
        self.workdir        = description["workdir"]
        self.stdout         = description["stdout"]
        self.stderr         = description["stderr"]
        self.output_data    = description["output_data"]



        # dynamic task properties
        self.slots          = None

        self.started        = None
        self.finished       = None

        self.state          = None
        self.exit_code      = None

        self.stdout_id      = None
        self.stderr_id      = None

        self._log           = list()
        self._pid           = None


# ==============================================================================
#
# Execution Worker
#
# ==============================================================================
#
class ExecWorker (object):
    """ 
    An ExecWorker competes for the execution of tasks in a task queue.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, logger, command_queue, exec_env,
                  mongodb_url, database_name, pilot_id, session_id):

        self.daemon              = True
        self.logger              = logger

        self._terminate          = False
        self._pilot_id           = pilot_id
        
        self._command_queue      = command_queue # queued commands by the agent
        
        self._running_tasks      = dict() # Launched tasks by this ExecWorker

        self._exec_env           = exec_env
        self._lrms               = self._exec_env.lrms
        self._slots              = self._lrms.slot_list
        self._node_list          = self._lrms.node_list,
        self._cores_per_node     = self._lrms.cores_per_node,
        self._mpi_launch_method  = self._exec_env.mpi_launch_method
        self._task_launch_method = self._exec_env.task_launch_method

        mongo_client             = pymongo.MongoClient (mongodb_url)
        self._mongo_db           = mongo_client[database_name]
        self._p                  = self._mongo_db["%s.p" % session_id]
        self._w                  = self._mongo_db["%s.w" % session_id]

        # get some threads going -- those will do all the work.
        self._launcher_shell     = sups.PTYShell ("fork://localhost/", 
                                                  logger=self.logger)
        self._monitor_shell      = sups.PTYShell ("fork://localhost/", 
                                                  logger=self.logger)

        # queues toward the updater
        self._updater_queue      = multiprocessing.Queue ()

        self._launcher           = threading.Thread (target=self.launcher)
        self._monitor            = threading.Thread (target=self.monitor )
        self._updater            = threading.Thread (target=self.updater )

        self._launcher.start ()
        self._monitor .start ()
        self._updater .start ()

        # keep a slot allocation history (short status), start with presumably
        # empty state now
        self._slot_history    = list()
        self._slot_history.append (self._slot_status ())

        # publish pilot state.  That state is frequently updated to allow
        # higher level load balancing.
      # self._capability      = self._slots2caps (self._lrms)
        self._capability      = self._slots2free (self._lrms)

        self._p.update ({"_id" : ObjectId(self._pilot_id)},
                        {"$set": {"slothistory" : self._slot_history,
                                  "capability"  : self._capability,
                                  "slots"       : self._slots}})

    # --------------------------------------------------------------------------
    #
    def stop (self) :

        self._terminate = True

        self.logger.info ("terminating exec worker")

        self.launcher.join ()
        self.monitor.join  ()
        self.updater.join  ()

        # we are done -- push slot history 
        self._p.update(
            {"_id": ObjectId(self._pilot_id)},
            {"$set": {"slothistory" : self._slot_history, 
                      "capability"  : 0,
                      "slots"       : self._slots}}
            )

        self.logger.info ("terminated  exec worker")


    # --------------------------------------------------------------------------
    #
    def launcher (self) :
        """Starts the process when Process.start() is called.
        """
        try:

            while self._terminate is False:

                try:
                    command, arg = self._command_queue.get (block=True, timeout=1)

                    if  command == COMMAND_LAUNCH_COMPUTE_UNIT :

                        task = arg
                        ret  = self._task_launch (task)

                        if  ret == OK :

                            # task spawned ok -- schedule for state updates
                            task.state = EXECUTING
                            self._running_tasks[task.pid] = task
                            self._updater_queue.put ([task, timestamp()])

                        elif ret == FAIL :

                            # no game -- schedule for state updates
                            task.state = FAILED
                            self._updater_queue.put ([task, timestamp()])

                        elif ret == RETRY :

                            # No resources free, put back in queue
                            # FIXME: avoid busy-spin on one non-suitable task!
                            self._command_queue.put ([COMMAND_LAUNCH_COMPUTE_UNIT, task])


                    elif command == COMMAND_CANCEL_COMPUTE_UNIT :

                        cuid = arg
                        task = self._running_tasks.get (cuid, None)
                        ret  = self._task_cancel (task)

                        # FIXME: eval ret

                        if  ret == OK :
                            self._updater_queue.put ([task, timestamp])

                    else:
                        self.logger.error ("Command %s not applicable.", \
                                          command[COMMAND_TYPE])
                        continue


                except Queue.Empty:

                    # timed out -- i.e. an opportunity for checking self._terminate
                    pass


        except Exception :
            
            self._terminate = True
            pilot_FAILED (self._p, self._pilot_id, self.logger, 
                         "Error in ExecWorker loop: %s" \
                         % traceback.format_exc())


    # --------------------------------------------------------------------------
    #
    def monitor (self) :

        MONITOR_READ_TIMEOUT = 1.0   # check for stop signal now and then

        ret, out, _ = self.monitor.run_sync (" /bin/sh %s/wrapper.sh $$" \
                                             % self._workdir)

        if  ret != 0 :
            log_raise (self.logger, RuntimeError, 
                       "failed to run bootstrap: (%s)(%s)", (ret, out))

        # shell_wrapper.sh will report its own PID.
        self.logger.debug ("monitor startup: %s" % out)

        try:

            self.monitor.run_async ("MONITOR")

            while self.monitor.alive () :

                _, out = self.monitor.find (['\n'], 
                                            timeout=MONITOR_READ_TIMEOUT)
                line   = out.strip ()

                if  not line :

                    # just a read timeout, i.e. an opportiunity to check for
                    if  self._terminate :
                        self.logger.debug ("stop monitor")
                        return


                elif line == 'EXIT' or line == "Killed" :
                    self.logger.error ("monitor failed - disable notifications")
                    return


                elif not ':' in line :
                    self.logger.warn ("monitor noise: %s" % line)


                else :

                    pid, state, rc = line.split (':', 2)
                    state          = string_to_state (state)
                    task           = self._running_tasks.get (pid, None)

                    if  not task :
                        self.logger.warn ("event for unknown pid %s" % pid)
                        continue

                    # before doing anything else, let the launcher know that
                    # there are new free slots...
                    self._change_slot_states (task.slots, FREE)

                    # we don't get any notifications which leaves the task
                    # running -- so assume its dead now...
                    del self._running_tasks[task.uid]

                    # update return code if available (not be on cancel etc)
                    if  rc :
                        task.exit_code = int (rc)

                   
                    # failed, canceled or unknown jobs need no post processing
                    # but just a status update 
                    if  state is not DONE :

                        task.state = state

                    # task is done -- upload stdout/stderr if needed
                    else :

                        task.state = DONE
                        if  task.output_data :
                            state = PENDING_OUTPUT_TRANSFER
                        else:
                            state = DONE

                        # upload stdout and stderr via GridFS
                        workdir   = task.workdir
                        stdout_id = None
                        stderr_id = None

                        # FIXME: use actual stdout/stdin from task if set
                        stdout = "%s/STDOUT" % workdir
                        stderr = "%s/STDERR" % workdir

                        if  op.isfile (stdout) :
                            fs = gridfs.GridFS (self._mongo_db)
                            with open (stdout, 'r') as f :
                                stdout_id = fs.put (f.read(), filename=stdout)
                                self.logger.info ("Uploaded %s to MongoDB as %s." \
                                               % (stdout, str(stdout_id)))

                        if op.isfile (stderr) :
                            fs = gridfs.GridFS (self._mongo_db)
                            with open (stderr, 'r') as f :
                                stderr_id = fs.put (f.read(), filename=stderr)
                                self.logger.info ("Uploaded %s to MongoDB as %s." \
                                               % (stderr, str(stderr_id)))

                        task.stdout_id = stdout_id
                        task.stderr_id = stderr_id


                    # DONE or NOT DONE: tell it to the mountain
                    self._updater_queue.put ([task, timestamp()])


        except Exception :

            self.logger.error ("error in monitor thread: %s" \
                               % traceback.format_exc())


    # --------------------------------------------------------------------------
    #
    def _slots2free(self, lrms):
        """Convert slots structure into a free core count """

        free_cores = 0
        for node in lrms.slot_list :
            free_cores += node['cores'].count (FREE)

        return free_cores


    # --------------------------------------------------------------------------
    #
    def _slots2caps (self, lrms) :
        """Convert slots structure into a capability structure """

        all_caps_tuples = dict()
        for node in lrms.slot_list :
            free_cores = node['cores'].count(FREE)

            # (Free_cores, Continuous, Single_Node) = Count
            # FIXME: the var below seems ununsed?
            caps_tuple = (free_cores, False, True)

            # FIXME: the all_caps_tuples are empty?
            if  caps_tuple in all_caps_tuples:
                all_caps_tuples[caps_tuple] += 1
            else:
                all_caps_tuples[caps_tuple]  = 1


        # Convert to please the gods of json and mongodb
        all_caps_dict = list()
        for caps_tuple in all_caps_tuples:
            free_cores, cont, single = caps_tuple
            count     = all_caps_tuples[caps_tuple]
            caps_dict = {'free_cores'  : free_cores,
                         'continuous'  : cont, 
                         'single_node' : single, 
                         'count'       : count}
            all_caps_dict.append(caps_dict)

        return all_caps_dict


    # --------------------------------------------------------------------------
    #
    def _slot_status(self):
        """Returns a multi-line string corresponding to slot status.
        """

        slot_matrix = ""
        for slot in self._slots:
            slot_matrix += "|"
            for core in slot['cores']:
                if  core is FREE:
                    slot_matrix += "-"
                else:
                    slot_matrix += "+"
        slot_matrix += "|"

        self.logger.debug ("slot status:\n%s", slot_matrix)
        return {'timestamp' : timestamp(), 
                'slotstate' : slot_matrix}


    # --------------------------------------------------------------------------
    #
    # Returns a data structure in the form of:
    #
    def _acquire_slots(self, cores_requested, single_node, continuous):

        # ----------------------------------------------------------------------
        # Find a needle (continuous sub-list) in a haystack (list)
        def find_sublist(haystack, needle):
            n = len(needle)
            # Find all matches (returns list of False/True for every position)
            hits = [(needle == haystack[i:i+n]) for i in xrange(len(haystack)-n+1)]

            try:
                # Grab the first occurrence
                index = hits.index(True)
            except ValueError:
                index = None

            return index

        # ----------------------------------------------------------------------
        # Transform the number of cores into a continuous list of "status"es,
        # and use that to find a sub-list.
        def find_cores_cont(slot_cores, cores_requested, status):
            return find_sublist(slot_cores, [status for _ in range(cores_requested)])

        # ----------------------------------------------------------------------
        # Find an available continuous slot within node boundaries.
        def find_slots_single_cont(cores_requested):

            for slot in self._slots:

                slot_node         = slot['node']
                slot_cores        = slot['cores']
                slot_cores_offset = find_cores_cont(slot_cores, cores_requested, FREE)

                if slot_cores_offset is not None:
                    self.logger.info ('Node %s satisfies %d cores at offset %d' \
                                   % (slot_node, cores_requested, slot_cores_offset))
                    return ['%s:%d' % (slot_node, core) \
                            for core in range(slot_cores_offset, 
                                              slot_cores_offset + cores_requested)]

            return None

        # ----------------------------------------------------------------------
        # Find an available continuous slot across node boundaries.
        def find_slots_multi_cont(cores_requested):

            # Convenience aliases
            cores_per_node = self._cores_per_node
            all_slots      = self._slots

            # Glue all slot core lists together
            all_slot_cores = [core for node in [node['cores'] for node in all_slots] \
                              for core in node]
          # self.logger.debug("all_slot_cores: %s" % all_slot_cores)

            # Find the start of the first available region.  Determine the first
            # slot in the slot list and the core offset within that node
            all_slots_first_core_offset = find_cores_cont(all_slot_cores, cores_requested, FREE)
            first_slot_index            = all_slots_first_core_offset / cores_per_node
            first_slot_core_offset      = all_slots_first_core_offset % cores_per_node

            self.logger.debug("all_slots_first_core_offset: %s" % all_slots_first_core_offset)
            self.logger.debug("first_slot_index: %s"            % first_slot_index)
            self.logger.debug("first_slot_core_offset: %s"      % first_slot_core_offset)

            if all_slots_first_core_offset is None:
                return None

            # Note: We subtract one here, because counting starts at zero;
            #       Imagine a zero offset and a count of 1, the only core used would be core 0.
            #       FIXME: Verify this claim :-)
            all_slots_last_core_offset = (first_slot_index       * cores_per_node) \
                                       +  first_slot_core_offset + cores_requested - 1
            last_slot_index            =  all_slots_last_core_offset / cores_per_node
            last_slot_core_offset      =  all_slots_last_core_offset % cores_per_node

            self.logger.debug("all_slots_last_core_offset: %s" % all_slots_last_core_offset)
            self.logger.debug("last_slot_index: %s"            % last_slot_index)
            self.logger.debug("last_slot_core_offset: %s"      % last_slot_core_offset)

            # Convenience aliases
            first_slot = self._slots[first_slot_index]
            first_node = first_slot['node']
            last_slot  = self._slots[last_slot_index]
            last_node  = last_slot['node']

            self.logger.debug("last_slot: %s"  % last_slot)
            self.logger.debug("last_node: %s"  % last_node)
            self.logger.debug("first_slot: %s" % first_slot)
            self.logger.debug("first_node: %s" % first_node)

            # Collect all node:core slots here
            task_slots = list()

            # Add cores from first slot for this task. As this is a multi-node
            # search, we can safely assume that we go from the offset all the
            # way to the last core
            task_slots.extend(['%s:%d' % (first_node, core) \
                               for core in range(first_slot_core_offset, cores_per_node)])

            # Add all cores from "middle" slots
            for slot_index in range(first_slot_index+1, last_slot_index):
                slot_node = all_slots[slot_index]['node']
                task_slots.extend (['%s:%d' % (slot_node, core) \
                                   for core in range(0, cores_per_node)])

            # Add the cores of the last slot
            task_slots.extend (['%s:%d' % (last_node, core) \
                               for core in range(0, last_slot_core_offset+1)])

            return task_slots
        #  End of inline functions, _acquire_slots() code begins after this
        ########################################################################

        #
        # Switch between searching for continuous or scattered slots
        #
        # Switch between searching for single or multi-node
        if  single_node:
            if  continuous:
                task_slots = find_slots_single_cont(cores_requested)
            else:
                log_raise (self.logger, NotImplementedError, 
                           'Scattered single node scheduler not implemented.')
        else:
            if  continuous:
                task_slots = find_slots_multi_cont(cores_requested)
            else:
                log_raise (self.logger, NotImplementedError, 
                           'Scattered multi node scheduler not implemented.')

        if  task_slots is not None:
            self._change_slot_states(task_slots, BUSY)

        return task_slots


    # --------------------------------------------------------------------------
    #
    # Change the reserved state of slots (FREE or BUSY)
    #
    def _change_slot_states(self, task_slots, new_state):

        # Convenience alias
        all_slots = self._slots

      # self.logger.debug("change_slot_states: task slots: %s" % task_slots)

        for slot in task_slots:

          # self.logger.debug("change_slot_states: slot content: %s" % slot)

            # Get the node and the core part
            [slot_node, slot_core] = slot.split(':')

            # Find the entry in the the all_slots list
            slot_entry = (slot for slot in all_slots \
                          if slot["node"] == slot_node).next()

            # Change the state of the slot
            slot_entry['cores'][int(slot_core)] = new_state

        # something changed - write history!
        #
        # mongodb entries MUST NOT grow larger than 16MB, or chaos will ensue.
        # We thus limit the slot history size to 4MB, to keep 'suffient space'
        # for the actual operational data
        if  len(str(self._slot_history)) < 4 * 1024 * 1024 :
            self._slot_history.append (self._slot_status ())
        else :
            # short in space: just replace the last entry ...
            self._slot_history[-1]  =  self._slot_status ()


    # --------------------------------------------------------------------------
    #
    def _task2cmd (self, task) :

        # ----------------------------------------------------------------------
        def quote_args (args) :

            ret = list()
            for arg in args :

                # if string is between outer single quotes,
                #    pass it as is.
                # if string is between outer double quotes,
                #    pass it as is.
                # otherwise (if string is not quoted)
                #    escape all double quotes

                if  arg[0] == arg[-1]  == "'" :
                    ret.append (arg)
                elif arg[0] == arg[-1] == '"' :
                    ret.append (arg)
                else :
                    arg = arg.replace ('"', '\\"')
                    ret.append ('"%s"' % arg)

            return  ret


        exe  = ""
        arg  = ""
        env  = ""
        cwd  = ""
        pre  = ""
        post = ""
        io   = ""
        cmd  = ""

        if  task.workdir :
            cwd = "mkdir -p %s && cd %s && " % (task.workdir, task.workdir)

        if  task.environment :
            for e in task.environment :
                env += "export %s=%s\n"  %  (e, task.environment[e])

        if  task.pre_exec :
            pre = '\n'.join (task.pre_exec)

        if  task.executable :
            exe = task.executable

        if  task.arguments :
            arg = " ".join (quote_args (task.arguments))

        if  task.stdin  :
            io += "<%s "  % task.stdin
        if  task.stdout :
            io += "1>%s " % task.stdout
        if  task.stderr :
            io += "2>%s " % task.stderr

        if  task.post_exec :
            post = '\n'.join (task.post_exec)

        if  task.mpi :
            cmd = self._exec_env.mpi_launch_method.command  (self._exec_env.lrms, task.slots, task.cores)
        else :                                                     
            cmd = self._exec_env.task_launch_method.command (self._exec_env.lrms, task.slots, task.cores)

        script  = "%s\n"            %  cwd
        script += "%s\n"            %  env
        script += "%s\n"            %  pre
        script += "(%s %s %s) %s\n" % (cmd, exe, arg, io)
        script += "%s\n"            %  post

        self.logger.debug ("execution script:\n%s\n" % script)

        return script


    # --------------------------------------------------------------------------
    #
    def _task_launch (self, task) :
        """ runs a task on the wrapper via pty, and returns the pid """

        if task.mpi: launch_method = self._mpi_launch_method
        else       : launch_method = self._task_launch_method

        if  not launch_method :
            self.logger.error ("no launch method for task %s" % task.uid)
            return FAIL

        # IBRUN (e.g. Stampede) needs continuous slots for multi core execution
        # FIXME: Dont have scattered scheduler yet, so test is disabled.
      # if  task.mpi and \
      #     self.mpi_launch_method.name in CONT_SLOTS_LAUNCH_METHODS :
        if  True :
            req_cont = True
        else:
            req_cont = False

        # First try to find all cores on a single node
        task.slots   = self._acquire_slots (task.cores, single_node=True, 
                                            continuous=req_cont)

        # on failure, see if our launch method supports multiple nodes
        if  task.slots is None and launch_method in MULTI_NODE_LAUNCH_METHODS:
            task.slots = self._acquire_slots (task.cores, single_node=False, 
                                              continuous=req_cont)

        # Check if we got results
        if  task.slots is None :
            return RETRY

        # we got an allocation: go off and launch the process
        script    = self._task2cmd (task)
        run_cmd   = "BULK\nLRUN\n%s\nLRUN_EOT\nBULK_RUN\n" % script

        if  self._lrms.target_is_macos :
            run_cmd = run_cmd.replace ("\\", "\\\\\\\\") # hello MacOS

        ret, out, _ = self._launcher_shell.run_sync (run_cmd)

        if  ret != 0 :
            self.logger.error ("failed to run task '%s': (%s)(%s)" \
                            % (run_cmd, ret, out))
            return FAIL

        lines = filter (None, out.split ("\n"))

      # self.logger.debug (lines)

        if  len (lines) < 2 :
            log_raise (self.logger, RuntimeError, 
                       "Failed to run task (%s)", lines)
        
        if  lines[-2] != "OK" :
            self.logger.error ("Failed to run task (%s)" % lines)
            return FAIL

        # FIXME: verify format of returned pid (\d+)!
        task.pid     = lines[-1].strip ()
        task.started = timestamp()

      # self.logger.debug ("started task %s" % pid)

        # before we return, we need to clean the 
        # 'BULK COMPLETED message from lrun
        ret, out = self._launcher_shell.find_prompt ()
        if  ret != 0 :
            self.logger.error ("failed to run task '%s': (%s)(%s)" \
                            % (run_cmd, ret, out))
            return FAIL

        return OK


    # --------------------------------------------------------------------------
    #
    def _task_cancel (self, task) :

        if  task == None :
            self.logger.error ("Cannot cancel task $s: not running" % task.uid)
            return None

        task.state = CANCELED

        # FIXME: check retval
        ret = self._launcher_shell.run_sync ("CANCEL %s" % task.pid)

        return OK
    


    # --------------------------------------------------------------------------
    #
    def updater (self) :

        """ 
        Updates database entries for tasks coming through the updater queue.  
        """

        while not self._terminate :

            try :
                task, ts = self._updater_queue.get (block=True, timeout=1)

                # AM: FIXME: this needs to become a bulk op
                self._w.update(
                    {"_id"   : ObjectId(task.uid)}, 
                    {"$set"  : {"state"        : task.state,
                                "slots"        : task.slots,
                                "exit_code"    : task.exit_code,
                                "stdout_id"    : task.stdout_id,
                                "stderr_id"    : task.stderr_id},
                     "$push" : {"statehistory" : {"state"     : task.state, 
                                                  "timestamp" : ts}}
                    })

                # Update capabilities on monitored, launched and failed tasks
              # self._capability = self._slots2caps(self._lrms)
                self._capability = self._slots2free(self._lrms)

                self._p.update(
                    {"_id" : ObjectId(self._pilot_id)},
                    {"$set": {"capability"  : self._capability}
                    })

            except Queue.Empty :

                # timed out -- i.e. an opportunity for checking self._terminate
                pass




# ==============================================================================
#
class Agent (object):

    # --------------------------------------------------------------------------
    #
    def __init__ (self, options, logger):

        self._options       = options
        self.logger         = logger
        self._pilot_id      = options.pilot_id
        self._session_id    = options.session_id
        self._runtime       = options.runtime
        self._mongodb_url   = options.mongodb_url
        self._database_name = options.database_name
        self._workdir       = options.workdir
        self._starttime     = time.time()

        # interpret some options
        if  self._workdir is '.' :
            self._workdir = os.getcwd ()

        # initialize mongodb connection
        mongo_client = pymongo.MongoClient (self._mongodb_url)
        mongo_db     = mongo_client[self._database_name]
        self._p      = mongo_db["%s.p" % self._session_id]
        self._w      = mongo_db["%s.w" % self._session_id]

        # Discover environment, nodes, cores, mpi, etc.
        self._exec_env = ExecutionEnvironment (
            lrms               = self._options.lrms,
            requested_cores    = self._options.cores,
            task_launch_method = self._options.task_launch_method,
            mpi_launch_method  = self._options.mpi_launch_method,
            logger             = logger,
        )
        self._lrms   = self._exec_env.lrms

        # first order of business: set the start time and state of the pilot
        self.logger.info ("agent started. Database updated.")
        self._p.update(
            {"_id"  : ObjectId(self._pilot_id)}, 
            {"$set" : {"state"          : "Active",
                       "nodes"          : self._lrms.node_list,
                       "cores_per_node" : self._lrms.cores_per_node,
                       "capability"     : 0},
             "$push": {"statehistory"   : {"state"    : 'Active', 
                                           "timestamp": timestamp()}}
            })

        # create a queue to communicate with the exec workers.  The workers will
        # compete for command execution.
        # FIXME: cancel commands can only be executed by workers which created
        # the task -- competing for cancel will thus create problems when more
        # than one exec worker is active.
        self._command_queue = multiprocessing.Queue()

        # we assign each node partition to a task execution worker -- it will
        # automatically spawn some threads for task launching, monitoring and
        # updating
        # FIXME: what partitions :P
        self._exec_worker   = ExecWorker(
            logger          = self.logger,
            command_queue   = self._command_queue,
            exec_env        = self._exec_env,
            mongodb_url     = self._mongodb_url,
            database_name   = self._database_name,
            pilot_id        = self._pilot_id,
            session_id      = self._session_id,
        )

        self.logger.info ("Started %s serving nodes %s" \
                       % (self._exec_worker, self._lrms.node_list))


    # --------------------------------------------------------------------------
    #
    def work (self) :

        while True:

            try:

                # do the work...
                busy  = self.check_state    ()
                busy += self.check_commands ()
                busy += self.check_tasks    ()

                # if nothing interesting happened, zzzz a bit
                if  not busy :
                    time.sleep (1)

            except Exception as ex :
                # If we arrive here, there was an exception in the main loop.
                pilot_FAILED (self._p, self._pilot_id, self.logger, 
                              "ERROR in agent main loop: %s. %s" \
                              % (str(ex), traceback.format_exc()))


    # --------------------------------------------------------------------------
    def check_state (self) :

        # Make sure that we haven't exceeded the agent runtime. if 
        # we have, terminate. 
        if  time.time() >= self._starttime + (int(self._runtime) * 60):
            self.logger.info ("agent has reached runtime limit of %s seconds." \
                           % str(int(self._runtime)*60))
            pilot_DONE (self._p, self._pilot_id, self.logger)

        return 0


    # --------------------------------------------------------------------------
    def check_commands (self) :

        # Check if there's a command waiting
        retdoc = self._p.find_and_modify (
                    query  = {"_id"  :  ObjectId (self._pilot_id)},
                    update = {"$set" : {COMMAND_FIELD: []}},
                    fields = [COMMAND_FIELD]
        )

        commands = list()
        if  retdoc:
            commands = retdoc['commands']
            self.logger.info ("commands: %s" % commands)


        for command in commands:

            if command[COMMAND_TYPE] == COMMAND_CANCEL_PILOT:
                self.logger.info ("Received Cancel Pilot command.")
                self._exec_worker.stop ()
                pilot_CANCELED (self._p, self._pilot_id, self.logger, 
                                "CANCEL received. Terminating.")

            elif command[COMMAND_TYPE] == COMMAND_CANCEL_COMPUTE_UNIT:
                # Put it on the command queue of the worker
                self._command_queue.put(command)
                self.logger.info ("Received Cancel Compute Unit (%s)" \
                               % command[COMMAND_ARG])

            elif command[COMMAND_TYPE] == COMMAND_KEEP_ALIVE:
                self.logger.info ("Received KeepAlive command.")

            else:
                log_raise (self.logger, Exception, 
                           "Received unknown command: %s with arg: %s.", \
                           (command[COMMAND_TYPE], command[COMMAND_ARG]))

        return len(commands)


    # --------------------------------------------------------------------------
    def check_tasks (self) :

        # Try to get new tasks from the database. for this, we check the
        # wu_queue of the pilot. if there are new entries, we get them, get the
        # actual pilot entries for them and remove them from the wu_queue.

        # Check if there are work units waiting for execution,
        # and log that we pulled it.
        wu_cursor  =  self._w.find_and_modify (
            query  = {"pilot" : self._pilot_id,
                      "state" : "PendingExecution"},
            update = {"$set" : {"state"       : "Scheduling"},
                      "$push": {"statehistory": {"state":     "Scheduling", 
                                                 "timestamp": timestamp()}}}
        )

        # There are new work units in the wu_queue on the database.
        # Get the corresponding wu entries.
        if wu_cursor is not None:

            if not isinstance(wu_cursor, list):
                wu_cursor = [wu_cursor]

                self.logger.info ("Found %d tasks in pilot queue" \
                               % len(wu_cursor))

            for wu in wu_cursor:
                # Create new task objects and put them into the task queue
                w_uid = str(wu["_id"])
                self.logger.info("Found new task in pilot queue: %s" % w_uid)

                task_dir_name = "%s/unit-%s" % (self._workdir, str(wu["_id"]))

                if  not 'workdir' in wu : wu['workdir'] = task_dir_name,
                if  not 'stdout'  in wu : wu['workdir'] = task_dir_name+'/STDOUT',
                if  not 'stderr'  in wu : wu['workdir'] = task_dir_name+'/STDERR',

                task       = Task(uid = w_uid, description = wu)
                task.state = 'Scheduling'

                self._command_queue.put ([COMMAND_LAUNCH_COMPUTE_UNIT, task])

            return len(wu_cursor)

        return 0


# ==============================================================================
#
# main
#
# ==============================================================================
# 
def main () :

    # parse command line options
    options   = parse_commandline()

    # configure the agent logger

    logger    = logging.getLogger   ('radical.pilot.agent')
    handler   = logging.FileHandler ("AGENT.LOG")
    formatter = logging.Formatter   ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    handler.setFormatter (formatter)
    logger.addHandler    (handler)
    logger.setLevel      (options.debug_level)

    logger.info ("RADICAL-Pilot multi-core agent for package/API version %s" \
              % options.package_version)

    #---------------------------------------------------------------------------
    # Establish database for signal handlers
    try:
        mongo_client = pymongo.MongoClient(options.mongodb_url)
        mongo_db     = mongo_client[options.database_name]
        mongo_p      = mongo_db["%s.p"  % options.session_id]

    except Exception, ex:
        logger.error ("Couldn't establish database connection: %s", str(ex))
        sys.exit (1)

    #---------------------------------------------------------------------------
    # Some signal handling magic
    def sigint_handler (_signal, frame):
        pilot_FAILED (mongo_p, options.pilot_id, logger, 
                      'Caught SIGINT. EXITING.')
    signal.signal (signal.SIGINT, sigint_handler)

    def sigalarm_handler (_signal, frame):
        pilot_FAILED (mongo_p, options.pilot_id, logger, 
                      'Caught SIGALRM (Walltime limit reached?). EXITING')
    signal.signal(signal.SIGALRM, sigalarm_handler)


    #---------------------------------------------------------------------------
    # Launch the agent
    try:
        agent = Agent (options = options, 
                       logger  = logger)
        agent.work ()

    except Exception as e :
        pilot_FAILED (mongo_p, options.pilot_id, logger, 
                      "Error running agent: %s" % e)

    except SystemExit:
        pilot_FAILED (mongo_p, options.pilot_id, logger, 
                      "Caught keyboard interrupt. EXITING")

# ==============================================================================
#
if __name__ == "__main__":

    main ()


# ==============================================================================
#

