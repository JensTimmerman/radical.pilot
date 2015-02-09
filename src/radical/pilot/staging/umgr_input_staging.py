
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import threading
import multiprocessing

import saga
import saga.filesystem as sfs

from   radical.pilot.states             import *
from   radical.pilot.updater            import UnitUpdater
from   radical.pilot.staging_directives import CREATE_PARENTS

# ------------------------------------------------------------------------------
#
# Scheduler setup
#
UMGR_STAGING_INPUT_THREADS   = 'threading'
UMGR_STAGING_INPUT_PROCESSES = 'multiprocessing'

UMGR_STAGING_INPUT_MODE      = UMGR_STAGING_INPUT_THREADS

if UMGR_STAGING_INPUT_MODE == UMGR_STAGING_INPUT_THREADS:
    COMPONENT_MODE = threading
    COMPONENT_TYPE = threading.Thread
    QUEUE_TYPE     = multiprocessing.Queue
elif UMGR_STAGING_INPUT_MODE == UMGR_STAGING_INPUT_PROCESSES:
    COMPONENT_MODE = multiprocessing
    COMPONENT_TYPE = multiprocessing.Process
    QUEUE_TYPE     = multiprocessing.Queue


# ------------------------------------------------------------------------------
#
# config flags 
#
# FIXME: move to some RP config
#
UMGR_STAGING_INPUT = 'UMGR_Staging_Input'
rp_config          = dict()


# ==============================================================================
#
# UMGR Staging Input
#
# ==============================================================================
#
class UMGR_Staging_Input(COMPONENT_TYPE):

    """
    The UMGR_Staging_Input performs all unit manager controlled input staging.
    It can only perform input staging for comput units which have been scheduled
    to a pilot -- otherwise no staging target would be known.  Later iterations
    of RP, for example where compute follows data, may place this component
    elsewhere in the pipeline, ie. before the UMGR Scheduler...
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, config, logger, session, umgr_staging_input_queue, 
                 update_queue):

        # At some point in the future, the stager will get an
        # agent_staging_input_queue and an agent_scheduling_queue -- but for
        # now, those components are fed from the agent class which pulls from the
        # DB, which is fed by the updater -- so pushing to the updater is
        # enough...

        COMPONENT_TYPE.__init__(self)

        self.name                       = name
        self._config                    = config
        self._log                       = logger
        self._session                   = session
        self._umgr_staging_input_queue  = umgr_staging_input_queue
      # self._agent_staging_input_queue = None
      # self._agent_scheduling_queue    = None
        self._update_queue              = update_queue

        self._saga_dirs                 = dict()
        self._terminate                 = COMPONENT_MODE.Event()


    # --------------------------------------------------------------------------
    #
    def stop(self):
        self._terminate.set()


    # --------------------------------------------------------------------------
    #
    def run(self):

        self._log.info("started %s.", self)

        while not self._terminate.is_set():

            try:

                cu = self._umgr_staging_input_queue.get()

                if not cu:
                    continue

                UnitUpdater.update_unit(queue = self._update_queue, 
                                        cu    = cu,
                                        state = UMGR_STAGING_INPUT)

              # cu_list = rpu.blowup(cu, UMGR_STAGING_INPUT) 

                try:
                    self._handle_unit(cu)

                    if cu['Agent_Input_Directives']:
                        # agent staging is needed
                        UnitUpdater.update_unit(queue = self._update_queue, 
                                                cu    = cu,
                                                state = AGENT_STAGING_INPUT_PENDING)
                    else:
                        # no agent input staging needed -- schedule for execution
                        UnitUpdater.update_unit(queue = self._update_queue, 
                                                cu    = cu,
                                                state = AGENT_SCHEDULING_PENDING)

                except Exception as e:

                    # If we catch an exception, assume the staging failed
                    msg = "umgr input staging failed for unit %s: %s" % (cu['_id'], e)
                    self._log.exception(msg)

                    # If a staging directive fails, fail the CU also.
                    UnitUpdater.update_unit(queue = self._update_queue, 
                                            cu    = cu,
                                            state = FAILED,
                                            msg   = msg)

            except Exception as e:
                self._log.exception('%s died', self.name)
                sys.exit(1)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, cu):

        uid        = cu["_id"]
        workdir    = cu["sandbox"]
        directives = cu["UMGR_Staging_Input_Directives"]

        # We need to create the CU's directory in case it doesn't exist yet.
        self._log.info("Creating ComputeUnit workdir %s." % workdir)

        # the normal case is that we need to create many unit dirs on one
        # target resources.  We thus cache the dir handle for each target
        # resource, which saves a significant number of roundtrips.  The
        # SAGA level connection pool should ensure that the number of open
        # connections stays within bounds(famous last words...)
        saga_dir = None

        try:
            url      = saga.Url(workdir)
            url.path = '/'
            key      = str(url)

            if key not in self._saga_dirs:
                self._saga_dirs[key] = sfs.Directory(url, flags=sfs.CREATE_PARENTS, 
                                                     session=self._session)

            saga_dir = self._saga_dirs[key]
            saga_dir.make_dir(workdir, flags=sfs.CREATE_PARENTS)

        except:
            raise

        finally:
            if saga_dir:
                saga_dir.close()


        # Loop over all transfer directives and execute them.
        self._log.info("Processing input file transfers for ComputeUnit %s" % uid)

        for d in directives:

            # FIXME: we should interrupt file transfer on unit cancellation --
            # at least we should check before running the first op, and should
            # check inbetween ops...

          # action = d['action']   # not interpreted?
            source = d['source']
            target = d['target']
            flags  = d['flags']

            abs_src = "file://localhost/%s" % os.path.abspath(source)

            if not target:
                abs_target = workdir
            else: 
                abs_target = "%s/%s" % (workdir, target)

            self._log.debug("Transferring input file %s -> %s", abs_src, abs_target)

            input_file = None

            try:
                input_file = sfs.File(abs_src, session=self._session)

                if CREATE_PARENTS in flags:
                    copy_flags = sfs.CREATE_PARENTS
                else:
                    copy_flags = 0

                input_file.copy(target, flags=copy_flags)

            except:
                raise

            finally:
                if input_file:
                    input_file.close()

# ------------------------------------------------------------------------------

