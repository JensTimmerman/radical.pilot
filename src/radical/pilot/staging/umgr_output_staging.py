
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import threading
import multiprocessing

import saga.filesystem as sfs

import radical.pilot   as rp
from   radical.pilot.update_worker      import UpdateWorker
from   radical.pilot.staging_directives import CREATE_PARENTS

# ------------------------------------------------------------------------------
#
# Scheduler setup
#
UMGR_STAGING_OUTPUT_THREADS   = 'threading'
UMGR_STAGING_OUTPUT_PROCESSES = 'multiprocessing'

UMGR_STAGING_OUTPUT_MODE      = UMGR_STAGING_OUTPUT_THREADS

if UMGR_STAGING_OUTPUT_MODE == UMGR_STAGING_OUTPUT_THREADS:
    COMPONENT_MODE = threading
    COMPONENT_TYPE = threading.Thread
    QUEUE_TYPE     = multiprocessing.Queue
elif UMGR_STAGING_OUTPUT_MODE == UMGR_STAGING_OUTPUT_PROCESSES:
    COMPONENT_MODE = multiprocessing
    COMPONENT_TYPE = multiprocessing.Process
    QUEUE_TYPE     = multiprocessing.Queue


# ------------------------------------------------------------------------------
#
# config flags 
#
# FIXME: move to some RP config
#
UMGR_STAGING_OUTPUT = 'UMGR_Staging_Output'
rp_config           = dict()


# ==============================================================================
#
# UMGR Staging Output
#
# ==============================================================================
#
class UMGR_Staging_Output(COMPONENT_TYPE):

    """
    The UMGR_Staging_Output performs all unit manager controlled output staging.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, config, logger, session, umgr_staging_output_queue, 
                 update_queue):

        COMPONENT_TYPE.__init__(self)

        self.name                       = name
        self._config                    = config
        self._log                       = logger
        self._session                   = session
        self._umgr_staging_output_queue = umgr_staging_output_queue
        self._update_queue              = update_queue

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

                cu = self._umgr_staging_output_queue.get()

                if not cu:
                    continue

                UpdateWorker.update_unit(queue = self._update_queue, 
                                         cu    = cu,
                                         state = rp.UMGR_STAGING_OUTPUT)

              # cu_list = rpu.blowup(cu, UMGR_STAGING_OUTPUT) 

                try:
                    self._handle_unit(cu)

                    UpdateWorker.update_unit(queue = self._update_queue, 
                                             cu    = cu,
                                             state = rp.DONE)

                except Exception as e:

                    # If we catch an exception, assume the staging failed
                    msg = "umgr output staging failed for unit %s: %s" % (cu['_id'], e)
                    self._log.exception(msg)

                    # If a staging directive fails, fail the CU also.
                    UpdateWorker.update_unit(queue = self._update_queue, 
                                             cu    = cu,
                                             state = rp.FAILED,
                                             msg   = msg)

            except Exception as e:
                self._log.exception('%s died', self.name)
                sys.exit(1)


    # --------------------------------------------------------------------------
    #
    def _handle_unit(self, cu):

        uid        = cu["_id"]
        workdir    = cu["sandbox"]
        directives = cu["UMGR_Staging_Output_Directives"]

        # Loop over all transfer directives and execute them.
        self._log.info("Processing output file transfers for ComputeUnit %s" % uid)

        for d in directives:

            # FIXME: we should interrupt file transfer on unit cancellation --
            # at least we should check before running the first op, and should
            # check inbetween ops...

          # action = d['action']   # not interpreted?
            source = d['source']
            target = d['target']
            flags  = d['flags']

            abs_src = "%s/%s" % (workdir, source)

            if os.path.basename(target) == target:
                abs_target = "file://localhost%s" % os.path.join(os.getcwd(), target)
            else:
                abs_target = "file://localhost%s" % os.path.abspath(target)

            self._log.debug("Transferring output file %s -> %s", abs_src, abs_target)

            output_file = None

            try:
                output_file = sfs.File(abs_src, session=self._session)

                if CREATE_PARENTS in flags:
                    copy_flags = sfs.CREATE_PARENTS
                else:
                    copy_flags = 0

                output_file.copy(abs_target, flags=copy_flags)

            except:
                raise

            finally:
                if output_file:
                    output_file.close()

# ------------------------------------------------------------------------------

