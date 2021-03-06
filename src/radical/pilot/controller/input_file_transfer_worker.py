
__copyright__ = "Copyright 2013-2015, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import thread
import threading

from radical.pilot.states import * 
from radical.pilot.utils.logger import logger
from radical.pilot.staging_directives import CREATE_PARENTS

IDLE_TIME  = 1.0  # seconds to sleep after idle cycles

# ----------------------------------------------------------------------------
#
class InputFileTransferWorker(threading.Thread):
    """InputFileTransferWorker handles the staging of input files
    for a UnitManagerController.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, session, db_connection_info, unit_manager_id, number=None):

        self._session = session

        # threading stuff
        threading.Thread.__init__(self)
        self.daemon = True

        self.db_connection_info = db_connection_info
        self.unit_manager_id = unit_manager_id

        self._worker_number = number
        self.name = "InputFileTransferWorker-%s" % str(self._worker_number)

        # we cache saga directories for performance, to speed up sandbox
        # creation.
        self._saga_dirs = dict()

        # Stop event can be set to terminate the main loop
        self._stop = threading.Event()
        self._stop.clear()


    # ------------------------------------------------------------------------
    #
    def stop(self):
        """stop() signals the process to finish up and terminate.
        """
        logger.debug("itransfer %s stopping" % (self.name))
        self._stop.set()
        self.join()
        logger.debug("itransfer %s stopped" % (self.name))


    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the process when Process.start() is called.
        """

        # make sure to catch sys.exit (which raises SystemExit)
        try :

            logger.info("Starting InputFileTransferWorker")

            # Try to connect to the database and create a tailable cursor.
            try:
                connection = self.db_connection_info.get_db_handle()
                db = connection[self.db_connection_info.dbname]
                um_col = db["%s.cu" % self.db_connection_info.session_id]
                logger.debug("Connected to MongoDB. Serving requests for UnitManager %s." % self.unit_manager_id)

            except Exception as e :
                logger.exception("Connection error: %s" % e)
                raise

            while not self._stop.is_set():
                # See if we can find a ComputeUnit that is waiting for
                # input file transfer.
                ts = datetime.datetime.utcnow()
                compute_unit = um_col.find_and_modify(
                    query={"unitmanager": self.unit_manager_id,
                           "state"      : PENDING_INPUT_STAGING,
                           },
                    update={"$set" : {"state": STAGING_INPUT},
                            "$push": {"statehistory": {"state": STAGING_INPUT, "timestamp": ts}}}
                )

                if compute_unit is None:
                    # Sleep a bit if no new units are available.
                    time.sleep(IDLE_TIME)

                else:
                    compute_unit_id = None
                    state = STAGING_INPUT

                    try:
                        log_messages = []

                        # We have found a new CU. Now we can process the transfer
                        # directive(s) wit SAGA.
                        compute_unit_id = str(compute_unit["_id"])
                        logger.debug ("InputStagingController: unit found: %s" % compute_unit_id)
                        remote_sandbox = compute_unit["sandbox"]
                        input_staging = compute_unit.get("FTW_Input_Directives", [])

                        # We need to create the CU's directory in case it doesn't exist yet.
                        log_msg = "InputStagingController: Creating ComputeUnit sandbox directory %s." % remote_sandbox
                        log_messages.append(log_msg)
                        logger.info(log_msg)

                        # Creating/initialising the sandbox directory.
                        try:
                            logger.debug ("saga.fs.Directory ('%s')" % remote_sandbox)

                            # url used for saga
                            remote_sandbox_url = saga.Url(remote_sandbox)

                            # keyurl and key used for cache
                            remote_sandbox_keyurl = saga.Url(remote_sandbox)
                            remote_sandbox_keyurl.path = '/'
                            remote_sandbox_key = str(remote_sandbox_keyurl)

                            if  remote_sandbox_key not in self._saga_dirs :
                                self._saga_dirs[remote_sandbox_key] = \
                                        saga.filesystem.Directory(remote_sandbox_url,
                                                flags=saga.filesystem.CREATE_PARENTS,
                                                session=self._session)

                            saga_dir = self._saga_dirs[remote_sandbox_key]
                        except Exception as e :
                            logger.exception('Error: %s' % e)
                            raise

                        logger.info("InputStagingController: Processing input file transfers for ComputeUnit %s" % compute_unit_id)
                        # Loop over all transfer directives and execute them.
                        for sd in input_staging:

                            logger.debug("InputStagingController: sd: %s : %s" % (compute_unit_id, sd))

                            # Check if there was a cancel request
                            state_doc = um_col.find_one(
                                {"_id": compute_unit_id},
                                fields=["state"]
                            )
                            if state_doc['state'] == CANCELED:
                                logger.info("Compute Unit Canceled, interrupting input file transfers.")
                                state = CANCELED
                                # Break out of the loop for this CU's SD's
                                break

                            abs_src = os.path.abspath(sd['source'])
                            input_file_url = saga.Url("file://localhost%s" % abs_src)
                            if not sd['target']:
                                target = '%s/%s' % (remote_sandbox, os.path.basename(abs_src))
                            else:
                                target = "%s/%s" % (remote_sandbox, sd['target'])

                            log_msg = "Transferring input file %s -> %s" % (input_file_url, target)
                            log_messages.append(log_msg)
                            logger.debug(log_msg)

                            # Execute the transfer.
                            if CREATE_PARENTS in sd['flags']:
                                copy_flags = saga.filesystem.CREATE_PARENTS
                            else:
                                copy_flags = 0

                            try:
                                saga_dir.copy(input_file_url, target, flags=copy_flags)
                            except Exception as e:
                                logger.exception(e)
                                raise Exception("copy failed(%s)" % e.message)

                        # If this CU was canceled we can skip the remainder of this loop,
                        # to process more CUs.
                        if state == CANCELED:
                            continue

                        # All IFTW staging done for this CU, push to Agent.
                        logger.debug("InputStagingController: %s : push to agent" % compute_unit_id)
                        um_col.update({'_id': compute_unit_id},
                                      {'$set': {'state': PENDING_AGENT_INPUT_STAGING},
                                       '$push': {
                                           'statehistory': {
                                               'state': PENDING_AGENT_INPUT_STAGING,
                                               'timestamp': ts},
                                           'log': {
                                               'timestamp': datetime.datetime.utcnow(),
                                               'message': 'push unit to agent after ftw staging'
                                       }}})

                    except Exception as e :

                        # Update the CU's state to 'FAILED'.
                        ts = datetime.datetime.utcnow()
                        logentry = {'message': "Input transfer failed: %s" % e,
                                    'timestamp': ts}

                        um_col.update({'_id': compute_unit_id},
                                      {'$set': {'state': FAILED},
                                       '$push': {
                                           'statehistory': {'state': FAILED, 'timestamp': ts},
                                           'log': logentry
                                       }})

                        logger.exception(str(logentry))
                        raise

        except SystemExit as e :
            logger.debug("input file transfer thread caught system exit -- forcing application shutdown")
            thread.interrupt_main ()
