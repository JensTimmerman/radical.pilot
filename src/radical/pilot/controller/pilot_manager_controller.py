"""
.. module:: radical.pilot.controller.pilot_launcher_worker
.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import bson
import pprint
import datetime
import traceback
import thread
import threading

import weakref
from multiprocessing import Pool

import radical.utils as ru

from radical.pilot.states       import *
from radical.pilot.utils.logger import logger

from radical.pilot.controller.pilot_launcher_worker import PilotLauncherWorker

from radical.pilot.db.database import COMMAND_CANCEL_PILOT

import saga.utils.pty_shell as sup

IDLE_TIME  = 1.0  # seconds to sleep after idle cycles


# ----------------------------------------------------------------------------
#
class PilotManagerController(threading.Thread):
    """PilotManagerController is a threading worker that handles backend
       interaction for the PilotManager and Pilot classes.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, pilot_manager_uid, pilot_manager_data, 
        session, db_connection, db_connection_info, pilot_launcher_workers=1):
        """Le constructeur.
        """
        self._session = session

        # The MongoDB database handle.
        self._db = db_connection

        # Multithreading stuff
        threading.Thread.__init__(self)

        # Stop event can be set to terminate the main loop
        self._stop = threading.Event()
        self._stop.clear()

        # Initialized is set, once the run loop has pulled status
        # at least once. Other functions use it as a guard.
        self._initialized = threading.Event()
        self._initialized.clear()

        # Startup results contains a list of asynchronous startup results.
        self.startup_results = list()
        self.startup_results_lock = threading.Lock()

        # The shard_data_manager handles data exchange between the worker
        # process and the API objects. The communication is unidirectional:
        # workers WRITE to _shared_data and API methods READ from _shared_data.
        # The strucuture of _shared_data is as follows:
        #
        #  self._shared_data[pilot_uid] = {
        #      'data':          pilot_json,
        #      'callbacks':     []
        #      'facade_object': None
        #  }
        #  self._shared_worker_data = {
        #      'job_services':  {url: saga.job.Service}    # dict of job services
        #      'job_ids'     :  {pilot_id : (job_id, url)} # dict of pilot job handles 
        #  }
        #
        self._shared_data = dict()
        self._shared_worker_data = {'job_services' : dict(), 
                                    'job_ids'      : dict()}

        # The manager-level callbacks.
        self._manager_callbacks = list()

        # The different command queues hold pending operations
        # that are passed to the worker. Command queues are inspected during
        # runtime in the run() loop and the worker acts upon them accordingly.
        #
        if pilot_manager_uid is None:
            # Try to register the PilotManager with the database.
            self._pm_id = self._db.insert_pilot_manager(
                pilot_manager_data=pilot_manager_data,
                pilot_launcher_workers=pilot_launcher_workers
            )
            self._num_pilot_launcher_workers = pilot_launcher_workers
        else:
            pm_json = self._db.get_pilot_manager(pilot_manager_id=pilot_manager_uid)
            self._pm_id = pilot_manager_uid
            self._num_pilot_launcher_workers = pm_json["pilot_launcher_workers"]

        # The pilot launcher worker(s) are autonomous processes that
        # execute pilot bootstrap / launcher requests concurrently.
        self._pilot_launcher_worker_pool = []
        for worker_number in range(1, self._num_pilot_launcher_workers+1):
            worker = PilotLauncherWorker(
                session=self._session,
                db_connection_info=db_connection_info, 
                pilot_manager_id=self._pm_id,
                shared_worker_data=self._shared_worker_data,
                number=worker_number
            )
            self._pilot_launcher_worker_pool.append(worker)
            worker.start()

        self._callback_histories = dict()

    # ------------------------------------------------------------------------
    #
    @classmethod
    def uid_exists(cls, db_connection, pilot_manager_uid):
        """Checks wether a pilot unit manager UID exists.
        """
        exists = False

        if pilot_manager_uid in db_connection.list_pilot_manager_uids():
            exists = True

        return exists

    # ------------------------------------------------------------------------
    #
    @property
    def pilot_manager_uid(self):
        """Returns the uid of the associated PilotMangager
        """
        return self._pm_id

    # ------------------------------------------------------------------------
    #
    def list_pilots(self):
        """List all known pilots.
        """
        return self._db.list_pilot_uids(self._pm_id)

    # ------------------------------------------------------------------------
    #
    def get_compute_pilot_data(self, pilot_ids=None):
        """Returns the raw data (json dicts) of one or more ComputePilots
           registered with this Worker / PilotManager
        """
        # Wait for the initialized event to assert proper operation.
        self._initialized.wait()

        try:
            if  pilot_ids is None:
                pilot_ids = self._shared_data.keys ()

            return_list_type = True
            if not isinstance(pilot_ids, list):
                return_list_type = False
                pilot_ids = [pilot_ids]

            data = list()
            for pilot_id in pilot_ids:
                data.append(self._shared_data[pilot_id]['data'])

            if  return_list_type :
                return data
            else :
                return data[0]

        except KeyError as e:
            logger.exception ("Unknown Pilot ID %s : %s" % (pilot_id, e))
            raise

    # ------------------------------------------------------------------------
    #
    def cancel_launcher(self):
        """cancel the launcher threads
        """
        for worker in self._pilot_launcher_worker_pool:
            logger.debug("pworker %s stops   launcher %s" % (self.name, worker.name))
            worker.stop ()
            worker.join ()
            logger.debug("pworker %s stopped launcher %s" % (self.name, worker.name))


    # ------------------------------------------------------------------------
    #
    def stop(self):
        """stop() signals the process to finish up and terminate.
        """
        logger.debug("pworker %s stopping" % (self.name))
        self._stop.set()
        self.join()
        logger.debug("pworker %s stopped" % (self.name))

      # logger.debug("Worker thread (ID: %s[%s]) for PilotManager %s stopped." %
      #             (self.name, self.ident, self._pm_id))

    # ------------------------------------------------------------------------
    #
    def call_callbacks(self, pilot_id, new_state):
        """Wrapper function to call all all relevant callbacks, on pilot-level
        as well as manager-level.
        """

        # this is the point where, at the earliest, the application could have
        # been notified about pilot state changes.  So we record that event.
        if  not pilot_id in self._callback_histories :
            self._callback_histories[pilot_id] = list()
        self._callback_histories[pilot_id].append (
                {'timestamp' : datetime.datetime.utcnow(), 
                 'state'     : new_state})

        for [cb, cb_data] in self._shared_data[pilot_id]['callbacks']:
            try:
                if  self._shared_data[pilot_id]['facade_object'] :
                    if  cb_data :
                        cb (self._shared_data[pilot_id]['facade_object'](), new_state, cb_data)
                    else :
                        cb (self._shared_data[pilot_id]['facade_object'](), new_state)
                else :
                    logger.error("Couldn't call callback (no pilot instance)")
            except Exception as e:
                logger.exception("Couldn't call callback function %s" % e)
                raise

        # If we have any manager-level callbacks registered, we
        # call those as well!
        for [cb, cb_data] in self._manager_callbacks:
            try:
                if  self._shared_data[pilot_id]['facade_object'] :
                    if  cb_data :
                        cb(self._shared_data[pilot_id]['facade_object'](), new_state, cb_data)
                    else :
                        cb(self._shared_data[pilot_id]['facade_object'](), new_state)
                else :
                    logger.error("Couldn't call manager callback (no pilot instance)")
            except Exception as e:
                logger.exception(
                    "Couldn't call callback function %s" % e)
                raise

        # if we meet a final state, we record the object's callback history for
        # later evalutation
        if  new_state in (DONE, FAILED, CANCELED) :
            self._db.publish_compute_pilot_callback_history (pilot_id, self._callback_histories[pilot_id])
      # print 'publishing Callback history for %s' % pilot_id


    # ------------------------------------------------------------------------
    #
    def run(self):
        """run() is called when the process is started via
           PilotManagerController.start().
        """

        # make sure to catch sys.exit (which raises SystemExit)
        try :

            logger.debug("Worker thread (ID: %s[%s]) for PilotManager %s started." %
                        (self.name, self.ident, self._pm_id))

            while not self._stop.is_set():

                # # Check if one or more startup requests have finished.
                # self.startup_results_lock.acquire()

                # new_startup_results = list()

                # for transfer_result in self.startup_results:
                #     if transfer_result.ready():
                #         result = transfer_result.get()

                #         self._db.update_pilot_state(
                #             pilot_uid=result["pilot_uid"],
                #             state=result["state"],
                #             sagajobid=result["saga_job_id"],
                #             pilot_sandbox=result["sandbox"],
                #             global_sandbox=result["global_sandbox"],
                #             submitted=result["submitted"],
                #             logs=result["logs"]
                #         )

                #     else:
                #         new_startup_results.append(transfer_result)

                # self.startup_results = new_startup_results

                # self.startup_results_lock.release()

                # Check and update pilots. This needs to be optimized at
                # some point, i.e., state pulling should be conditional
                # or triggered by a tailable MongoDB cursor, etc.
                pilot_list = self._db.get_pilots(pilot_manager_id=self._pm_id)
                action = False

                for pilot in pilot_list:
                    pilot_id = str(pilot["_id"])

                    new_state = pilot["state"]
                    if pilot_id in self._shared_data:
                        old_state = self._shared_data[pilot_id]["data"]["state"]
                    else:
                        old_state = None
                        self._shared_data[pilot_id] = {
                            'data':          pilot,
                            'callbacks':     [],
                            'facade_object': None
                        }

                    self._shared_data[pilot_id]['data'] = pilot

                    # FIXME: *groan* what a hack...  The Canceling state is by
                    # the nature of it not recorded in the database, but only in
                    # the local cache.  So if we see it as old state, we have to
                    # avoid state transitions into non-final states in the cache
                    # at all cost -- so we catch this here specifically
                    no_cb = False
                    if  old_state == CANCELING :
                        if  new_state not in [DONE, FAILED, CANCELED] :
                            # restore old state, making the cache explicitly
                            # different than the DB recorded state
                            self._shared_data[pilot_id]["data"]["state"] = old_state 

                            # do not tr igger a state cb!
                            no_cb = True

                    if new_state != old_state :
                        action = True

                        if not no_cb :
                            # On a state change, we fire zee callbacks.
                            logger.info("ComputePilot '%s' state changed from '%s' to '%s'." \
                                            % (pilot_id, old_state, new_state))

                            # The state of the pilot has changed, We call all
                            # pilot-level callbacks to propagate this.  This also
                            # includes communication to the unit scheduler which
                            # may, or may not, cancel the pilot's units.
                            self.call_callbacks(pilot_id, new_state)

                    # If the state is 'DONE', 'FAILED' or 'CANCELED', we also
                    # set the state of the compute unit accordingly (but only
                    # for non-final units)
                    if new_state in [FAILED, DONE, CANCELED]:
                        unit_ids = self._db.pilot_list_compute_units(pilot_uid=pilot_id)
                        self._db.set_compute_unit_state (
                            unit_ids=unit_ids, 
                            state=CANCELED,
                            src_states=[ PENDING_INPUT_STAGING,
                                         STAGING_INPUT,
                                         PENDING_EXECUTION,
                                         SCHEDULING,
                                         EXECUTING,
                                         PENDING_OUTPUT_STAGING,
                                         STAGING_OUTPUT
                                       ],
                            log="Pilot '%s' has terminated with state '%s'. CU canceled." % (pilot_id, new_state))

                # After the first iteration, we are officially initialized!
                if not self._initialized.is_set():
                    self._initialized.set()

                # sleep a little if this cycle was idle
                if  not action :
                    time.sleep(IDLE_TIME)

        except SystemExit as e :
            logger.exception ("pilot manager controller thread caught system exit -- forcing application shutdown")
            thread.interrupt_main ()

        finally :
            # shut down the autonomous pilot launcher worker(s)
            for worker in self._pilot_launcher_worker_pool:
                logger.debug("pworker %s stops   launcher %s" % (self.name, worker.name))
                worker.stop ()
                logger.debug("pworker %s stopped launcher %s" % (self.name, worker.name))



    # ------------------------------------------------------------------------
    #
    def register_start_pilot_request(self, pilot, resource_config):
        """Register a new pilot start request with the worker.
        """

        # create a new UID for the pilot
        pilot_uid = ru.generate_id ('pilot')

        # switch endpoint type
        fs_url = saga.Url(resource_config['filesystem_endpoint'])

        # Get the sandbox from either the pilot_desc or resource conf
        if pilot.description.sandbox:
            workdir_raw = pilot.description.sandbox
        else:
            workdir_raw = resource_config.get('default_remote_workdir', "$PWD")

        # If the sandbox contains expandables, we need to resolve those remotely.
        # TODO: Note that this will only work for (gsi)ssh or shell based access mechanisms
        if '$' in workdir_raw or '`' in workdir_raw:
            js_url = saga.Url(resource_config['job_manager_endpoint'])

            # The PTYShell will swallow in the job part of the scheme
            if js_url.scheme.endswith('+ssh'):
                js_url.scheme = 'ssh'
            elif js_url.scheme.endswith('+gsissh'):
                js_url.scheme = 'gsissh'
            elif js_url.scheme == 'fork':
                pass
            else:
                raise Exception("Are there more flavours we need to support?! (%s)" % js_url.scheme)

            # TODO: Why is this 'translation' required?
            if js_url.port is not None:
                url = "%s://%s:%d/" % (js_url.schema, js_url.host, js_url.port)
            else:
                url = "%s://%s/" % (js_url.schema, js_url.host)

            logger.debug("saga.utils.PTYShell ('%s')" % url)
            shell = sup.PTYShell(url, self._session, logger)

            ret, out, err = shell.run_sync(' echo "WORKDIR: %s"' % workdir_raw)
            if ret == 0 and 'WORKDIR:' in out :
                workdir_expanded = out.split(":")[1].strip()
                logger.debug("Determined remote working directory for %s: '%s'" % (url, workdir_expanded))
            else :
                error_msg = "Couldn't determine remote working directory."
                logger.error(error_msg)
                raise Exception(error_msg)
        else:
            workdir_expanded = workdir_raw

        # At this point we have determined the remote 'pwd'
        fs_url.path = "%s/radical.pilot.sandbox" % workdir_expanded

        # This is the base URL / 'sandbox' for the pilot!
        agent_dir_url = saga.Url("%s/%s-%s/" % (str(fs_url), self._session.uid, pilot_uid))

        # Create a database entry for the new pilot.
        pilot_uid, pilot_json = self._db.insert_pilot(
            pilot_uid=pilot_uid,
            pilot_manager_uid=self._pm_id,
            pilot_description=pilot.description,
            pilot_sandbox=str(agent_dir_url), 
            global_sandbox=str(fs_url.path)
            )

        # Create a shared data store entry
        self._shared_data[pilot_uid] = {
            'data':          pilot_json,
            'callbacks':     [],
            'facade_object': weakref.ref(pilot)
        }

        return pilot_uid

    # ------------------------------------------------------------------------
    #
    def register_pilot_callback(self, pilot, callback_func, callback_data=None):
        """Registers a callback function.
        """
        pilot_uid = pilot.uid
        self._shared_data[pilot_uid]['callbacks'].append([callback_func, callback_data])

        # Add the facade object if missing, e.g., after a re-connect.
        if  self._shared_data[pilot_uid]['facade_object'] is None:
            self._shared_data[pilot_uid]['facade_object'] = weakref.ref(pilot)

        # Callbacks can only be registered when the ComputeAlready has a
        # state. To partially address this shortcomming we call the callback
        # with the current ComputePilot state as soon as it is registered.
        self.call_callbacks(
            pilot.uid,
            self._shared_data[pilot_uid]["data"]["state"]
        )

    # ------------------------------------------------------------------------
    #
    def register_manager_callback(self, callback_func, callback_data=None):
        """Registers a manager-level callback.
        """
        self._manager_callbacks.append([callback_func, callback_data])

    # ------------------------------------------------------------------------
    #
    def register_cancel_pilots_request(self, pilot_ids=None):
        """Registers one or more pilots for cancelation.
        """

        if pilot_ids is None:

            pilot_ids = list()

            for pilot in self._db.get_pilots(pilot_manager_id=self._pm_id) :
                pilot_ids.append (str(pilot["_id"]))


        self._db.send_command_to_pilot(COMMAND_CANCEL_PILOT, pilot_ids=pilot_ids)
        logger.info("Sent 'COMMAND_CANCEL_PILOT' command to pilots %s.", pilot_ids)

        # pilots which are in ACTIVE state should now have time to react on the
        # CANCEL command sent above.  Meanwhile, we'll cancel all pending
        # pilots.  If that is done, we wait a little, say 10 seconds, to give
        # the pilot time to pick up the request and shut down -- but if it does
        # not do that, it will get killed the hard way...
        delayed_cancel = list()

        for pilot_id in pilot_ids :
            if  pilot_id in self._shared_data :

                # read state fomr _shared_data only once, so that it does not
                # change under us...
                old_state = str(self._shared_data[pilot_id]["data"]["state"])

                logger.warn ("actively cancel pilot %s state: %s" % (pilot_id, old_state))
                if  old_state in [DONE, FAILED, CANCELED] :
                    logger.warn ("can't actively cancel pilot %s: already in final state" % pilot_id)

                elif old_state in [PENDING_LAUNCH, LAUNCHING, PENDING_ACTIVE] :
                    if pilot_id in self._shared_worker_data['job_ids'] :

                        try :
                            job_id, js_url = self._shared_worker_data['job_ids'][pilot_id]
                            self._shared_data[pilot_id]["data"]["state"] = CANCELING
                            logger.info ("actively cancel pilot %s (%s, %s)" % (pilot_id, job_id, js_url))

                            js = self._shared_worker_data['job_services'][js_url]
                            job = js.get_job (job_id)
                            job.cancel ()
                        except Exception as e :
                            logger.exception ('pilot cancelation failed')


                    else :
                        logger.warn ("can't actively cancel pilot %s: no job id known" % pilot_id)
                        logger.debug (pprint.pformat (self._shared_worker_data))

                else :
                    logger.debug ("delay to actively cancel pilot %s: state %s" % (pilot_id, old_state))
                    delayed_cancel.append (pilot_id)

            else :
                logger.warn  ("can't actively cancel pilot %s: unknown pilot" % pilot_id)
                logger.debug (pprint.pformat (self._shared_data))

        # now tend to all delayed cancellation requests (ie. active pilots) --
        # if there are any
        if  delayed_cancel :

            # grant some levay to the unruly children...
            time.sleep (10)

            for pilot_id in delayed_cancel :

                if pilot_id in self._shared_worker_data['job_ids'] :

                    try :
                        job_id, js_url = self._shared_worker_data['job_ids'][pilot_id]
                        logger.info ("actively cancel pilot %s (delayed) (%s, %s)" % (pilot_id, job_id, js_url))

                        js = self._shared_worker_data['job_services'][js_url]
                        job = js.get_job (job_id)
                        job.cancel ()
                    except Exception as e :
                        logger.warn ('delayed pilot cancelation failed. '
                                'This is not necessarily a problem.')

                else :
                    logger.warn ("can't actively cancel pilot %s: no job id known (delayed)" % pilot_id)
                    logger.debug (pprint.pformat (self._shared_worker_data))



# ------------------------------------------------------------------------------

