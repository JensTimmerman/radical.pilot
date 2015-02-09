#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.pilot_manager
   :platform: Unix
   :synopsis: Provides the interface for the PilotManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import time
import threading
import multiprocessing

import radical.utils as ru

from radical.pilot.types        import *
from radical.pilot.states       import *
from radical.pilot.exceptions   import *

from radical.pilot.compute_pilot   import ComputePilot
from radical.pilot.utils.logger    import logger
from radical.pilot.resource_config import ResourceConfig

# ------------------------------------------------------------------------------
#
# PilotManager setup
#
PMGR_THREADS   = 'threading'
PMGR_PROCESSES = 'multiprocessing'

PMGR_MODE      = PMGR_THREADS

if PMGR_MODE == PMGR_THREADS :
    COMPONENT_MODE = threading
    COMPONENT_TYPE = threading.Thread
    QUEUE_TYPE     = multiprocessing.Queue
    LOCK_TYPE      = multiprocessing.RLock
elif PMGR_MODE == PMGR_PROCESSES :
    COMPONENT_MODE = multiprocessing
    COMPONENT_TYPE = multiprocessing.Process
    QUEUE_TYPE     = multiprocessing.Queue
    LOCK_TYPE      = multiprocessing.RLock

# component IDs

PMGR                = 'PMGR'
PMGR_LAUNCHER       = 'PMGR_Launcher'

# Number of worker threads
NUMBER_OF_WORKERS = {
        PMGR                :   1,
        PMGR_LAUNCHER       :   1
}

# factor by which the number of pilots are increased at a certain step.  Value of
# '1' will leave the pilots unchanged.  Any blowup will leave on pilot as the
# original, and will then create clones with an changed pilot ID (see blowup()).
BLOWUP_FACTOR = {
        PMGR                :   1,
        PMGR_LAUNCHER       :   1
}

# flag to drop all blown-up pilots at some point in the pipeline.  The pilots
# with the original IDs will again be left untouched, but all other pilots are
# silently discarded.
DROP_CLONES = {
        PMGR                : True,
        PMGR_LAUNCHER       : True
}

# ------------------------------------------------------------------------------
#
# config flags 
#
# FIXME: move to some RP config
#
rp_config = dict()
rp_config['blowup_factor']     = BLOWUP_FACTOR
rp_config['drop_clones']       = DROP_CLONES
rp_config['number_of_workers'] = NUMBER_OF_WORKERS


# -----------------------------------------------------------------------------
#
class PilotManager(object):
    """A PilotManager holds :class:`radical.pilot.ComputePilot` instances that are
    submitted via the :func:`radical.pilot.PilotManager.submit_pilots` method.

    Each PilotManager has a unique identifier :data:`radical.pilot.PilotManager.uid`
    that can be used to re-connect to previoulsy created PilotManager in a
    given :class:`radical.pilot.Session`.

    **Example**::

        s = radical.pilot.Session()

        pm1 = radical.pilot.PilotManager(session=s)                # create new
        pm2 = radical.pilot.PilotManager(session=s, uid=pm1.uid)   # reconnect

        # pm1 and pm2 are pointing to the same PilotManager
        assert pm1.uid == pm2.uid
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self, session, uid=None):
        """Creates a new PilotManager and attaches it to the session.

        **Arguments:**

            * **session** [:class:`radical.pilot.Session`]:
              The session instance to use.

            * uid (`string`): If a PilotManager id is given, the backend is
              searched for an existing instance to reconnect to.

        **Returns:**

            * A new `PilotManager` object [:class:`radical.pilot.PilotManager`].
        """

        COMPONENT_TYPE.__init__(self)

        self._session       = session
        self._log           = session._log
        self._updater       = session._updater

        # set up internal state
        self._state          = ACTIVE
        self._callbacks      = dict()
        self._pilots         = list()  # dict?
        self._worker_list    = list()

        if uid :
            # FIXME: re-implement reconnect.  Basically need to dig out the
            # pilot handles...
            self._uid = uid

        else :
            self._uid = ru.generate_id('pmgr.%(counter)02d', ru.ID_CUSTOM)

        # register with session for shutdown
        # FIXME: should session be a factory after all?
        self._session._pilot_manager_objects.append(self)

        # we want to own all queues -- that simplifies startup and shutdown
        self._pmgr_launcher_queue = QUEUE_TYPE()

        # spawn the pilot launcher
        for n in range(rp_config['number_of_workers'][PMGR_LAUNCHER]):
            worker = PMGR_Launcher(
                name                      = "PMGR-Launcher-%d" % n,
                config                    = rp_config, 
                logger                    = self._log,
                session                   = self._session,
                pmgr_launcher_queue       = self._pmgr_launcher_queue)
            self._worker_list.append(worker)


        # FIXME: also spawn a separate thread/process to pull for state updates
        # from mongodb, and to issue callbacks etc from there.  This will need
        # to be replaced with some queue, eventually -- for now we pull :/
        # coordinate actions with worker thread/process
        self._lock = LOCK_TYPE()
        self.start ()


    # -------------------------------------------------------------------------
    #
    def close(self, terminate=True):
        """Shuts down the PilotManager and its background workers in a 
        coordinated fashion.

        **Arguments:**

            * **terminate** [`bool`]: If set to True, all active pilots will 
              be canceled (default: False).

        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")

        logger.debug("pmgr    %s closing" % (str(self._uid)))

        # if terminate is set, we cancel all pilots. 
        if  terminate :

            to_cancel = [pilot.uid for pilot in self._pilots if pilot.state not in FINAL]

            for pid in to_cancel:
                logger.debug("pmgr    %s cancel   pilot  %s" % (str(self._uid), pilot._uid))

            self.cancel_pilots(to_cancel)
            states = self.wait_pilots(to_cancel, timeout=10)

            for state,pilot in zip(states, self._pilots):
                if state not in FINAL :
                    logger.warning("pmgr    %s ignore   pilot  %s (%s)" % (str(self._uid), pilot._uid, state))

        # drop object references
        self._pilots = list()

        # stop all threads etc
        for worker in self._worker_list:
            worker.stop()

        self.stop()

        # also send a wakeup signals through all queues
        self._pmgr_launcher_queue.put(None)

        # bye bye Junimond...
        self._state = CANCELED
        logger.debug("pmgr    %s closed" % (str(self._uid)))


    # -------------------------------------------------------------------------
    #
    def __str__(self):

        return self._uid


    # -------------------------------------------------------------------------
    #
    @property
    def uid(self):

        return self._uid


    # -------------------------------------------------------------------------
    #
    def submit_pilots(self, pilot_descriptions):
        """Submits a new :class:`radical.pilot.ComputePilot` to a resource.

        **Arguments:**

            * **pilot_descriptions** [:class:`radical.pilot.ComputePilotDescription`
              or list of :class:`radical.pilot.ComputePilotDescription`]: The
              description of the compute pilot instance(s) to create.

        **Returns:**

            * One or more :class:`radical.pilot.ComputePilot` instances
              [`list of :class:`radical.pilot.ComputePilot`].
        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")

        # Implicit list conversion.
        return_list_type = True
        if  not isinstance(pilot_descriptions, list):
            return_list_type   = False
            pilot_descriptions = [pilot_descriptions]

        # Itereate over the pilot descriptions, try to create a pilot for
        # each one
        pilots = list()

        for pilot_description in pilot_descriptions:

            if not pilot_description.resource:
                raise BadParameter("ComputePilotDescription must define 'resource'.")

            if not pilot_description.runtime:
                raise BadParameter("ComputePilotDescription must define 'runtime'.")

            if not pilot_description.cores:
                raise BadParameter("ComputePilotDescription must define 'cores'.")

            resource_key = pilot_description.resource
            resource_cfg = self._session.get_resource_config(resource_key)

            # Check resource-specific mandatory attributes
            for arg in resource_cfg.get ("mandatory_args", []):
                if not pilot_description[arg] :
                    raise BadParameter("ComputePilotDescription must define"
                            " '%s' for resource '%s'" % (arg, resource_key))

            # we expand and exchange keys in the resource config, depending on
            # the selected schema so better use a deep copy...
            import copy
            resource_cfg  = copy.deepcopy (resource_cfg)
            schema        = pilot_description['access_schema']

            if  not schema :
                if 'schemas' in resource_cfg :
                    schema = resource_cfg['schemas'][0]
              # import pprint
              # print "no schema, using %s" % schema
              # pprint.pprint (pilot_description)

            if  not schema in resource_cfg :
              # import pprint
              # pprint.pprint (resource_cfg)
                logger.warning ("schema %s unknown for resource %s -- continue with defaults" \
                             % (schema, resource_key))

            else :
                for key in resource_cfg[schema] :
                    # merge schema specific resource keys into the
                    # resource config
                    resource_cfg[key] = resource_cfg[schema][key]

            # If 'default_sandbox' is defined, set it.
            if pilot_description.sandbox is not None:
                if resource_cfg.get("valid_roots"):
                    is_valid = False
                    for vr in resource_cfg["valid_roots"]:
                        if pilot_description.sandbox.startswith(vr):
                            is_valid = True
                            break
                    if not is_valid:
                        raise BadParameter("Working directory for resource '%s' "
                                "defined as '%s' but needs to be rooted in %s " \
                                % (resource_key, pilot_description.sandbox, 
                                   resource_cfg["valid_roots"]))

            # After the sanity checks have passed, we can register a pilot
            # startup request with the worker process and create a facade
            # object.

            pilot = ComputePilot(description=pilot_description, pmgr=self)

            # TODO: publish pilots in DB

            # resource_config=resource_cfg)

            pilots.append(pilot)


        with self._lock:
            self._pilots += pilots


        if return_list_type : return pilots
        else                : return pilots[0]


    # -------------------------------------------------------------------------
    #
    def list_pilots(self):
        """Lists the unique identifiers of all :class:`radical.pilot.ComputePilot`
        instances associated with this PilotManager

        **Returns:**

            * A list of :class:`radical.pilot.ComputePilot` uids [`string`].
        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")

        return [pilot.uid for pilot in self._pilots]


    # -------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_ids=None):
        """Returns one or more :class:`radical.pilot.ComputePilot` instances  identified by their IDs.

        **Arguments:**

            * **pilot_uids** [string or`list of strings`]: If pilot_uids is set,
              only the Pilots with  the specified uids are returned. If
              pilot_uids is `None`, all Pilots are returned.

        **Returns:**

            * A list of :class:`radical.pilot.ComputePilot` objects
              [`list of :class:`radical.pilot.ComputePilot`].
        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")

        if not pilot_ids:
            return self._pilots[:]

        return_list_type = True
        if not isinstance(pilot_ids, list):
            return_list_type = False
            pilot_ids = [pilot_ids]

        pilots = [pilot for pilot in self._pilots if pilot.uid in pilot_ids]

        if return_list_type : return pilots
        else                : return pilots[0]


    # -------------------------------------------------------------------------
    #
    def wait_pilots(self, pilot_ids=None, state=None, timeout=None):
        """Returns when one or more :class:`radical.pilot.ComputePilots` reach a
        specific state or when an optional timeout is reached.

        If `pilot_uids` is `None`, `wait_pilots` returns when **all**
        ComputePilots reach the state defined in `state`, or have progressed
        beyond that state.

        **Example**::

            # TODO -- add example

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`]
              If pilot_uids is set, only the Pilots with the specified uids are
              considered. If pilot_uids is `None` (default), all Pilots are
              considered.

            * **state** [`list of strings`]
              The state(s) that Pilots have to reach in order for the call
              to return.

              By default `wait_pilots` waits for the Pilots to reach
              a **terminal** state, which can be one of the following:

              * :data:`radical.pilot.DONE`
              * :data:`radical.pilot.FAILED`
              * :data:`radical.pilot.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless
              whether the Pilots have reached the desired state or not.
              The default value **-1.0** never times out.
        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")

        state_values = {NEW               : 0,
                        LAUNCHING_PENDING : 1,
                        LAUNCHING         : 2,
                        ACTIVE_PENDING    : 3,
                        ACTIVE            : 4,
                        DONE              : 5,
                        FAILED            : 5,
                        CANCELED          : 5}

        if not state:
            state = [DONE, FAILED, CANCELED]

        if not isinstance(state, list):
            state = [state]

        return_list_type = True
        if not pilot_ids :
            pilots = self._pilots
        else :
            if not isinstance(pilot_ids, list):
                return_list_type = False
                pilots = self.get_pilots ([pilot_ids])
            else:
                pilots = self.get_pilots (pilot_ids)

        # we don't need to check pilots over and over again, once they satisfy
        # the state condition -- so keep a check list and reduce it over time
        checklist = pilots[:]

        start  = time.time()
        states = list()

        while True :

            # check timeout
            if  timeout and (timeout <= (time.time()-start)):
                logger.debug ("wait timed out: %s" % states)

                if return_list_type : return [pilot.state for pilot in pilots]
                else                : return pilots[0].state

            # sleep a little if previous cycle was idle
            time.sleep (0.1)

            # check pilot states
            all_ok = True
            for pilot in checklist[:] :
                
                pilot_ok    = False
                pilot_state = pilot.state
                
                for s in state:
                    if  state_values(pilot_state) >= state_values(s):
                        pilot_ok = True
                        checklist.remove(pilot) # don't check this one anymore...
                        break

                if not pilot_ok :
                    all_ok = False
                    break

            if all_ok:
                # did not see a 'not pilot_ok', so declare success!
                if return_list_type : return [pilot.state for pilot in pilots]
                else                : return pilots[0].state


    # -------------------------------------------------------------------------
    #
    def cancel_pilots(self, pilot_ids=None):
        """Cancels one or more ComputePilots.

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`]
              If pilot_uids is set, only the Pilots with the specified uids are
              canceled. If pilot_uids is `None`, all Pilots are canceled.
        """
        # Check if the object instance is still valid.
        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")

        pilots = self.get_pilots (pilot_ids)

        # FIXME:
        # cancelling pilots needs two actions:
        #   - setting the state in the DB to CANCELLED
        #   - sending CANCEL_PILOT commands to all components downstream of 
        #     the current units. Whoever receives the cancel call will simply 
        #     drop the pilot.

        for pilot in pilots:
            pilot.cancel()


    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_function, metric=PILOT_STATE, callback_data=None):
        """Registers a new callback function with the PilotManager.
        Manager-level callbacks get called if the specified metric changes.  The
        metric `PILOT_STATE` fires the callback if any of the ComputePilots
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def callback_func(obj, value, data)

        where ``object`` is a handle to the object that triggered the callback,
        ``value`` is the metric, and ``data`` is the data provided on
        callback registration.  In the example of `PILOT_STATE` above, the
        object would be the pilot in question, and the value would be the new
        state of the pilot.

        Available metrics are:

          * `PILOT_STATE`: fires when the state of any of the pilots which are
            managed by this pilot manager instance is changing.  It communicates
            the pilot object instance and the pilot's new state.

        """
        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")

        if  metric not in PILOT_MANAGER_METRICS :
            raise ValueError ("Metric '%s' is not available on the pilot manager" % metric)

        with self._lock :

            if not metric in self._callbacks :
                self._callbacks[metric] = list()

            self._callbacks[metric].append (callback_function, callback_state)


# ------------------------------------------------------------------------------

