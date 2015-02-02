    #pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.unit_manager
   :platform: Unix
   :synopsis: Implementation of the UnitManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import time
import threading
import multiprocessing

import radical.utils as ru

from radical.pilot.compute_unit import ComputeUnit
from radical.pilot.utils.logger import logger

from radical.pilot.scheduler    import UMGR_Scheduler
from radical.pilot.staging      import UMGR_Staging_Input
from radical.pilot.staging      import UMGR_Staging_Output

from radical.pilot.types        import *
from radical.pilot.states       import *
from radical.pilot.exceptions   import *

# ------------------------------------------------------------------------------
#
# UnitManager setup
#
UMGR_THREADS   = 'threading'
UMGR_PROCESSES = 'multiprocessing'

UMGR_MODE      = UMGR_THREADS

if UMGR_MODE == UMGR_THREADS :
    COMPONENT_MODE = threading
    COMPONENT_TYPE = threading.Thread
    QUEUE_TYPE     = multiprocessing.Queue
elif UMGR_MODE == UMGR_PROCESSES :
    COMPONENT_MODE = multiprocessing
    COMPONENT_TYPE = multiprocessing.Process
    QUEUE_TYPE     = multiprocessing.Queue

# component IDs

UMGR                = 'UMGR'
UMGR_SCHEDULER      = 'UMGR_Scheduler'
UMGR_STAGING_INPUT  = 'UMGR_Staging_Input'
UMGR_STAGING_OUTPUT = 'UMGR_Staging_Output'

# Number of worker threads
NUMBER_OF_WORKERS = {
        UMGR                :   1,
        UMGR_SCHEDULER      :   1,
        UMGR_STAGING_INPUT  :   1,
        UMGR_STAGING_OUTPUT :   1
}

# factor by which the number of units are increased at a certain step.  Value of
# '1' will leave the units unchanged.  Any blowup will leave on unit as the
# original, and will then create clones with an changed unit ID (see blowup()).
BLOWUP_FACTOR = {
        UMGR                :   1,
        UMGR_SCHEDULER      :   1,
        UMGR_STAGING_INPUT  :   1,
        UMGR_STAGING_OUTPUT :   1
}

# flag to drop all blown-up units at some point in the pipeline.  The units
# with the original IDs will again be left untouched, but all other units are
# silently discarded.
DROP_CLONES = {
        UMGR                : True,
        UMGR_SCHEDULER      : True,
        UMGR_STAGING_INPUT  : True,
        UMGR_STAGING_OUTPUT : True
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


# =============================================================================
#
class UnitManager(COMPONENT_TYPE):
    """A UnitManager manages :class:`radical.pilot.ComputeUnit` instances which
    represent the **executable** workload in RADICAL-Pilot. A UnitManager connects
    the ComputeUnits with one or more :class:`Pilot` instances (which represent
    the workload **executors** in RADICAL-Pilot) and a **scheduler** which
    determines which :class:`ComputeUnit` gets executed on which
    :class:`Pilot`.

    Each UnitManager has a unique identifier :data:`radical.pilot.UnitManager.uid`
    that can be used to re-connect to previoulsy created UnitManager in a
    given :class:`radical.pilot.Session`.

    **Example**::

        s = radical.pilot.Session(database_url=DBURL)

        pm = radical.pilot.PilotManager(session=s)

        pd = radical.pilot.ComputePilotDescription()
        pd.resource = "futuregrid.alamo"
        pd.cores = 16

        p1 = pm.submit_pilots(pd) # create first pilot with 16 cores
        p2 = pm.submit_pilots(pd) # create second pilot with 16 cores

        # Create a workload of 128 '/bin/sleep' compute units
        compute_units = []
        for unit_count in range(0, 128):
            cu = radical.pilot.ComputeUnitDescription()
            cu.executable = "/bin/sleep"
            cu.arguments = ['60']
            compute_units.append(cu)

        # Combine the two pilots, the workload and a scheduler via
        # a UnitManager.
        um = radical.pilot.UnitManager(session=session,
                                   scheduler=radical.pilot.SCHED_ROUND_ROBIN)
        um.add_pilot(p1)
        um.submit_units(compute_units)
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self, session, scheduler=None, uid=None):
        """Creates a new UnitManager and attaches it to the session.

        **Args:**

            * session (`string`): The session instance to use.

            * scheduler (`string`): The name of the scheduler plug-in to use.

            * uid (`string`): If a UnitManager id is given, the backend is
            * searched for an existing instance to reconnect to.
              worker processes to launch in the background. 
        """

        COMPONENT_TYPE.__init__(self)

        self._session = session
        self._log     = session._log


        if uid :
            # FIXME: re-implement reconnect.  Basically need to dig out the
            # pilot handles...
            self._uid = uid

        else :
            self._uid = ru.generate_id('umgr.%(counter)02d', ru.ID_CUSTOM)

        # register with session for shutdown
        # FIXME: should session be a factory after all?
        self._session._unit_manager_objects.append(self)

        # keep track of some stuff
        self.wait_queue_size = 0
        self._unit_list      = list()
        self._pilot_list     = list()
        self._worker_list    = list()
        self._scheduler_list = list()

        # we want to own all queues -- that simplifies startup and shutdown
        self._umgr_schedule_queue       = QUEUE_TYPE()
        self._umgr_staging_input_queue  = QUEUE_TYPE()
        self._umgr_staging_output_queue = QUEUE_TYPE()
        self._update_queue              = QUEUE_TYPE()



        # spawn the scheduler instances (usually 1)
        for n in range(rp_config['number_of_workers'][UMGR_SCHEDULER]):
            worker = UMGR_Scheduler.create (
                name                      = "UMGR_Scheduler-%d" % n,
                config                    = rp_config, 
                logger                    = self._log,
                session                   = self._session,
                scheduler                 = scheduler, 
                umgr_schedule_queue       = self._umgr_schedule_queue,
                umgr_staging_input_queue  = self._umgr_staging_input_queue,
              # agent_staging_input_queue = self._None,
              # agent_scheduling_queue    = self._None,
                update_queue              = self._update_queue)
            self._worker_list.append(worker)
            self._scheduler_list.append(worker)

        # spawn the input stager
        for n in range(rp_config['number_of_workers'][UMGR_STAGING_INPUT]):
            worker = UMGR_Staging_Input(
                name                      = "UMGR_Staging_Input-%d" % n,
                config                    = rp_config, 
                logger                    = self._log,
                session                   = self._session,
                umgr_staging_input_queue  = self._umgr_staging_input_queue,
              # agent_staging_input_queue = self._None,
              # agent_scheduling_queue    = self._None,
                update_queue              = self._update_queue)
            self._worker_list.append(worker)

        # spawn the output stager
        for n in range(rp_config['number_of_workers'][UMGR_STAGING_OUTPUT]):
            worker = UMGR_Staging_Output(
                name                      = "UMGR_Staging_Output-%d" % n,
                config                    = rp_config, 
                logger                    = self._log,
                session                   = self._session,
                umgr_staging_output_queue = self._umgr_staging_output_queue,
                update_queue              = self._update_queue)
            self._worker_list.append(worker)


        # FIXME: also spawn a separate thread/process to pull for state updates
        # from mongodb, and to issue callbacks etc from there.  This will need
        # to be replaced with some queue, eventually -- for now we pull :/
        self.start ()




    #--------------------------------------------------------------------------
    #
    def close(self):
        """Shuts down the UnitManager and its background workers in a 
        coordinated fashion.
        """

        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        for worker in self._worker_list:
            worker.stop()

        # also send a wakeup signals through all queues
        self._umgr_schedule_queue     .put(None)
        self._umgr_staging_input_queue.put(None)
        self._update_queue            .put(None)

        logger.info("Closed UnitManager %s." % str(self._uid))


    #--------------------------------------------------------------------------
    #
    @classmethod
    def _reconnect(cls, session, unit_manager_id):
        """Reconnect to an existing UnitManager.
        """

        raise NotImplementedError ("not yet implemented again")

        # FIXME!

      # uid_exists = UnitManagerController.uid_exists(
      #     db_connection=session._dbs,
      #     unit_manager_uid=unit_manager_id)
      #
      # if not uid_exists:
      #     raise BadParameter(
      #         "UnitManager with id '%s' not in database." % unit_manager_id)
      #
      # # The UnitManager object
      # obj = cls(session=session, scheduler=None, _reconnect=True)
      #
      # # Retrieve or start a worker process fo this PilotManager instance.
      # worker = session._process_registry.retrieve(unit_manager_id)
      # if worker is not None:
      #     obj._worker = worker
      # else:
      #     obj._worker = UnitManagerController(
      #         unit_manager_uid=unit_manager_id,
      #         session=session,
      #         db_connection=session._dbs,
      #         db_connection_info=session._connection_info)
      #     session._process_registry.register(unit_manager_id, obj._worker)
      #
      # # start the worker if it's not already running
      # if obj._worker.is_alive() is False:
      #     obj._worker.start()
      #
      # # Now that the worker is running (again), we can get more information
      # # about the UnitManager
      # um_data = obj._worker.get_unit_manager_data()
      #
      # obj._scheduler = get_scheduler(name=um_data['scheduler'], 
      #                                manager=obj,
      #                                session=obj._session)
      # # FIXME: we need to tell the scheduler about all the pilots...
      #
      # obj._uid = unit_manager_id
      #
      # logger.info("Reconnected to existing UnitManager %s." % str(obj))
      # return obj


    # -------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the UnitManager object.
        """

        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        return self._uid

    #--------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the unique id.
        """

        return self._uid


    # -------------------------------------------------------------------------
    #
    def add_pilots(self, pilots):
        """Associates one or more pilots with the unit manager.

        **Arguments:**

            * **pilots** [:class:`radical.pilot.ComputePilot` or list of
              :class:`radical.pilot.ComputePilot`]: The pilot objects that will be
              added to the unit manager.
        """

        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        if not isinstance(pilots, list):
            pilots = [pilots]

        # TODO: publish pilot IDs in DB

        for scheduler in self._scheduler_list :
            scheduler.add_pilot (pilots)

        self._pilot_list += pilots


    # -------------------------------------------------------------------------
    #
    def list_pilots(self):
        """Lists the UIDs of the pilots currently associated with
        the unit manager.

        **Returns:**

              * A list of :class:`radical.pilot.ComputePilot` UIDs [`string`].
        """

        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        return [pilot['_id'] for pilot in self._pilot_list]


    # -------------------------------------------------------------------------
    #
    def get_pilots(self):
        """get the pilots instances currently associated with
        the unit manager.

        **Returns:**

              * A list of :class:`radical.pilot.ComputePilot` instances.
        """

        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        return self._pilot_list

    # -------------------------------------------------------------------------
    #
    def remove_pilots(self, pilot_ids, drain=True):
        """Disassociates one or more pilots from the unit manager.

        TODO: Implement 'drain'.

        After a pilot has been removed from a unit manager, it won't process
        any of the unit manager's units anymore. Calling `remove_pilots`
        doesn't stop the pilot itself.

        **Arguments:**

            * **drain** [`boolean`]: Drain determines what happens to the units
              which are managed by the removed pilot(s). If `True`, all units
              currently assigned to the pilot are allowed to finish execution.
              If `False` (the default), then `ACTIVE` units will be canceled.
        """

        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        if not isinstance(pilot_ids, list):
            pilot_ids = [pilot_ids]

        for scheduler in self._scheduler_list :
            scheduler.remove_pilots(pilot_ids)

        for pilot_id in pilot_ids :
            for pilot in self._pilot_list[:] :
                if  pilot_id == pilot.uid :
                    self._pilot_list.remove (pilot)

        # TODO: update DB

        # FIXME:
        # if a pilot gets removed, we need to re-assign all its units to other
        # pilots.   We thus move them all into the wait queue, and call the
        # global rescheduler.  Some of the CUs might already be in final state
        # -- those are ignored (they'll be in the done_queue anyways).  We leave
        # it to the scheduling policy what happens to non-NEW CUs in the
        # wait_queue, i.e. if they get rescheduled, or if they'll raise an
        # error.


    # -------------------------------------------------------------------------
    #
    def list_units(self):
        """Returns the UIDs of the :class:`radical.pilot.ComputeUnit` managed by
        this unit manager.

        **Returns:**

              * A list of :class:`radical.pilot.ComputeUnit` UIDs [`string`].
        """

        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        return self._worker.get_compute_unit_uids()


    # -------------------------------------------------------------------------
    #
    def submit_units(self, unit_descriptions):
        """Submits on or more :class:`radical.pilot.ComputeUnit` instances to the
        unit manager.

        **Arguments:**

            * **unit_descriptions** [:class:`radical.pilot.ComputeUnitDescription`
              or list of :class:`radical.pilot.ComputeUnitDescription`]: The
              description of the compute unit instance(s) to create.

        **Returns:**

              * A list of :class:`radical.pilot.ComputeUnit` objects.

        **Raises:**

            * :class:`radical.pilot.PilotException`
        """

        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        return_list_type = True
        if not isinstance(unit_descriptions, list):
            return_list_type  = False
            unit_descriptions = [unit_descriptions]

        # we return a list of compute units
        units = list()

        for ud in unit_descriptions :

            units.append (ComputeUnit.create (unit_description=ud,
                                              unit_manager_obj=self))

        # TODO: publish units in DB
        # TODO: push to scheduling queue (as dict)

        self._unit_list += units

        if  return_list_type :
            return units
        else :
            return units[0]


        # FIXME: derive changed wait queue size
        # old_wait_queue_size = self.wait_queue_size

        # self.wait_queue_size = len(unscheduled)
        # if  old_wait_queue_size != self.wait_queue_size :
        #     self._worker.fire_manager_callback (WAIT_QUEUE_SIZE, self,
        #                                         self.wait_queue_size)


    # -------------------------------------------------------------------------
    #
    def get_units(self, unit_ids=None):
        """Returns one or more compute units identified by their IDs.

        **Arguments:**

            * **unit_ids** [`string` or `list of strings`]: The IDs of the
              compute unit objects to return.

        **Returns:**

              * A list of :class:`radical.pilot.ComputeUnit` objects.
        """

        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            return_list_type = False
            unit_ids = [unit_ids]

        # TODO: filter for uids
        units = [unit for unit in self._unit_list if unit['_id'] in unit_ids]

        if  return_list_type :
            return units
        else :
            return units[0]


    # -------------------------------------------------------------------------
    #
    def wait_units(self, unit_ids=None,
                   state=[DONE, FAILED, CANCELED],
                   timeout=None):
        """Returns when one or more :class:`radical.pilot.ComputeUnits` reach a
        specific state.

        If `unit_uids` is `None`, `wait_units` returns when **all**
        ComputeUnits reach the state defined in `state`.

        **Example**::

            # TODO -- add example

        **Arguments:**

            * **unit_uids** [`string` or `list of strings`]
              If unit_uids is set, only the ComputeUnits with the specified
              uids are considered. If unit_uids is `None` (default), all
              ComputeUnits are considered.

            * **state** [`string`]
              The state that ComputeUnits have to reach in order for the call
              to return.

              By default `wait_units` waits for the ComputeUnits to
              reach a terminal state, which can be one of the following:

              * :data:`radical.pilot.DONE`
              * :data:`radical.pilot.FAILED`
              * :data:`radical.pilot.CANCELED`

            * **timeout** [`float`]
              Timeout in seconds before the call returns regardless of Pilot
              state changes. The default value **None** waits forever.
        """

        if  not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        if not isinstance(state, list):
            state = [state]

        return_list_type = True
        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            return_list_type = False
            unit_ids = [unit_ids]

        units  = self._unit_list
        start  = time.time()
        all_ok = False
        states = list()

        while not all_ok :

            all_ok = True
            states = list()

            for unit in units :
                if  unit.state not in state :
                    all_ok = False

                states.append (unit.state)

            # check timeout
            if  (None != timeout) and (timeout <= (time.time() - start)):
                if  not all_ok :
                    logger.debug ("wait timed out: %s" % states)
                break

            # sleep a little if this cycle was idle
            if  not all_ok :
                time.sleep (0.1)

        # done waiting
        if  return_list_type :
            return states
        else :
            return states[0]


    # -------------------------------------------------------------------------
    #
    def cancel_units(self, unit_ids=None):
        """Cancel one or more :class:`radical.pilot.ComputeUnits`.

        **Arguments:**

            * **unit_ids** [`string` or `list of strings`]: The IDs of the
              compute unit objects to cancel.
        """

        if not self._uid:
            raise IncorrectState(msg="Invalid object instance.")

        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            unit_ids = [unit_ids]

        cus = self.get_units(unit_ids)
        for cu in cus:
            cu.cancel()

        # FIXME: send a CANCEL_UNIT command through all queues, and set DB
        # entry.  Whoever receives the cancel call will simply drop the unit --
        # we update the DB



    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_function, metric=UNIT_STATE, callback_data=None):

        """
        Registers a new callback function with the UnitManager.  Manager-level
        callbacks get called if the specified metric changes.  The default
        metric `UNIT_STATE` fires the callback if any of the ComputeUnits
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def callback_func(obj, value, data)

        where ``object`` is a handle to the object that triggered the callback,
        ``value`` is the metric, and ``data`` is the data provided on
        callback registration..  In the example of `UNIT_STATE` above, the
        object would be the unit in question, and the value would be the new
        state of the unit.

        Available metrics are:

          * `UNIT_STATE`: fires when the state of any of the units which are
            managed by this unit manager instance is changing.  It communicates
            the unit object instance and the units new state.

          * `WAIT_QUEUE_SIZE`: fires when the number of unscheduled units (i.e.
            of units which have not been assigned to a pilot for execution)
            changes.
        """

        if  metric not in UNIT_MANAGER_METRICS :
            raise ValueError ("Metric '%s' is not available on the unit manager" % metric)

        # self._worker.register_manager_callback(callback_function, metric, callback_data)

        # FIXME: this needs to go to our own worker thread

