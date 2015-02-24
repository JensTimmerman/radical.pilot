#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.compute_unit
   :platform: Unix
   :synopsis: Implementation of the ComputeUnit class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import copy
import time

import radical.utils as ru

from radical.pilot.utils.logger import logger

from radical.pilot.states       import *
from radical.pilot.logentry     import *
from radical.pilot.exceptions   import *

from radical.pilot.staging_directives import expand_staging_directives
from radical.pilot.db.database        import COMMAND_CANCEL_COMPUTE_UNIT


# -----------------------------------------------------------------------------
#
class ComputeUnit(object):
    """A ComputeUnit represent a 'task' that is executed on a ComputePilot.
    ComputeUnits allow to control and query the state of this task.

    .. note:: A ComputeUnit cannot be created directly. The factory method
              :meth:`radical.pilot.UnitManager.submit_units` has to be used instead.

                **Example**::

                      um = radical.pilot.UnitManager(session=s)
                      ud = radical.pilot.ComputeUnitDescription()
                      ud.executable = "/bin/date"
                      ud.cores      = 1

                      unit = umgr.submit_units(ud)
    """

    # -------------------------------------------------------------------------
    #
    def __init__(ud=None, uid=None, umgr=None):
        """ 
        Create a new Compute Unit, either by passing a ComputeUnitDescritption,
        or be passing a unit ID, to reconnect to a previously created and
        submitted unit.
        """

        self._callbacks   = list()
        self._data        = dict()
        self._data['pid'] = None
        self._umgr        = umgr

        if not ud and not uid:
            raise ValueError ("need either compute unit description of ID for object creation")

        if ud and uid:
            raise ValueError ("need either compute unit description of ID for object creation, not both")

        # sanity check on description
        if not (ud.get('executable') or ud.get('kernel')):
            raise PilotException ("ComputeUnitDescription needs an executable or application kernel")

        # 'static' members
        if ud:
            self._data['_id']         = ru.generate_id('unit.%(counter)06d', ru.ID_CUSTOM)
            self._data['description'] = copy.deepcopy (ud)  # keep the original ud reusable

            # expand any staging directives
            expand_staging_directives(self._data['description'],  logger)

        else:
            pass
            # FIXME: reconnect!
          # self._description = None
          # self._uid         = None

        self._uid         = self._description['_id']
        self._name        = self._description.get ('name')



    #--------------------------------------------------------------------------
    #
    def __str__(self):

        return "%s (%-15s: %s %s)" % (self.uid, self.state, 
                                      self.description.executable, 
                                      " ".join (self.description.arguments))


    # -------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the unit's unique identifier.

        The uid identifies the ComputeUnit within a :class:`UnitManager` and
        can be used to retrieve an existing ComputeUnit.

        **Returns:**
            * A unique identifier (string).
        """

        return self._data['_id']


    # -------------------------------------------------------------------------
    #
    @property
    def name(self):
        """Returns the unit's application specified name.

        **Returns:**
            * A name (string).
        """

        return self._data['name']


    # -------------------------------------------------------------------------
    #
    @property
    def working_directory(self):
        """Returns the full working directory URL of this ComputeUnit.

        **Returns:**
            * A URL
        """

        return self._data['workdir']


    # -------------------------------------------------------------------------
    #
    @property
    def pilot_id(self):
        """Returns the pilot_id of this ComputeUnit.
        """

        return self._data['pid']


    # -------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        """Returns a snapshot of the executable's STDOUT stream.

        If this property is queried before the ComputeUnit has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can become very inefficient for large data volumes.
        """

        # note: we may want to make this one pull based...
        return self._data['stdout']


    # -------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        """Returns a snapshot of the executable's STDERR stream.

        If this property is queried before the ComputeUnit has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can become very inefficient for large data volumes.
        """

        # note: we may want to make this one pull based...
        return self._data['stderr']


    # -------------------------------------------------------------------------
    #
    @property
    def description(self):
        """Returns the ComputeUnitDescription the ComputeUnit was started with.
        """

        return self._data['description']


    # -------------------------------------------------------------------------
    #
    @property
    def state(self):
        """Returns the current state of the ComputeUnit.
        """

        return self._data['state']


    # -------------------------------------------------------------------------
    #
    @property
    def state_history(self):
        """Returns the complete state history of the ComputeUnit.
        """

        ret = list()
        for state in self._data['statehistory']:
            ret.append(State(state=state["state"], timestamp=state["timestamp"]))

        return ret


    # -------------------------------------------------------------------------
    #
    @property
    def callback_history(self):
        """Returns the complete callback history of the ComputeUnit.
        """

        ret = list()
        for state in self._data['callbackhistory']:
            ret.append(State(state=state["state"], timestamp=state["timestamp"]))

        return ret


    # -------------------------------------------------------------------------
    #
    @property
    def exit_code(self):
        """Returns the exit code of the ComputeUnit.

        If this property is queried before the ComputeUnit has reached
        'DONE' or 'FAILED' state it will return None.
        """

        return self._data['exit_code']


    # -------------------------------------------------------------------------
    #
    @property
    def log(self):
        """Returns the logs of the ComputeUnit.
        """

        ret = list()
        for log in self._data['log']:
            ret.append(Logentry.from_dict (log))

        return ret


    # -------------------------------------------------------------------------
    #
    @property
    def submitted(self):
        """ Returns the time the ComputeUnit was submitted.
        """
        return self._data['submitted']

    # -------------------------------------------------------------------------
    #
    @property
    def started(self):
        """ Returns the time the ComputeUnit was started on the backend.
        """

        return self._data['started']

    # -------------------------------------------------------------------------
    #
    @property
    def finished(self):
        """ Returns the time the ComputeUnit was stopped.
        """

        return self._data['finished']

    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_func, callback_data=None):
        """Registers a callback function that is triggered every time the
        ComputeUnit's state changes.

        All callback functions need to have the same signature::

            def callback_func(obj, state, data=None)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.
        """

        self._callbacks.append ([callback_func, callback_data])


    # -------------------------------------------------------------------------
    #
    def wait(self, state=[DONE, FAILED, CANCELED], timeout=None):
        """Returns when the ComputeUnit reaches a specific state or
        when an optional timeout is reached.

        **Arguments:**

            * **state** [`list of strings`]
              The state(s) that compute unit has to reach in order for the
              call to return.

              By default `wait` waits for the compute unit to reach
              a **terminal** state, which can be one of the following:

              * :data:`radical.pilot.DONE`
              * :data:`radical.pilot.FAILED`
              * :data:`radical.pilot.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless
              whether the compute unit has reached the desired state or not.
              The default value **None** never times out.

        **Raises:**
        """

        if not isinstance(state, list):
            state = [state]

        start_wait = time.time()
        new_state  = self._data['state']
        while True:

            if new_state in state:
                break

            if new_state in [DONE, FAILED, CANCELED]:
                break

            if timeout and (timeout <= (time.time() - start_wait)):
                break

            time.sleep(0.1)
            new_state = self.state


        return new_state


    # -------------------------------------------------------------------------
    #
    def cancel(self):
        """Cancel the ComputeUnit.
        """

        # FIXME: use updater

        pid = self._data['pid']

        if self.state in [DONE, FAILED, CANCELED]:
            # nothing to do
            logger.debug("Compute unit %s has state %s, can't cancel any longer." % (self._uid, self.state))

        elif self.state in [NEW, UNSCHEDULED, PENDING_INPUT_STAGING]:
            logger.debug("Compute unit %s has state %s, going to prevent from starting." % (self._uid, self.state))
            self._umgr._session._dbs.set_compute_unit_state(self._uid, CANCELED, ["Received Cancel"])

        elif self.state == STAGING_INPUT:
            logger.debug("Compute unit %s has state %s, will cancel the transfer." % (self._uid, self.state))
            self._umgr._session._dbs.set_compute_unit_state(self._uid, CANCELED, ["Received Cancel"])

        elif self.state in [PENDING_EXECUTION, SCHEDULING]:
            logger.debug("Compute unit %s has state %s, will abort start-up." % (self._uid, self.state))
            self._umgr._session._dbs.set_compute_unit_state(self._uid, CANCELED, ["Received Cancel"])

        elif self.state == EXECUTING:
            logger.debug("Compute unit %s has state %s, will terminate the task." % (self._uid, self.state))
            self._umgr._session._dbs.send_command_to_pilot(cmd=COMMAND_CANCEL_COMPUTE_UNIT, arg=self.uid, pilot_ids=pid)

        elif self.state == PENDING_OUTPUT_STAGING:
            logger.debug("Compute unit %s has state %s, will abort the transfer." % (self._uid, self.state))
            self._umgr._session._dbs.set_compute_unit_state(self._uid, CANCELED, ["Received Cancel"])

        elif self.state == STAGING_OUTPUT:
            logger.debug("Compute unit %s has state %s, will cancel the transfer." % (self._uid, self.state))
            self._umgr._session._dbs.set_compute_unit_state(self._uid, CANCELED, ["Received Cancel"])

        else:
            raise IncorrectState("Unknown Compute Unit state: %s, cannot cancel" % self.state)


# ------------------------------------------------------------------------------

