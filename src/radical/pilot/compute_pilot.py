#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.compute_pilot
   :platform: Unix
   :synopsis: Provides the interface for the ComputePilot class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga

import radical.utils as ru

from radical.pilot.utils.logger import logger

from radical.pilot.states       import *
from radical.pilot.logentry     import *
from radical.pilot.exceptions   import *


# -----------------------------------------------------------------------------
#
class ComputePilot (object):
    """A ComputePilot represent a resource overlay on a local or remote
       resource.

    .. note:: A ComputePilot cannot be created directly. The factory method
              :meth:`radical.pilot.PilotManager.submit_pilots` has to be used instead.

                **Example**::

                      pm = radical.pilot.PilotManager(session=s)
                      pd = radical.pilot.ComputePilotDescription()
                      pd.resource = "local.localhost"
                      pd.cores    = 2
                      pd.runtime  = 5 # minutes

                      pilot = pm.submit_pilots(pd)
    """

    # -------------------------------------------------------------------------
    #
    def __init__(pd=None, pid=None, pmgr=None):
        """ 
        Create a new Compute Pilot, either by passing a ComputePilotDescritption,
        or be passing a pilot ID, to reconnect to a previously created and
        submitted pilot.
        """

        self._callbacks     = list()
        self._data          = dict()
        self._pmgr          = pmgr

        if not pd and not pid:
            raise ValueError ("need either compute pilot description of ID for object creation")

        if pd and pid:
            raise ValueError ("need either compute pilot description of ID for object creation, not both")

        # sanity check on description
        if not pd.get('resource'):
            raise PilotException ("ComputePilotDescription needs a resource")

        # 'static' members
        if pd:
            self._data['_id']         = ru.generate_id('pilot.%(counter)02d', ru.ID_CUSTOM)
            self._data['description'] = copy.deepcopy (pd)  # keep the original pd reusable

        else:
            pass
            # FIXME: reconnect!
          # self._description = None
          # self._pid         = None

        self._pid         = self._description['_id']
      # self._name        = self._description.get ('name')


    # -------------------------------------------------------------------------
    #
    def __str__(self):

        return "%s (%-15s: %s)" % (self.pid, self.state, 
                                   self.description.resource)


    # -------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the Pilot's unique identifier.

        The uid identifies the Pilot within the :class:`PilotManager` and
        can be used to retrieve an existing Pilot.

        **Returns:**
            * A unique identifier (string).
        """
        return self._pid


    # -------------------------------------------------------------------------
    #
    @property
    def description(self):
        """Returns the pilot description the pilot was started with.
        """
        return self._description


    # -------------------------------------------------------------------------
    #
    @property
    def sandbox(self):
        """Returns the full sandbox directory URL of this ComputePilot.

        **Returns:**
            * A URL
        """
        if not self._uid:
            return None

        return self._data['sandbox']


    # FIXME: from here
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
    def state(self):
        """Returns the current state of the pilot.
        """

        return self._data['state']


    # -------------------------------------------------------------------------
    #
    @property
    def state_history(self):
        """Returns the complete state history of the pilot.
        """
        if not self._uid:
            return None

        states = []

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        for state in pilot_json['statehistory']:
            states.append(State(state=state["state"], timestamp=state["timestamp"]))

        return states

    # -------------------------------------------------------------------------
    #
    @property
    def callback_history(self):
        """Returns the complete callback history of the pilot.
        """
        if not self._uid:
            return None

        callbacks = []

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        if 'callbackhostory' in pilot_json:
            for callback in pilot_json['callbackhistory']:
                callbacks.append(State(state=callback["state"], timestamp=callback["timestamp"]))

        return callbacks

    # -------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        """Returns the stdout of the pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json.get ('stdout')

    # -------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        """Returns the stderr of the pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json.get ('stderr')

    # -------------------------------------------------------------------------
    #
    @property
    def logfile(self):
        """Returns the logfile of the pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json.get ('logfile')

    # -------------------------------------------------------------------------
    #
    @property
    def log(self):
        """Returns the log of the pilot.
        """
        if not self._uid:
            return None

        logs = []

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        for log in pilot_json['log']:
            logs.append (Logentry.from_dict (log))

        return logs

    # -------------------------------------------------------------------------
    #
    @property
    def resource_detail(self):
        """Returns the names of the nodes managed by the pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        resource_details = {
            'nodes':          pilot_json['nodes'],
            'cores_per_node': pilot_json['cores_per_node']
        }
        return resource_details

    # -------------------------------------------------------------------------
    #
    @property
    def pilot_manager(self):
        """ Returns the pilot manager object for this pilot.
        """
        return self._manager

    # -------------------------------------------------------------------------
    #
    @property
    def unit_managers(self):
        """ Returns the unit manager object UIDs for this pilot.
        """
        if not self._uid:
            return None

        raise NotImplementedError("Not Implemented")

    # -------------------------------------------------------------------------
    #
    @property
    def units(self):
        """ Returns the units scheduled for this pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            return None

        raise NotImplementedError("Not Implemented")

    # -------------------------------------------------------------------------
    #
    @property
    def submitted(self):
        """ Returns the time the pilot was submitted.
        """
        # Check if this instance is valid
        if not self._uid:
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json['submitted']

    # -------------------------------------------------------------------------
    #
    @property
    def started(self):
        """ Returns the time the pilot was started on the backend.
        """
        if not self._uid:
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json['started']

    # -------------------------------------------------------------------------
    #
    @property
    def finished(self):
        """ Returns the time the pilot was stopped.
        """
        if not self._uid:
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json['finished']

    # -------------------------------------------------------------------------
    #
    @property
    def resource(self):
        """ Returns the resource.
        """
        if not self._uid:
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json['description']['resource']

    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_func, callback_data=None):
        """Registers a callback function that is triggered every time the
        ComputePilot's state changes.

        All callback functions need to have the same signature::

            def callback_func(obj, state, data)

        where ``object`` is a handle to the object that triggered the callback,
        ``state`` is the new state of that object, and ``data`` is the data
        passed on callback registration.
        """
        self._worker.register_pilot_callback(self, callback_func, callback_data)

    # -------------------------------------------------------------------------
    #
    def wait(self, state=[DONE, FAILED, CANCELED],
             timeout=None):
        """Returns when the pilot reaches a specific state or
        when an optional timeout is reached.

        **Arguments:**

            * **state** [`list of strings`]
              The state(s) that Pilot has to reach in order for the
              call to return.

              By default `wait` waits for the Pilot to reach
              a **terminal** state, which can be one of the following:

              * :data:`radical.pilot.states.DONE`
              * :data:`radical.pilot.states.FAILED`
              * :data:`radical.pilot.states.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless
              whether the Pilot has reached the desired state or not.
              The default value **None** never times out.

        **Raises:**

            * :class:`radical.pilot.exceptions.radical.pilotException` if the state of
              the pilot cannot be determined.
        """
        # Check if this instance is valid
        if not self._uid:
            raise IncorrectState("Invalid instance.")

        if not isinstance(state, list):
            state = [state]

        start_wait = time.time()
        # the self.state property pulls the state from the back end.
        new_state = self.state

        while new_state not in state:
            time.sleep(0.1)
            new_state = self.state

            if (timeout is not None) and (timeout <= (time.time() - start_wait)):
                break

        # done waiting -- return the state
        return new_state

    # -------------------------------------------------------------------------
    #
    def cancel(self):
        """Sends sends a termination request to the pilot.

        **Raises:**

            * :class:`radical.pilot.radical.pilotException` if the termination
              request cannot be fulfilled.
        """
        # Check if this instance is valid
        if not self._uid:
            raise IncorrectState(msg="Invalid instance.")

        if self.state in [DONE, FAILED, CANCELED]:
            # nothing to do as we are already in a terminal state
            return

        # now we can send a 'cancel' command to the pilot.
        self._manager.cancel_pilots(self.uid)

# ------------------------------------------------------------------------------

