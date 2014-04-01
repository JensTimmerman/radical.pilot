#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.pilot_manager
   :platform: Unix
   :synopsis: Provides the interface for the PilotManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import json
import urllib2

from radical.pilot.states import *
from radical.pilot.exceptions import *

from radical.pilot.object import Object
from radical.pilot.mpworker import PilotManagerWorker
from radical.pilot.compute_pilot import ComputePilot
from radical.pilot.utils.logger import logger
from radical.pilot.exceptions import * 

# -----------------------------------------------------------------------------
#
class PilotManager(Object):
    """A PilotManager holds :class:`radical.pilot.ComputePilot` instances that are
    submitted via the :func:`radical.pilot.PilotManager.submit_pilots` method.

    It is possible to attach one or more :ref:`chapter_machconf`
    to a PilotManager to outsource machine specific configuration
    parameters to an external configuration file.

    Each PilotManager has a unique identifier :data:`radical.pilot.PilotManager.uid`
    that can be used to re-connect to previoulsy created PilotManager in a
    given :class:`radical.pilot.Session`.

    **Example**::

        s = radical.pilot.Session(database_url=dbURL)

        pm1 = radical.pilot.PilotManager(session=s, resource_configurations=RESCONF)
        # Re-connect via the 'get()' method.
        pm2 = radical.pilot.PilotManager.get(session=s, pilot_manager_id=pm1.uid)

        # pm1 and pm2 are pointing to the same PilotManager
        assert pm1.uid == pm2.uid
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self, session, resource_configurations=None):
        """Creates a new PilotManager and attaches is to the session.

        .. note:: The `resource_configurations` (see :ref:`chapter_machconf`)
                  parameter is currently mandatory for creating a new
                  PilotManager instance.

        **Arguments:**

            * **session** [:class:`radical.pilot.Session`]:
              The session instance to use.

            * **resource_configurations** [`string` or `list of strings`]:
              A list of URLs pointing to :ref:`chapter_machconf`. Currently
              `file://`, `http://` and `https://` URLs are supported.

              If one or more resource_configurations are provided, Pilots
              submitted  via this PilotManager can access the configuration
              entries in the  files via the :class:`ComputePilotDescription`.
              For example::

                  pm = radical.pilot.PilotManager(session=s, resource_configurations="https://raw.github.com/radical-cybertools/radical.pilot/master/configs/futuregrid.json")

                  pd = radical.pilot.ComputePilotDescription()
                  pd.resource = "futuregrid.INDIA"  # defined in futuregrid.json
                  pd.cores    = 16
                  pd.runtime  = 5 # minutes

                  pilot = pm.submit_pilots(pd)

        **Returns:**

            * A new `PilotManager` object [:class:`radical.pilot.PilotManager`].

        **Raises:**
            * :class:`radical.pilot.PilotException`
        """
        self._session = session
        self._worker = None
        self._uid = None

        if resource_configurations == "~=RECON=~":
            # When we get the "~=RECON=~" keyword as resource_configurations,
            # we were called  from the 'get()' class method. In this case
            # object instantiation happens there...
            return

        ###############################
        # Create a new pilot manager. #
        ###############################

        # Donwload and parse the configuration file(s) and the content to
        # our resource dictionary.
        self._resource_cfgs = {}

        # Add 'localhost' as a built-in resource configuration
        self._resource_cfgs["localhost"] = {
            "URL":              "fork://localhost",
            "filesystem":       "file://localhost",
            "pre_bootstrap":    ["hostname", "date"],
            "task_launch_mode": "LOCAL",
        }

        if resource_configurations is not None:

            # implicit list conversion
            if not isinstance(resource_configurations, list):
                resource_configurations = [resource_configurations]

            for rcf in resource_configurations:
                try:
                    # download resource configuration file
                    response = urllib2.urlopen(rcf)
                    rcf_content = response.read()
                except urllib2.URLError, err:
                    msg = "Couln't open/download resource configuration file '%s': %s." % (rcf, str(err))
                    raise BadParameter(msg=msg)

                try:
                    # convert JSON string to dictionary and append
                    rcf_dict = json.loads(rcf_content)
                    for key, val in rcf_dict.iteritems():
                        if key in self._resource_cfgs:
                            raise BadParameter("Resource configuration entry for '%s' defined in %s is already defined." % (key, rcf))
                        self._resource_cfgs[key] = val
                except ValueError, err:
                    raise BadParameter("Couldn't parse resource configuration file '%s': %s." % (rcf, str(err)))

        # Start a worker process fo this PilotManager instance. The worker
        # process encapsulates database access, persitency et al.
        self._worker = PilotManagerWorker(
            pilot_manager_uid=None,
            pilot_manager_data={},
            db_connection=session._dbs)
        self._worker.start()

        self._uid = self._worker.pilot_manager_uid

        # Each pilot manager has a worker thread associated with it. The task
        # of the worker thread is to check and update the state of pilots, fire
        # callbacks and so on.
        self._session._pilot_manager_objects.append(self)
        self._session._process_registry.register(self._uid, self._worker)

    #--------------------------------------------------------------------------
    #
    def close(self, terminate=False):
        """Shuts down the PilotManager and its background workers in a 
        coordinated fashion.

        **Arguments:**

            * **terminate** [`bool`]: If set to True, all active pilots will 
              get canceled (default: False).

        """
        # Spit out a warning in case the object was already closed.
        if not self._uid:
            logger.warning("PilotManager object already closed.")
            return

        # If terminate is set, we cancel all pilots. 
        if terminate is True:
            self.cancel_pilots()

        # Shut down all worker processes if still active. stop() returns
        # only after a successful join(). 
        if self._worker is not None:
            # Stop the worker process
            self._worker.stop()
            # Remove worker from registry
            self._session._process_registry.remove(self._uid)

        logger.info("Closed PilotManager %s." % str(self._uid))
        self._uid = None

    #--------------------------------------------------------------------------
    #
    @classmethod
    def _reconnect(cls, session, pilot_manager_id):
        """PRIVATE: reconnect to an existing pilot manager.
        """
        uid_exists = PilotManagerWorker.uid_exists(
            db_connection=session._dbs,
            pilot_manager_uid=pilot_manager_id
        )

        if not uid_exists:
            raise BadParameter(
                "PilotManager with id '%s' not in database." % pilot_manager_id)

        obj = cls(session=session, resource_configurations="~=RECON=~")
        obj._uid = pilot_manager_id
        obj._resource_cfgs = None  # TODO: reconnect

        # Retrieve or start a worker process fo this PilotManager instance.
        worker = session._process_registry.retrieve(pilot_manager_id)
        if worker is not None:
            obj._worker = worker
        else:
            obj._worker = PilotManagerWorker(
                pilot_manager_uid=pilot_manager_id,
                pilot_manager_data={},
                db_connection=session._dbs)
            session._process_registry.register(pilot_manager_id, obj._worker)

        # start the worker if it's not already running
        if obj._worker.is_alive() is False:
            obj._worker.start()

        return obj

    # -------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        self._assert_obj_is_valid()

        object_dict = {
            'uid': self.uid
        }
        return object_dict

    # -------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        return str(self.as_dict())

    # -------------------------------------------------------------------------
    #
    def submit_pilots(self, pilot_descriptions):
        """Submits a new :class:`radical.pilot.ComputePilot` to a resource.

        **Returns:**

            * One or more :class:`radical.pilot.ComputePilot` instances
              [`list of :class:`radical.pilot.ComputePilot`].

        **Raises:**

            * :class:`radical.pilot.radical.pilotException`
        """
        # Check if the object instance is still valid.
        self._assert_obj_is_valid()

        # Implicit list conversion.
        if not isinstance(pilot_descriptions, list):
            pilot_descriptions = [pilot_descriptions]

        # Itereate over the pilot descriptions, try to create a pilot for
        # each one and append it to 'pilot_obj_list'.
        pilot_obj_list = list()

        for pilot_description in pilot_descriptions:

            if pilot_description.resource is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'resource'."
                raise BadParameter(error_msg)

            elif pilot_description.runtime is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'runtime'."
                raise BadParameter(error_msg)

            elif pilot_description.cores is None:
                error_msg = "ComputePilotDescription does not define mandatory attribute 'cores'."
                raise BadParameter(error_msg)

            # Make sure resource key is known.
            if pilot_description.resource not in self._resource_cfgs:
                error_msg = "ComputePilotDescription.resource key '%s' is not known by this PilotManager." % pilot_description.resource
                raise BadParameter(error_msg)
            else:
                resource_cfg = self._resource_cfgs[pilot_description.resource]

            # If 'default_sandbox' is defined, set it.
            if pilot_description.sandbox is not None:
                if "valid_roots" in resource_cfg:
                    is_valid = False
                    for vr in resource_cfg["valid_roots"]:
                        if pilot_description.sandbox.startswith(vr):
                            is_valid = True
                    if is_valid is False:
                        raise BadParameter("Working directory for resource '%s' defined as '%s' but needs to be rooted in %s " % (pilot_description.resource, pilot_description.sandbox, resource_cfg["valid_roots"]))

            # After the sanity checks have passed, we can register a pilot
            # startup request with the worker process and create a facade
            # object.

            pilot = ComputePilot._create(
                pilot_description=pilot_description,
                pilot_manager_obj=self)

            pilot_uid = self._worker.register_start_pilot_request(
                pilot=pilot,
                resource_config=resource_cfg,
                session=self._session)

            pilot._uid = pilot_uid

            pilot_obj_list.append(pilot)

        # Implicit return value conversion
        if len(pilot_obj_list) == 1:
            return pilot_obj_list[0]
        else:
            return pilot_obj_list

    # -------------------------------------------------------------------------
    #
    def list_pilots(self):
        """Lists the unique identifiers of all :class:`radical.pilot.ComputePilot`
        instances associated with this PilotManager

        **Returns:**

            * A list of :class:`radical.pilot.ComputePilot` uids [`string`].

        **Raises:**

            * :class:`radical.pilot.radical.pilotException`
        """
        # Check if the object instance is still valid.
        self._assert_obj_is_valid()

        # Get the pilot list from the worker
        return self._worker.list_pilots()

    # -------------------------------------------------------------------------
    #
    def get_pilots(self, pilot_ids=None):
        """Returns one or more :class:`radical.pilot.ComputePilot` instances.

        **Arguments:**

            * **pilot_uids** [`list of strings`]: If pilot_uids is set,
              only the Pilots with  the specified uids are returned. If
              pilot_uids is `None`, all Pilots are returned.

        **Returns:**

            * A list of :class:`radical.pilot.ComputePilot` objects
              [`list of :class:`radical.pilot.ComputePilot`].

        **Raises:**

            * :class:`radical.pilot.radical.pilotException`
        """
        self._assert_obj_is_valid()

        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            pilot_ids = [pilot_ids]

        pilots = ComputePilot._get(pilot_ids=pilot_ids, pilot_manager_obj=self)
        return pilots

    # -------------------------------------------------------------------------
    #
    def wait_pilots(self, pilot_ids=None,
                    state=[DONE, FAILED, CANCELED],
                    timeout=None):
        """Returns when one or more :class:`radical.pilot.ComputePilots` reach a
        specific state or when an optional timeout is reached.

        If `pilot_uids` is `None`, `wait_pilots` returns when **all** Pilots
        reach the state defined in `state`.

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

        **Raises:**

            * :class:`radical.pilot.radical.pilotException`
        """
        self._assert_obj_is_valid()

        if not isinstance(state, list):
            state = [state]

        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            pilot_ids = [pilot_ids]

        start_wait = time.time()
        all_done = False
        return_states = []

        while all_done is False:

            all_done = True

            pilots_json = self._worker.get_compute_pilot_data()

            for pilot in pilots_json:
                if pilot['info']['state'] not in state:
                    all_done = False
                    break  # leave for loop
                else:
                    return_states.append(pilot['info']['state'])

            # check timeout
            if (None != timeout) and (timeout <= (time.time() - start_wait)):
                break

            # wait a bit
            time.sleep(1)

        # done waiting
        return return_states

    # -------------------------------------------------------------------------
    #
    def cancel_pilots(self, pilot_ids=None):
        """Cancels one or more ComputePilots.

        **Arguments:**

            * **pilot_uids** [`string` or `list of strings`]
              If pilot_uids is set, only the Pilots with the specified uids are
              canceled. If pilot_uids is `None`, all Pilots are canceled.

        **Raises:**

            * :class:`radical.pilot.radical.pilotException`
        """
        # Check if the object instance is still valid.
        self._assert_obj_is_valid()

        # Implicit list conversion.
        if (not isinstance(pilot_ids, list)) and (pilot_ids is not None):
            pilot_ids = [pilot_ids]

        # Register the cancelation request with the worker.
        self._worker.register_cancel_pilots_request(pilot_ids=pilot_ids)

    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_function):
        """Registers a new callback function with the PilotManager.
        Manager-level callbacks get called if any of the ComputePilots managed
        by the PilotManager change their state.

        All callback functions need to have the same signature::

            def callback_func(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.
        """
        self._assert_obj_is_valid()

        self._worker.register_manager_callback(callback_function)