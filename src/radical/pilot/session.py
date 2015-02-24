#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.session
   :platform: Unix
   :synopsis: Implementation of the Session class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 
import bson
import glob
import copy
import threading
import multiprocessing

import saga
import radical.utils as ru

from radical.pilot.unit_manager    import UnitManager
from radical.pilot.pilot_manager   import PilotManager
from radical.pilot.utils.logger    import logger
from radical.pilot.resource_config import ResourceConfig
from radical.pilot.exceptions      import PilotException
from radical.pilot.updater         import UnitUpdater
from radical.pilot.states          import *

from radical.pilot.db              import Session as dbSession
from radical.pilot.db              import DBException


SESSION_THREADS   = 'threading'
SESSION_PROCESSES = 'multiprocessing'

SESSION_MODE      = SESSION_THREADS

if SESSION_MODE == SESSION_THREADS:
    COMPONENT_MODE = threading
    COMPONENT_TYPE = threading.Thread
    QUEUE_TYPE     = multiprocessing.Queue
elif SESSION_MODE == SESSION_PROCESSES:
    COMPONENT_MODE = multiprocessing
    COMPONENT_TYPE = multiprocessing.Process
    QUEUE_TYPE     = multiprocessing.Queue

# component IDs

UPDATER = 'Updater'

# Number of worker threads
NUMBER_OF_WORKERS = {
        UPDATER  :   1
}

# factor by which the number of units are increased at a certain step.  Value of
# '1' will leave the units unchanged.  Any blowup will leave on unit as the
# original, and will then create clones with an changed unit ID (see blowup()).
BLOWUP_FACTOR = {
        UPDATER  :   1
}

# flag to drop all blown-up units at some point in the pipeline.  The units
# with the original IDs will again be left untouched, but all other units are
# silently discarded.
DROP_CLONES = {
        UPDATER  : True
}

# ------------------------------------------------------------------------------
#
# config flags 
#
# FIXME: move to some RP config
#
rp_config = {
    # max time period to collec db requests into bulks (seconds)
    'bulk_collection_time' : 1.0,
    
    # time to sleep between queue polls (seconds)
    'queue_poll_sleeptime' : 0.1,
    
    # time to sleep between database polls (seconds)
    'db_poll_sleeptime'    : 0.1,
    
    # time between checks of internal state and commands from mothership (seconds)
    'heartbeat_interval'   : 10,
}
rp_config['blowup_factor']     = BLOWUP_FACTOR
rp_config['drop_clones']       = DROP_CLONES
rp_config['number_of_workers'] = NUMBER_OF_WORKERS


# ------------------------------------------------------------------------------
#
class Session(saga.Session):
    """A Session encapsulates a RADICAL-Pilot instance and is the *root* object
    for all other RADICAL-Pilot objects. 

    A Session holds :class:`radical.pilot.PilotManager` and :class:`radical.pilot.UnitManager`
    instances which in turn hold  :class:`radical.pilot.Pilot` and
    :class:`radical.pilot.ComputeUnit` instances.

    Each Session has a unique identifier :data:`radical.pilot.Session.uid` that can be
    used to re-connect to a RADICAL-Pilot instance in the database.

    **Example**::

        s1 = radical.pilot.Session(database_url=DBURL)
        s2 = radical.pilot.Session(database_url=DBURL, uid=s1.uid)

        # s1 and s2 are pointing to the same session
        assert s1.uid == s2.uid
    """

    #---------------------------------------------------------------------------
    #
    def __init__(self, database_url=None, uid=None, name=None):
        """Creates a new or reconnects to an exising session.

        If called without a uid, a new Session instance is created and 
        stored in the database. If uid is set, an existing session is 
        retrieved from the database. 

        **Arguments:**
            * **database_url** (`string`): The MongoDB URL.  If none is given,
              RP uses the environment variable RADICAL_PILOT_DBURL.  If that is
              not set, an error will be raises.

            * **uid** (`string`): If uid is set, we try 
              re-connect to an existing session instead of creating a new one.

            * **name** (`string`): An optional human readable name.

        **Returns:**
            * A new Session instance.

        **Raises:**
            * :class:`radical.pilot.DatabaseError`

        """

        # init the base class inits
        saga.Session.__init__(self)

        # set up own state
        self._state  = ACTIVE
        self._config = rp_config   # FIXME: get from somewhere...

        # before doing anything else, set up the debug helper for the lifetime
        # of the session.
        self._debug_helper = ru.DebugHelper()
        self._log          = logger

        # dictionaries holding all manager objects created during the session
        # (on shutdown, we need to signal them all)
        self._pilot_manager_objects = list()
        self._unit_manager_objects  = list()

        # database handles
        if not database_url:
            database_url = os.environ.get("RADICAL_PILOT_DBURL", None)

        if not database_url:
            raise PilotException("no database URL (set RADICAL_PILOT_DBURL)")  

        self._database_url = ru.Url(database_url)

        # if the database url contains no path element (ie. database name), we 
        # set the default to 'radicalpilot'
        if not (self._database_url.path and len(self._database_url.path) > 1):
            self._database_url.path = 'radicalpilot'

        self._log.info("using database url %s", self._database_url)

        # Loading all "default" resource configurations
        self._resource_configs = dict()

        module_path   = os.path.dirname(os.path.abspath(__file__))
        default_cfgs  = "%s/configs/*.json" % module_path
        config_files  = glob.glob(default_cfgs)

        for config_file in config_files:

            try:
                rcs = ResourceConfig.from_file(config_file)
            except Exception as e:
                self._log.error("skip config file %s: %s", config_file, e)
                continue

            for rc in rcs:
                self._log.info("Loaded resource configurations for %s", rc)
                self._resource_configs[rc] = rcs[rc].as_dict() 

        user_cfgs     = "%s/.radical/pilot/configs/*.json" % os.environ.get('HOME')
        config_files  = glob.glob(user_cfgs)


        for config_file in config_files:

            try:
                rcs = ResourceConfig.from_file(config_file)
            except Exception as e:
                self._log.error("skip config file %s: %s", config_file, e)
                continue

            for rc in rcs:
                self._log.info("Loaded resource configurations for %s", rc)

                if rc in self._resource_configs:
                    # config exists -- merge user config into it
                    ru.dict_merge(self._resource_configs[rc],
                                  rcs[rc].as_dict(),
                                  policy='overwrite')
                else:
                    # new config -- add as is
                    self._resource_configs[rc] = rcs[rc].as_dict() 

        default_aliases = "%s/configs/aliases.json" % module_path
        self._resource_aliases = ru.read_json_str(default_aliases)['aliases']

        # ----------------------------------------------------------------------
        #
        if uid:
            # FIXME: reconnect
            raise NotImplementedError('oops')

        else:
            try:
                self._connected  = None

                if name: self._uid = name
                else   : self._uid = ru.generate_id('rp.session', mode=ru.ID_PRIVATE)

                self._data = UnitUpdater.insert_session (self._database_url, 
                        self._uid, self._config)

                self._log.info("New Session created (%s)", self)

            except Exception, ex:
                self._log.exception('session create failed')
                raise PilotException("Couldn't create new session (database URL '%s' incorrect?): %s" \
                                % (self._database_url, ex))  



        self._worker_list  = list()
        self._update_queue = QUEUE_TYPE()

        # spawn updater threads/procs/...
        for n in range(rp_config['number_of_workers'][UPDATER]):
            worker = UnitUpdater(
                name         = "Updater-%d" % n,
                config       = rp_config, 
                logger       = self._log,
                session_id   = self._uid,
                update_queue = self._update_queue,
                dburl        = self._database_url)
            self._worker_list.append(worker)

        self._state = ACTIVE


    #---------------------------------------------------------------------------
    #
    def __del__(self):

        self.close(cleanup=True, terminate=True)


    #---------------------------------------------------------------------------
    #
    def close(self, cleanup=True, terminate=True):
        """Closes the session.

        All subsequent attempts access objects attached to the session will 
        result in an error. If cleanup is set to True (default) the session
        data is removed from the database.

        **Arguments:**
            * **cleanup** (`bool`): Remove session from MongoDB (implies * terminate)
            * **terminate** (`bool`): Shut down all pilots associated with the session. 

        **Raises:**
            * :class:`radical.pilot.IncorrectState` if the session is closed
              or doesn't exist. 
        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")
        
        self._log.debug("session %s closing", self._uid)

        uid = self._uid

        if not self._uid:
            self._log.error("Session object already closed.")
            return

        if cleanup:
            # cleanup implies terminate
            terminate = True

        for pmgr in self._pilot_manager_objects:
            self._log.debug("session %s closes   pmgr   %s", self._uid, pmgr._uid)
            pmgr.close(terminate=terminate)
            self._log.debug("session %s closed   pmgr   %s", self._uid, pmgr._uid)

        for umgr in self._unit_manager_objects:
            self._log.debug("session %s closes   umgr   %s", self._uid, umgr._uid)
            umgr.close(termindate=terminate)
            self._log.debug("session %s closed   umgr   %s", self._uid, umgr._uid)

        if cleanup:
            self._data = UnitUpdater.delete_session (self._database_url, self._uid)
            self._log.info("session %s deleted", self._uid)

        self._state = CANCELED


        self._log.debug("session %s closed", self._uid)


    #---------------------------------------------------------------------------
    #
    def __str__(self):

        return self._uid


    #---------------------------------------------------------------------------
    #
    @property
    def uid(self):

        return self._uid


    #---------------------------------------------------------------------------
    #
    @property
    def state(self):

        return self._state


    #---------------------------------------------------------------------------
    #
    @property
    def created(self):
        """Returns the UTC date and time the session was created.
        """
        return self._data['created']


    #---------------------------------------------------------------------------
    #
    @property
    def modified(self):
        """Returns the UTC date and time the session was last modified.
        """
        return self._data['modified']


    #---------------------------------------------------------------------------
    #
    @property
    def deleted(self):
        """Returns the UTC date and time the session was deleted.
        """
        return self._data['deleted']


    #---------------------------------------------------------------------------
    #
    def list_pilot_managers(self):
        """Lists the unique identifiers of all :class:`radical.pilot.PilotManager` 
        instances associated with this session.

        **Example**::

            s = radical.pilot.Session(database_url=DBURL)
            for pm_uid in s.list_pilot_managers():
                pm = radical.pilot.PilotManager(session=s, pilot_manager_uid=pm_uid) 

        **Returns:**
            * A list of :class:`radical.pilot.PilotManager` uids (`list` oif strings`).
        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")
        
        return self._data['pmgr']


    # --------------------------------------------------------------------------
    #
    def get_pilot_managers(self, uids=None):
        """ Re-connects to and returns one or more existing PilotManager(s).

        **Arguments:**

            * **session** [:class:`radical.pilot.Session`]: 
              The session instance to use.

            * **pilot_manager_uid** [`string`]: 
              The unique identifier of the PilotManager we want 
              to re-connect to.

        **Returns:**

            * One or more new [:class:`radical.pilot.PilotManager`] objects.
        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")
        
        return_scalar = False

        if not uids:
            uids = self.list_pilot_managers ()

        elif not isinstance(uids, list):
            uids = [uids]
            return_scalar = True

        ret = []

        for uid in uids:
            ret.append(PilotManager(session=self, uid=uid))

        if return_scalar: return ret[0]
        else            : return ret


    #---------------------------------------------------------------------------
    #
    def list_unit_managers(self):
        """Lists the unique identifiers of all :class:`radical.pilot.UnitManager` 
        instances associated with this session.

        **Example**::

            s = radical.pilot.Session(database_url=DBURL)
            for pm_uid in s.list_unit_managers():
                pm = radical.pilot.PilotManager(session=s, pilot_manager_uid=pm_uid) 

        **Returns:**
            * A list of :class:`radical.pilot.UnitManager` uids (`list` of `strings`).
        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")
        
        return self._data['umgr']


    # --------------------------------------------------------------------------
    #
    def get_unit_managers(self, uids=None):
        """ Re-connects to and returns one or more existing UnitManager(s).

        **Arguments:**

            * **session** [:class:`radical.pilot.Session`]: 
              The session instance to use.

            * **pilot_manager_uid** [`string`]: 
              The unique identifier of the PilotManager we want 
              to re-connect to.

        **Returns:**

            * One or more new [:class:`radical.pilot.PilotManager`] objects.
        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")
        
        return_scalar = False

        if uids is None:
            uids = self.list_unit_managers()

        elif not isinstance(uids, list):
            uids = [uids]
            return_scalar = True

        ret = []

        for uid in uids:
            ret.append (UnitManager (session=self, uid=uid))

        if return_scalar: return ret[0]
        else            : return ret


    # -------------------------------------------------------------------------
    #
    def add_resource_config(self, resource_config):
        """Adds a new :class:`radical.pilot.ResourceConfig` to the PilotManager's 
           dictionary of known resources, or accept a string which points to
           a configuration file.

           For example::

                  rc = radical.pilot.ResourceConfig
                  rc.name                 = "mycluster"
                  rc.job_manager_endpoint = "ssh+pbs://mycluster
                  rc.filesystem_endpoint  = "sftp://mycluster
                  rc.default_queue        = "private"
                  rc.bootstrapper         = "default_bootstrapper.sh"

                  pm = radical.pilot.PilotManager(session=s)
                  pm.add_resource_config(rc)

                  pd = radical.pilot.ComputePilotDescription()
                  pd.resource = "mycluster"
                  pd.cores    = 16
                  pd.runtime  = 5 # minutes

                  pilot = pm.submit_pilots(pd)
        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")
        
        if isinstance(resource_config, basestring):

            # let exceptions fall through
            rcs = ResourceConfig.from_file(resource_config)

            for rc in rcs:
                self._log.info("Loaded resource configurations for %s", rc)
                self._resource_configs[rc] = rcs[rc].as_dict() 

        else:
            self._resource_configs [resource_config.name] = resource_config.as_dict()


    # -------------------------------------------------------------------------
    #
    def get_resource_config(self, resource_key, schema=None):
        """Returns a dictionary of the requested resource config
        """

        if self._state in FINAL:
            raise IncorrectState(msg="Invalid object instance.")
        

        if resource_key in self._resource_aliases:
            self._log.warning("using alias '%s' for deprecated resource key '%s'",
                              self._resource_aliases[resource_key], resource_key)
            resource_key = self._resource_aliases[resource_key]

        if resource_key not in self._resource_configs:
            error_msg = "Resource key '%s' is not known." % resource_key
            raise PilotException(error_msg)

        resource_cfg = copy.deepcopy(self._resource_configs[resource_key])

        if not schema:
            if 'schemas' in resource_cfg:
                schema = resource_cfg['schemas'][0]

        if schema not in resource_cfg:
            raise RuntimeError("schema %s unknown for resource %s" \
                             % (schema, resource_key))

        for key in resource_cfg[schema]:
            # merge schema specific resource keys into the
            # resource config
            resource_cfg[key] = resource_cfg[schema][key]

        return resource_cfg


# ------------------------------------------------------------------------------

