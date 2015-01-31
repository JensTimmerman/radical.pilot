
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import threading
import Queue
import time

import radical.utils       as ru
import radical.pilot       as rp
import radical.pilot.utils as rpu


# FIXME: into config
AGENT_THREADED = 'threading'
AGENT_MPROC    = 'multiprocess'

AGENT_MODE     = AGENT_THREADED


default_config_dict = {
    # directory for staging files inside the agent sandbox
    'staging_area'         : 'staging_area',
    
    # max number of cu out/err chars to push to db
    'max_io_loglength'     : 1*1024,
    
    # max time period to collec db requests into bulks (seconds)
    'bulk_collection_time' : 1.0,
    
    # time to sleep between queue polls (seconds)
    'queue_poll_sleeptime' : 0.1,
    
    # time to sleep between database polls (seconds)
    'db_poll_sleeptime'    : 0.5,
    
    # time between checks of internal state and commands from mothership (seconds)
    'heartbeat_interval'   : 10,
}
config_dict = default_config_dict



# ==============================================================================
#
class UpdateWorker(threading.Thread):
    """
    An UpdateWorker pushes CU and Pilot state updates to mongodb.  Its instances
    compete for update requests on the update_queue.  Those requests will be
    triplets of collection name, query dict, and update dict.  Update requests
    will be collected into bulks over some time (bulk_collection_time), to
    reduce number of roundtrips.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, logger, agent, session_id,
                 update_queue, mongodb_url, mongodb_name, mongodb_auth):

        threading.Thread.__init__(self)

        self.name           = name
        self._log           = logger
        self._agent         = agent
        self._session_id    = session_id
        self._update_queue  = update_queue
        self._terminate     = threading.Event()

        self._mongo_db      = rpu.get_mongodb(mongodb_url, mongodb_name, mongodb_auth)
        self._cinfo         = dict()  # collection cache

        # run worker thread
        self.start()

    # --------------------------------------------------------------------------
    #
    def stop(self):
        self._terminate.set()


    # --------------------------------------------------------------------------
    #
    def run(self):

        self._log.info("started %s.", self)

        while not self._terminate.is_set():

            # ------------------------------------------------------------------
            def timed_bulk_execute(cinfo):

                # returns number of bulks pushed (0 or 1)
                if not cinfo['bulk']:
                    return 0

                now = time.time()
                age = now - cinfo['last']

                if cinfo['bulk'] and age > config_dict['bulk_collection_time']:

                    res  = cinfo['bulk'].execute()
                    self._log.debug("bulk update result: %s", res)

                    rpu.prof('unit update bulk pushed (%d)' % len(cinfo['uids'].keys ()))
                    for uid in cinfo['uids']:
                        rpu.prof('unit update pushed (%s)' % cinfo['uids'][uid], uid=uid)

                    cinfo['last'] = now
                    cinfo['bulk'] = None
                    cinfo['uids'] = dict()
                    return 1

                else:
                    return 0
            # ------------------------------------------------------------------

            try:

                try:
                    update_request = self._update_queue.get_nowait()

                except Queue.Empty:

                    # no new requests: push any pending bulks
                    action = 0
                    for cname in self._cinfo:
                        action += timed_bulk_execute(self._cinfo[cname])

                    if not action:
                        time.sleep(config_dict['queue_poll_sleeptime'])

                    continue


                # got a new request.  Add to bulk (create as needed),
                # and push bulk if time is up.
                uid         = update_request.get('uid')
                state       = update_request.get('state', None)
                cbase       = update_request.get('cbase', '.cu')
                query_dict  = update_request.get('query', dict())
                update_dict = update_request.get('update',dict())

                rpu.prof('unit update pulled (%s)' % state, uid=uid)

                cname = self._session_id + cbase

                if not cname in self._cinfo:
                    coll =  self._mongo_db[cname]
                    self._cinfo[cname] = {
                            'coll' : coll,
                            'bulk' : None,
                            'last' : time.time(),  # time of last push
                            'uids' : dict()
                            }

                cinfo = self._cinfo[cname]

                if not cinfo['bulk']:
                    cinfo['bulk']  = coll.initialize_ordered_bulk_op()

                cinfo['uids'][uid] = state
                cinfo['bulk'].find  (query_dict) \
                             .update(update_dict)

                timed_bulk_execute(cinfo)
              # rpu.prof('unit update bulked', uid=uid)

            except Exception as e:
                self._log.exception("unit update failed (%s)", e)
                # FIXME: should we fail the pilot at this point?
                # FIXME: Are the strategies to recover?

# ------------------------------------------------------------------------------

