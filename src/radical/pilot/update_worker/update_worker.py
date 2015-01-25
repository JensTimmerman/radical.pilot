
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import threading
import Queue
import time

# needed?
import pymongo
import os
import sys
import datetime

# FIXME: into config
# max time period to collec db requests into bulks (seconds)
BULK_COLLECTION_TIME = 1.0

# FIXME: into config
# time to sleep between queue polls (seconds)
QUEUE_POLL_SLEEPTIME = 0.1


# FIXME: into config
AGENT_THREADED = 'threading'
AGENT_MPROC    = 'multiprocess'

AGENT_MODE     = AGENT_THREADED


# ------------------------------------------------------------------------------
#
# FIXME: into config
#
def get_mongodb(mongodb_url, mongodb_name, mongodb_auth):

    mongo_client = pymongo.MongoClient(mongodb_url)
    mongo_db     = mongo_client[mongodb_name]

    # do auth on username *and* password (ignore empty split results)
    auth_elems = filter(None, mongodb_auth.split(':', 1))
    if len(auth_elems) == 2:
        mongo_db.authenticate(auth_elems[0], auth_elems[1])

    return mongo_db

# ------------------------------------------------------------------------------
#
# time stamp for profiling etc.
#
def timestamp():
    # human readable absolute UTC timestamp for log entries in database
    return datetime.datetime.utcnow()

def timestamp_epoch():
    # absolute timestamp as seconds since epoch
    return float(time.time())

# absolute timestamp in seconds since epocj pointing at start of
# bootstrapper (or 'now' as fallback)
timestamp_zero = float(os.environ.get('TIME_ZERO', time.time()))

print "timestamp zero: %s" % timestamp_zero

def timestamp_now():
    # relative timestamp seconds since TIME_ZERO (start)
    return float(time.time()) - timestamp_zero


# ------------------------------------------------------------------------------
#
# profiling support
#
# If 'RADICAL_PILOT_PROFILE' is set in environment, the agent logs timed events.
#
if 'RADICAL_PILOT_PROFILE' in os.environ:
    profile_agent  = True
    profile_handle = open('agent.prof', 'a')
else:
    profile_agent  = False
    profile_handle = sys.stdout


# ------------------------------------------------------------------------------
#
profile_tags  = dict()
profile_freqs = dict()

def prof(etype, uid="", msg="", tag="", logger=None):

    # record a timed event.  We record the thread ID, the uid of the affected
    # object, a log message, event type, and a tag.  Whenever a tag changes (to
    # a non-None value), the time since the last tag change is added.  This can
    # be used to derive, for example, the duration which a uid spent in
    # a certain state.  Time intervals between the same tags (but different
    # uids) are recorded, too.
    #
    # TODO: should this move to utils?  Or at least RP utils, so that we can
    # also use it for the application side?

    if logger:
        logger("%s -- %s (%s): %s", etype, msg, uid, tag)


    if not profile_agent:
        return


    logged = False
    now    = timestamp_now()

    if   AGENT_MODE == AGENT_THREADED : tid = threading.current_thread().name
    elif AGENT_MODE == AGENT_MPROC    : tid = os.getpid ()

    if uid and tag:

        if not uid in profile_tags:
            profile_tags[uid] = {'tag'  : "",
                                 'time' : 0.0 }

        old_tag = profile_tags[uid]['tag']

        if tag != old_tag:

            tagged_time = now - profile_tags[uid]['time']

            profile_tags[uid]['tag' ] = tag
            profile_tags[uid]['time'] = timestamp_now()

            profile_handle.write("> %12.4f : %-20s : %12.4f : %-17s : %-24s : %-40s : %s\n" \
                                 % (tagged_time, tag, now, tid, uid, etype, msg))
            logged = True


            if not tag in profile_freqs:
                profile_freqs[tag] = {'last'  : now,
                                      'diffs' : list()}
            else:
                diff = now - profile_freqs[tag]['last']
                profile_freqs[tag]['diffs'].append(diff)
                profile_freqs[tag]['last' ] = now

              # freq = sum(profile_freqs[tag]['diffs']) / len(profile_freqs[tag]['diffs'])
              #
              # profile_handle.write("> %12s : %-20.4f : %12s : %-17s : %-24s : %-40s : %s\n" \
              #                      % ('frequency', freq, '', '', '', '', ''))



    if not logged:
        profile_handle.write("  %12s : %-20s : %12.4f : %-17s : %-24s : %-40s : %s\n" \
                             % (' ' , ' ', now, tid, uid, etype, msg))
  
    # FIXME: disable flush on production runs
    profile_handle.flush()





# ==============================================================================
#
class UpdateWorker(threading.Thread):
    """
    An UpdateWorker pushes CU and Pilot state updates to mongodb.  Its instances
    compete for update requests on the update_queue.  Those requests will be
    triplets of collection name, query dict, and update dict.  Update requests
    will be collected into bulks over some time (BULK_COLLECTION_TIME), to
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

        self._mongo_db      = get_mongodb(mongodb_url, mongodb_name, mongodb_auth)
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

                if cinfo['bulk'] and age > BULK_COLLECTION_TIME:

                    res  = cinfo['bulk'].execute()
                    self._log.debug("bulk update result: %s", res)

                    prof('state update bulk pushed (%d)' % len(cinfo['uids'].keys ()))
                    for uid in cinfo['uids']:
                        prof('state update pushed (%s)' % cinfo['uids'][uid], uid=uid)

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
                        time.sleep(QUEUE_POLL_SLEEPTIME)

                    continue


                # got a new request.  Add to bulk (create as needed),
                # and push bulk if time is up.
                uid         = update_request.get('uid')
                state       = update_request.get('state', None)
                cbase       = update_request.get('cbase', '.cu')
                query_dict  = update_request.get('query', dict())
                update_dict = update_request.get('update',dict())

                prof('state update pulled (%s)' % state, uid=uid)

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
              # prof('state update bulked', uid=uid)

            except Exception as e:
                self._log.exception("state update failed (%s)", e)
                # FIXME: should we fail the pilot at this point?
                # FIXME: Are the strategies to recover?

# ------------------------------------------------------------------------------

