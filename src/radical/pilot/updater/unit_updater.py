
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import threading
import Queue
import time

import radical.utils       as ru

from   radical.pilot.utils import get_mongodb, prof, blowup, timestamp


# FIXME: into config
AGENT_THREADED = 'threading'
AGENT_MPROC    = 'multiprocess'

AGENT_MODE     = AGENT_THREADED



# ==============================================================================
#
class UnitUpdater(threading.Thread):
    """
    An UnitUpdater pushes CU state updates to mongodb.  Its instances
    compete for update requests on the update_queue.  Those requests will be
    triplets of collection name, query dict, and update dict.  Update requests
    will be collected into bulks over some time (bulk_collection_time), to
    reduce number of roundtrips.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, config, logger, session_id, update_queue, dburl):

        threading.Thread.__init__(self)

        self.name           = name
        self._config        = config
        self._log           = logger
        self._session_id    = session_id
        self._update_queue  = update_queue
        self._terminate     = threading.Event()

        self._cinfo         = dict()  # collection cache

        _, self._mongo_db, _, _, _ = ru.mongodb_connect (dburl)

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

                if cinfo['bulk'] and age > self._config['bulk_collection_time']:

                    res  = cinfo['bulk'].execute()
                    self._log.debug("bulk update result: %s", res)

                    # FIXME: eval res

                    prof('unit update bulk pushed (%d)' % len(cinfo['uids'].keys ()))
                    for uid in cinfo['uids']:
                        prof('unit update pushed (%s)' % cinfo['uids'][uid], uid=uid)

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
                        time.sleep(self._config['queue_poll_sleeptime'])

                    continue


                # got a new request.  Add to bulk (create as needed),
                # and push bulk if time is up.
                uid         = update_request.get('uid')
                state       = update_request.get('state', None)
                cbase       = update_request.get('cbase', '.cu')
                query_dict  = update_request.get('query', dict())
                update_dict = update_request.get('update',dict())

                prof('unit update pulled (%s)' % state, uid=uid)

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
              # prof('unit update bulked', uid=uid)

            except Exception as e:
                self._log.exception("unit update failed (%s)", e)
                # FIXME: should we fail the pilot at this point?
                # FIXME: Are the strategies to recover?


    # --------------------------------------------------------------------------
    #
    # insert_session, get_session and delete_session are not enacted over the
    # updater queue -- (insert/get need to happen before the updater thread(s) attach
    # to the database)
    #
    @staticmethod
    def insert_session(dburl, sid, config):

        now = timestamp ()

        prof('session insert', msg='start')

        mongo, db, dbname, pname, cname = ru.mongodb_connect (dburl)

        if  db[sid].count() != 0 :
            raise ValueError ("Session '%s' already exists." % sid)

        s = db["%s" % sid]
        s.insert({'_id'          : sid,
                 'config'        : config,
                 'umgr'          : list(),
                 'pmgr'          : list(),
                 'created'       : True,
                 'modified'      : True, 
                 'deleted'       : False})

        s.update({'_id'          : sid},
                 {'$currentDate' : {'created'  : { '$type': 'timestamp'},
                                    'modified' : { '$type': 'timestamp'}}})

        # retrieve the netry again, which now has the time stamp
        cursor = s.find({'_id':sid})
        data   = cursor[0]

        # Create the pilot and unit collections, and the collection for the
        # managers
        db["%s.u"  % sid]
        db["%s.um" % sid] 
        db["%s.p"  % sid]
        db["%s.pm" % sid] 

        prof('session insert', msg='done')

        print " == data ================="
        import pprint
        pprint.pprint (data)
        print " ========================="

        return data


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def delete_session(dburl, sid):

        now = timestamp ()

        prof('session delete', msg='start')

        mongo, db, dbname, pname, cname = ru.mongodb_connect (dburl)

        if  db[sid].count() == 0 :
            self._log.warning ("Session '%s' already gone.", sid)

        s.update({'_id'          : sid},
                 {'$currentDate' : {'deleted'  : { '$type': 'timestamp'},
                                    'modified' : { '$type': 'timestamp'}}})

        # retrieve the netry again, which now has the time stamp
        cursor = s.find({'_id':sid})
        data   = cursor[0]
        db["%s"    % sid].drop 
        db["%s.u"  % sid].drop 
        db["%s.um" % sid].drop  
        db["%s.p"  % sid].drop 
        db["%s.pm" % sid].drop  

        prof('session delete', msg='done')

        print " == data ================="
        import pprint
        pprint.pprint (data)
        print " ========================="

        return data


    # --------------------------------------------------------------------------
    #
    # insert is synchronous for now.  Once it becomes async, we push it with
    # a COMMAND_INSERT onto the update queue -- then this method becomes
    # a staticmethod like update_unit.  Then we don't need to do the bulking in
    # the method, but can rely on the bulking behind the updater queue...
    #
    def insert_unit(units, msg=None, logger=None):

        now = timestamp ()

        if not isinstance (units, list):
            units=[units]

        cname = self._session_id + '.cu.2'   # FIXME: use def
        coll  = self._mongo_db[cname]
        bulk  = coll.initialize_ordered_bulk_op()

        for unit in units :

            uid = unit.uid

            if logger and msg:
                logger("unit '%s' insert: %s" % (uid, msg))

            # use pseudo-deep copies
            cu_dict = dict(unit._data)

            # let mongodb add timestamps for unit updates

            cu_data['type']          = 'unit'
            cu_data['state_history'] = {'state'     : NEW, 
                                        'timestamp' : now}
            cu_data['$currentDate']  = {'modified'  : { '$type': 'timestamp' }}

            bulk.insert (cu_dict)

        res = bulk.execute()
        self._log.debug("bulk insert result: %s", res)

        prof('unit insert bulk pushed (%d)' % len(units))
        for unit in units:
            prof('unit insert pushed', uid=unit.uid)


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def update_unit(queue, cu, state=None, msg=None, query=None, 
                    update=None, logger=None):

        now = timestamp ()
        uid = cu['_id']

        if  logger and msg:
            if state : logger("unit '%s' state change: %s" % (uid, msg))
            else     : logger("unit '%s' update: %s"       % (uid, msg))

        # use pseudo-deep copies
        query_dict  = dict(query)  if query  else dict()
        update_dict = dict(update) if update else dict()

        query_dict['_id'] = uid

        if state :
            if not '$set'  in update_dict : update_dict['$set']  = dict()
            if not '$push' in update_dict : update_dict['$push'] = dict()

            # this assumes that 'state' and 'stathistory' are not yet in the
            # updated dict -- otherwise they are overwritten...
            update_dict['$set']['state'] = state
            update_dict['$push']['statehistory'] = { 'state'     : state,
                                                     'timestamp' : now}

        if msg:
            if not '$push' in update_dict : update_dict['$push'] = dict()

            # this assumes that 'log' is not yet in the update_dict -- otherwise
            # it will be overwritten...
            update_dict['$push']['log'] = {'message'   : msg,
                                           'timestamp' : timestamp()}

        # we can artificially increase the load on the updater
        query_list = blowup (agent_config, query_dict, UPDATE) 

        for query in query_list :
            prof('push', msg="towards update (%s)" % state, uid=query['_id'])
            update_queue.put({'uid'    : query['_id'],
                              'state'  : state,
                              'cbase'  : '.cu',
                              'query'  : query,
                              'update' : update_dict})


# ------------------------------------------------------------------------------

