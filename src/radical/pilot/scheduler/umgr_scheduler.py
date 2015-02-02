
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import threading
import multiprocessing

from radical.pilot.states import *
from radical.pilot.utils  import prof, blowup

from radical.pilot.update_worker import UpdateWorker

# ------------------------------------------------------------------------------
#
# Scheduler setup
#
UMGR_SCHEDULER_THREADS   = 'threading'
UMGR_SCHEDULER_PROCESSES = 'multiprocessing'

UMGR_SCHEDULER_MODE      = UMGR_SCHEDULER_THREADS

if UMGR_SCHEDULER_MODE == UMGR_SCHEDULER_THREADS :
    COMPONENT_MODE = threading
    COMPONENT_TYPE = threading.Thread
    QUEUE_TYPE     = multiprocessing.Queue
elif UMGR_SCHEDULER_MODE == UMGR_SCHEDULER_PROCESSES :
    COMPONENT_MODE = multiprocessing
    COMPONENT_TYPE = multiprocessing.Process
    QUEUE_TYPE     = multiprocessing.Queue

# ------------------------------------------------------------------------------
#
# Scheduler names
#
UMGR_SCHEDULER_NAME_DIRECT      = 'Direct'
UMGR_SCHEDULER_NAME_FLOODING    = 'Flooding'
UMGR_SCHEDULER_NAME_ROUND_ROBIN = 'Round-Robin'
UMGR_SCHEDULER_NAME_BACKFILLING = 'Backfilling'

# ------------------------------------------------------------------------------
#
# Scheduler commands
#
COMMAND_SCHEDULE   = "schedule"
COMMAND_UNSCHEDULE = "unschedule"
COMMAND_RESCHEDULE = "reschedule"
COMMAND_CANCEL     = "Cancel"


# ------------------------------------------------------------------------------
#
# config flags 
#
# FIXME: move to some RP config
#
UMGR_SCHEDULER = 'UMGR_Scheduler'


# ==============================================================================
#
# Schedulers
#
# ==============================================================================
#
class UMGR_Scheduler(COMPONENT_TYPE):

    """
    The UMGR_Scheduler is the base class to all unit manager schedulers.  It
    provides most of the plumbing, and is complete short of the actual
    scheduling algorithm.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, config, logger, session, umgr_schedule_queue, 
                 umgr_staging_input_queue, update_queue):

        # At some point in the future, the scheduler will also get an
        # agent_staging_input_queue and an agent_scheduling_queue -- but for
        # now, those components are fed from the agent class which pulls from the
        # DB, which is fed by the updater -- so pushing to the updater is
        # enough...

        COMPONENT_TYPE.__init__(self)

        self.name                       = name
        self._config                    = config
        self._log                       = logger
        self._session                   = session
        self._umgr_schedule_queue       = umgr_schedule_queue
        self._umgr_staging_input_queue  = umgr_staging_input_queue
        self._agent_staging_input_queue = None
        self._agent_scheduling_queue    = None
        self._update_queue              = update_queue

        self._terminate                 = COMPONENT_MODE.Event()
        self._lock                      = COMPONENT_MODE.RLock()
        self._pilot_list                = list()
        self._pilot_list_lock           = COMPONENT_MODE.RLock()
        self._wait_queue                = list()
        self._wait_queue_lock           = COMPONENT_MODE.RLock()

        self._configure()


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Launch Method.
    #
    @classmethod
    def create(cls, name, config, logger, session, scheduler, umgr_schedule_queue, 
               umgr_staging_input_queue, update_queue):

        # Make sure that we are the base-class!
        if cls != UMGR_Scheduler:
            raise TypeError("Scheduler Factory only available to base class!")

        try:
            implementation = {
                UMGR_SCHEDULER_NAME_DIRECT      : UMGR_Scheduler_Direct,
              # UMGR_SCHEDULER_NAME_FLOODING    : UMGR_Scheduler_Flooding,
              # UMGR_SCHEDULER_NAME_ROUND_ROBIN : UMGR_Scheduler_RoundRobin,
              # UMGR_SCHEDULER_NAME_BACKFILLING : UMGR_Scheduler_Backfilling
            }[scheduler]

            impl = implementation(name, config, logger, session, umgr_schedule_queue, 
                                  umgr_staging_input_queue, update_queue)
            impl.start()
            return impl

        except KeyError:
            raise ValueError("Scheduler '%s' unknown!" % name)


    # --------------------------------------------------------------------------
    #
    def stop(self):
        self._terminate.set()


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        pass


    # --------------------------------------------------------------------------
    #
    def pilot_status(self):
        raise NotImplementedError("pilot_status() not implemented for Scheduler '%s'." % self.name)


    # --------------------------------------------------------------------------
    #
    def _allocate_pilot(self, cores):
        raise NotImplementedError("_allocate_pilot() not implemented for Scheduler '%s'." % self.name)


    # --------------------------------------------------------------------------
    #
    def _release_pilot(self, pid, cores):
        raise NotImplementedError("_release_pilot() not implemented for Scheduler '%s'." % self.name)


    # --------------------------------------------------------------------------
    #
    def add_pilot(self, pilots):

        if not isinstance (pilots, list) :
            pilots = [pilots]

        with self._pilot_list_lock :
            self._pilot_list += pilots

            # try to schedule waiting units to the new pilot
            self._umgr_schedule_queue.put([COMMAND_RESCHEDULE, None])


    # --------------------------------------------------------------------------
    #
    def remove_pilot(self, pilot):

        if not isinstance (pilots, list) :
            pilots = [pilots]

        with self._pilot_list_lock :

            for pilot in pilots :
                self._pilot_list.remove (pilot)

            # schedulers which want to recover units from a removed pilot need
            # to overload this method -- otherwise the expectation is that the
            # pilot will continue to execute units previously assigned to it.


    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, cu):

        cu['target_pilot'] = None

        # schedule this unit
        cu['target_pilot'] = self._allocate_pilot(cu['description']['cores'])


        if not cu['target_pilot']:
            # signal that CU remains unhandled
            return False


        # the unit is scheduled -- prep it up for the target resource
        # FIXME
        # unit.sandbox = schedule['pilots'][pid]['sandbox'] + "/" + str(unit.uid)

        # ud = unit.description

        # if  'kernel' in ud and ud['kernel'] :

        #     try :
        #         from radical.ensemblemd.mdkernels import MDTaskDescription
        #     except Exception as ex :
        #         logger.error ("Kernels are not supported in" \
        #               "compute unit descriptions -- install " \
        #               "radical.ensemblemd.mdkernels!")
        #         # FIXME: unit needs a '_set_state() method or something!
        #         self._session._dbs.set_compute_unit_state (unit._uid, FAILED, 
        #                 ["kernel expansion failed"])
        #         continue

        #     pilot_resource = schedule['pilots'][pid]['resource']

        #     mdtd           = MDTaskDescription ()
        #     mdtd.kernel    = ud.kernel
        #     mdtd_bound     = mdtd.bind (resource=pilot_resource)
        #     ud.environment = mdtd_bound.environment
        #     ud.pre_exec    = mdtd_bound.pre_exec
        #     ud.executable  = mdtd_bound.executable
        #     ud.mpi         = mdtd_bound.mpi



        # got an allocation, go off and advance the process
        prof('umgr schedule', msg="scheduled (%s)" % cu['target_pilot'], 
                 uid=cu['_id'], logger=self._log.info)

        cu_list = blowup (self._config, cu, UMGR_SCHEDULER)

        for _cu in cu_list :

            if cu['UMGR_Staging_Input_Directives'] :
                prof('push', msg="towards umgr staging input", uid=_cu['_id'])
                UpdateWorker.update_unit(queue = self._update_queue, 
                                         cu    = cu,
                                         state = UMGR_STAGING_INPUT_PENDING)
                self._umgr_staging_input_queue.put(_cu)

            elif cu['Agent_Staging_Input_Directives'] :
                prof('push', msg="towards agent staging input", uid=_cu['_id'])
                UpdateWorker.update_unit(queue = self._update_queue, 
                                         cu    = cu,
                                         state = AGENT_STAGING_INPUT_PENDING)
            else :
                prof('push', msg="towards agent scheduling", uid=_cu['_id'])
                UpdateWorker.update_unit(queue = self._update_queue, 
                                         cu    = cu,
                                         state = AGENT_SCHEDULING_PENDING)

        return True


    # --------------------------------------------------------------------------
    #
    def _reschedule(self):

        prof('reschedule')
        # cycle through wait queue, and see if we get anything running now.  We
        # cycle over a copy of the list, so that we can modify the list on the
        # fly
        for cu in self._wait_queue[:]:

            if self._try_allocation(cu):
                # NOTE: this is final, remove it from the wait queue
                with self._wait_queue_lock :
                    self._wait_queue.remove(cu)

        prof(self.pilot_status())
        prof('reschedule done')


    # --------------------------------------------------------------------------
    #
    def unschedule(self, cus):
        # release (for whatever reason) all pilots allocated to this CU

        prof('unschedule')
        prof(self.pilot_status())

        pilots_released = False

        if not isinstance(cus, list):
            cus = [cus]

        # needs to be locked as we try to release pilots, but pilots are acquired
        # in a different thread....
        with self._lock :

            for cu in cus:
                if cu['target_pilot']:
                    self._release_pilot(cu['target_pilot'], cu['cores'])
                    pilots_released = True
                    self._log.info (self.pilot_status())

        # notify the scheduling thread of released pilots
        if pilots_released:
            self._umgr_schedule_queue.put([COMMAND_RESCHEDULE, None])

        prof(self.pilot_status())
        prof('unschedule done - reschedule')


    # --------------------------------------------------------------------------
    #
    def run(self):

        self._log.info("started %s.", self)

        while not self._terminate.is_set():

            try:

                request = self._umgr_schedule_queue.get()

                # shutdown signal?
                if not request:
                    continue

                self._log.info ("request: %s" % request)

                # we either get a new scheduled CU, or get a trigger that cores were
                # freed, and we can try to reschedule waiting CUs
                command = request[0]
                data    = request[1]


                if command == COMMAND_RESCHEDULE:

                    self._reschedule()


                elif command == COMMAND_SCHEDULE:

                    cu = data

                    UpdateWorker.update_unit(queue = self._update_queue, 
                                             cu    = cu,
                                             state = UMGR_SCHEDULING)

                    # we got a new unit to schedule.  Either we can place 
                    # it straight away and move it to execution, or we have
                    # to put it on the wait queue.
                    prof('schedule', msg="unit received", uid=cu['_id'])
                    if not self._try_allocation(cu):
                        # No resources available, put in wait queue
                        with self._wait_queue_lock :
                            self._wait_queue.append(cu)
                        prof('schedule', msg="allocation failed", uid=cu['_id'])


                elif command == COMMAND_UNSCHEDULE :

                    cu = data

                    UpdateWorker.update_unit(queue = self._update_queue, 
                                             cu    = cu)

                    # we got a finished unit, and can re-use its cores
                    #
                    # FIXME: we may want to handle this type of requests
                    # with higher priority, so it might deserve a separate
                    # queue.  Measure first though, then optimize...
                    #
                    # NOTE: unschedule() runs re-schedule, which probably
                    # should be delayed until this bulk has been worked
                    # on...
                    prof('schedule', msg="unit deallocation", uid=cu['_id'])
                    self.unschedule(cu)

                else :
                    raise ValueError ("cannot handle scheduler command '%s'", command)


            except Exception as e:
                self._log.exception('Error in scheduler loop: %s', e)
                raise



# -----------------------------------------------------------------------------
# 
class UMGR_Scheduler_Direct(UMGR_Scheduler):
    """DirectSubmissionScheduler implements a single-pilot 'pass-through' 
    scheduling algorithm.
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self, name, config, logger, session, umgr_schedule_queue, 
                 umgr_staging_input_queue, update_queue) :

        UMGR_Scheduler.__init__(self, name, config, logger, session, 
                umgr_schedule_queue, umgr_staging_input_queue, update_queue)

        self._pilot_stats = dict()


    # -------------------------------------------------------------------------
    #
    def _allocate_pilot(self, cores):


        with self._pilot_list_lock :

            if not len(self._pilot_list):
                raise RuntimeError ('Direct Submission cannot operate on empty pilot set')

            if len(self._pilot_list) > 1:
                raise RuntimeError ('Direct Submission only works for a single pilot!')

            pilot = self._pilot_list[0]
            pid   = pilot['_id']

            if not pid in self._pilot_stats :
                self._pilot_stats[pid] = 0

            if pilot[cores] < cores :
                self._log ("pilot '%s' too small (%s < %s)", pid, pilot[cores], cores)
                return None

            self._pilot_stats[pid] += cores

            return pid


    # -------------------------------------------------------------------------
    #
    def _release_pilot(self, pid, cores):

        with self._pilot_list_lock :

            if not pid in self._pilot_stats :
                raise RuntimeError ('Direct Submission inconsistency')

            self._pilot_stats[pid] -= cores

            if not self._pilot_stats[pid] < 0 :
                raise RuntimeError ('Direct Submission inconsistency')


    # -------------------------------------------------------------------------
    #
    def pilot_status(self):

        ret = ""

        for pid, load in self._pilot_stats.iteritems():
            ret += "%10s : %d\n" % (pid, load)

        return ret


# ------------------------------------------------------------------------------

