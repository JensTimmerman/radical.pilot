
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import datetime

# -----------------------------------------------------------------------------
# The state "struct" encapsulates a state and its timestamp.
class State(object):

    __slots__ = ('state', 'timestamp')

    # ----------------------------------------
    #
    def __init__(self, state, timestamp):

        self.state     = state
        self.timestamp = timestamp

    # ----------------------------------------
    #
    def __eq__(self, other):
        """Compare 'State' as well as 'str' types.
        """
        if type(other) == str:
            return self.state.lower() == other.lower()
        else:
            return self.state == other.state

    # ----------------------------------------
    #
    def __ne__(self, other):
        return not self.__eq__(other)

    # ----------------------------------------
    #
    def as_dict(self):
        return {"state": self.state, "timestamp": self.timestamp}

    # ----------------------------------------
    #
    def __str__(self):
        """The string representation is just the state as string without its 
           timestamp.
        """
        return self.state


# -----------------------------------------------------------------------------
# ComputePilot States
UNKNOWN                      = 'Unknown'
NEW                          = 'New'
LAUNCHING_PENDING            = 'PendingLaunch'
LAUNCHING                    = 'Launching'
ACTIVE_PENDING               = 'PendingActive'
ACTIVE                       = 'Active'
DONE                         = 'Done'
FAILED                       = 'Failed'
CANCELED                     = 'Canceled'


# -----------------------------------------------------------------------------
# ComputeUnit States
UNKNOWN                      = 'Unknown'
NEW                          = 'New'
UMGR_SCHEDULING_PENDING      = 'UMGR_Scheduling_Pending'
UMGR_SCHEDULING              = 'UMGR_Scheduling'
UMGR_STAGING_INPUT_PENDING   = 'UMGR_Staging_Input_Pending'
UMGR_STAGING_INPUT           = 'UMGR_Staging_Input'
AGENT_STAGING_INPUT_PENDING  = 'Agent_Staging_Input_Pending'
AGENT_STAGING_INPUT          = 'Agent_Staging_Input'
AGENT_SCHEDULING_PENDING     = 'Agent_Scheduling_Pending'
AGENT_SCHEDULING             = 'Agent_Scheduling'
EXECUTION_PENDING            = 'Execution_Pending'
EXECUTING                    = 'Executing'
AGENT_STAGING_OUTPUT_PENDING = 'Agent_Staging_Output_Pending'
AGENT_STAGING_OUTPUT         = 'Agent_Staging_Output'
UMGR_STAGING_OUTPUT_PENDING  = 'UMGR_Staging_Output_Pending'
UMGR_STAGING_OUTPUT          = 'UMGR_Staging_Output'
DONE                         = 'Done'
FAILED                       = 'Failed'
CANCELED                     = 'Canceled'


# -----------------------------------------------------------------------------

