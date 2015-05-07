
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

from direct_submission import DirectSubmissionScheduler
from round_robin       import RoundRobinScheduler
from backfilling       import BackfillingScheduler

# -----------------------------------------------------------------------------
# Constants
SCHED_ROUND_ROBIN       = "round_robin"
SCHED_DIRECT            = "direct_submission"
SCHED_BACKFILLING       = "backfilling"

# alias:
SCHED_DIRECT_SUBMISSION = SCHED_DIRECT

# default:
SCHED_DEFAULT           = SCHED_ROUND_ROBIN

