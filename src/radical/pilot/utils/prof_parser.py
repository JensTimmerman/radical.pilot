import pandas as pd
import numpy as np

# "label", "component", "event", "message"
prof_entries = [
    ('a_get_u',         'MainThread',       'get', 'MongoDB to Agent (PendingExecution)'),
    ('a_build_u',       'MainThread',       'Agent get unit meta', ''),
    ('a_mkdir_u',       'MainThread',       'Agent get unit mkdir', ''),
    ('a_notify_alloc',  'MainThread',       'put', 'Agent to update_queue (Allocating)'),
    ('a_to_s',          'MainThread',       'put', 'Agent to schedule_queue (Allocating)'),

    ('s_get_alloc',     'CONTINUOUS',       'get', 'schedule_queue to Scheduler (Allocating)'),
    ('s_alloc_failed',  'CONTINUOUS',       'schedule', 'allocation failed'),
    ('s_allocated',     'CONTINUOUS',       'schedule', 'allocated'),
    ('s_to_ewo',        'CONTINUOUS',       'put', 'Scheduler to execution_queue (Allocating)'),
    ('s_unqueue',       'CONTINUOUS',       'unqueue', 're-allocation done'),

    ('ewo_get',         'ExecWorker-',      'get', 'executing_queue to ExecutionWorker (Executing)'),
    ('ewo_launch',      'ExecWorker-',      'ExecWorker unit launch', ''),
    ('ewo_spawn',       'ExecWorker-',      'ExecWorker spawn', ''),
    ('ewo_script',      'ExecWorker-',      'launch script constructed', ''),
    ('ewo_pty',         'ExecWorker-',      'spawning passed to pty', ''),
    ('ewo_notify_exec', 'ExecWorker-',      'put', 'ExecWorker to update_queue (Executing)'),
    ('ewo_to_ewa',      'ExecWorker-',      'put', 'ExecWorker to watcher (Executing)'),

    ('ewa_get',         'ExecWatcher-',     'get', 'ExecWatcher picked up unit'),
    ('ewa_complete',    'ExecWatcher-',     'execution complete', ''),
    ('ewa_notify_so',   'ExecWatcher-',     'put', 'ExecWatcher to update_queue (StagingOutput)'),
    ('ewa_to_sow',      'ExecWatcher-',     'put', 'ExecWatcher to stageout_queue (StagingOutput)'),

    ('sow_get_u',       'StageoutWorker-',  'get', 'stageout_queue to StageoutWorker (StagingOutput)'),
    ('sow_u_done',      'StageoutWorker-',  'final', 'stageout done'),
    ('sow_notify_done', 'StageoutWorker-',  'put', 'StageoutWorker to update_queue (Done)'),

    ('uw_get_alloc',    'UpdateWorker-',    'get', 'update_queue to UpdateWorker (Allocating)'),
    ('uw_push_alloc',   'UpdateWorker-',    'unit update pushed (Allocating)', ''),
    ('uw_get_exec',     'UpdateWorker-',    'get', 'update_queue to UpdateWorker (Executing)'),
    ('uw_push_exec',    'UpdateWorker-',    'unit update pushed (Executing)', ''),
    ('uw_get_so',       'UpdateWorker-',    'get', 'update_queue to UpdateWorker (StagingOutput)'),
    ('uw_push_so',      'UpdateWorker-',    'unit update pushed (StagingOutput)', ''),
    ('uw_get_done',     'UpdateWorker-',    'get', 'update_queue to UpdateWorker (Done)'),
    ('uw_push_done',    'UpdateWorker-',    'unit update pushed (Done)', '')
]

#
# Extract 'all', 'cloned' and 'real' Unit IDs from the raw profiling DF
# TODO: This probably needs to be converted to a DF straight away
#
def getuids_from_prof_df(rawdf):
    units = {}

    # Using "native" Python
    #units[exp]['all'] = [x for x in df.uid[df.uid > 0].unique() if x.startswith('unit')]
    units['all'] = [x for x in rawdf.uid.dropna().unique() if x.startswith('unit')]
    units['cloned']= [x for x in units['all'] if 'clone' in x]
    units['real'] = list(set(units['all']) - set(units['cloned']))

    # Or alternatively, with Pandas
    #uids_s = df['uid']
    #all_units_s = uids_s.loc[uids_s.str.startswith('unit.', na=False)].drop_duplicates()
    #units[exp]['all'] = set(all_units_s)
    #cloned_units_s = all_units_s.loc[all_units_s.str.contains('clone')]
    #units[exp]['cloned'] = set(cloned_units_s)
    #units[exp]['real'] = units[exp]['all'] - units[exp]['cloned']

    return units

#
# Lookup tuples in dataframe based on uid and the tuple from the prof_entries list
#
def tup2ts(df, uid, tup):
    all_for_uid = df[df.uid == uid].fillna('')
    val = all_for_uid[(all_for_uid.component.str.startswith(tup[1])) &
                      (all_for_uid.event == tup[2]) &
                      (all_for_uid.message == tup[3])].time
    try:
        return val.iloc[0]
    except Exception as e:
        return np.NaN

#
# Construct a unit based dataframe from a raw dataframe
#
def prof2df(rawdf, units):
    # TODO: create skip logic
    #if exp in indices and exp in info:
    #    continue

    indices = [unit for unit in units['real']]
    info = [{t[0]:tup2ts(rawdf, unit, t) for t in prof_entries} for unit in units['real']]

    # TODO: Also do this for cloned units

    return pd.DataFrame(info) # , index=indices[exp]).sort_index()
