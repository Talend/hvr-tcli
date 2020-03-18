#!python

################################################################################
#
# NAME
#     manifestagent.py - Write cycle manifest file
#
# SYNOPSIS
#     manifestagent.py <mode> <chn> <loc> [<userargs>]
#
# DESCRIPTION
#     This script should be defined in an HVR channel using action AgentPlugin.
#     Including this script results in a single manifest getting created for 
#     a refresh and an integration cycle.
#     Its behavior depends on the agent mode and options supplied in parameter
#     /UserArgument (see OPTIONS below).
#
# OPTIONS
#     -m mani_fexpr  Manifest file rename expression. Mandatory. Example:
#                    manifest-{hvr_integ_tstamp}.json
#                    Subdirectories are allowed.
#                    Only use {hvr_tbl_name} in the rename expression if the
#                    manifest are created once per table e.g. during a refresh
#                    that is run per table (to guarantee unique manifest names).
#
#     -s statedir    Use statedir for manifest files
#
# AGENT OPERATION
#
#     The following modes are supported by this agent. Other modes are ignored.
#     All modes are meant to be run by HVR integrate/refresh via AgentPlugin
#     action.
#
#     integ_end
#         Write manifest file implied by option [-m mani_fexpr]. For value
#         cycle_begin use value from $HVR_AGENT_BEGIN_TSTAMP, and use
#         $HVR_AGENT_END_TSTAMP for value cycle_end. Existing manifest files
#         are not deleted by this.
#
#     refr_write_end
#         Write manifest file (similar to integ_end).
#
################################################################################

import sys
import os
import getopt
import json
import re
import time
import traceback

class AgentError(Exception):
    pass

def plural(x):
    if x == 1:
        return ''
    else:
        return 's'

def usage(me, extra):
    usage= '{0}\nUsage: {1} <mode> <chn> <loc> [userargs]\n' + \
            '    userargs= "' + \
            '[-m mani_fexpr] ' + \
            '[-s statedir]"'
    raise AgentError(usage.format(extra, me))

def parse_opts(argv):
    global g_agent_env
    global g_hvr_vars
    global g_mode
    global g_chn
    global g_loc
    global g_mani_fexpr
    global g_statedir

    g_mani_fexpr= None
    g_statedir= None

    seen_opts= {}

    me= argv[0]
    if len(argv) < 4:
        usage(me, 'Missing mode,chn,loc arguments');
    elif len(argv) > 5:
        usage(me, 'Extra arguments after userargs argument')

    g_mode= argv[1]
    g_chn= argv[2]
    g_loc= argv[3]
    userargs= []
    if len(argv) >= 5:
        userargs= re.split(r'\s+', argv[4].strip())

    if g_mode not in ['integ_end','refr_write_end']:
        # Don't bother parsing options.
        return

    g_agent_env= load_agent_env()

    g_hvr_vars= {}
    for k,v in g_agent_env.items():
        if k.startswith('HVR_VAR_'):
            g_hvr_vars[k.lower()]= v

    try:
        opts, args= getopt.getopt(userargs, 'i:m:s:v:')
    except getopt.GetoptError as e:
        usage(me, str(e))

    for (opt_key, opt_val) in opts:
        if opt_key in seen_opts:
            usage(me, 'Multiple {0} options specified'.format(opt_key))

        elif opt_key == '-m':
            if not set(fexpr_hvr_vars(opt_val)).issubset(set(g_hvr_vars.keys())):
                raise AgentError( ("Option -m contains a context variable that "
                                   "does not exist in HVR_VAR_ environment "
                                   "vars: {} vs {}").format(
                                       fexpr_hvr_vars(opt_val),
                                       g_hvr_vars.keys()) )
            g_mani_fexpr= opt_val
        elif opt_key == '-s':
            g_statedir= opt_val

    if len(args):
        usage(me, 'extra userargs specified')

    if g_statedir is None:
        if 'HVR_LOC_STATEDIR' not in g_agent_env:
            raise AgentError('Option -s (or $HVR_LOC_STATEDIR) must be specified')
        g_statedir= g_agent_env['HVR_LOC_STATEDIR']

        g_statedir= url_strip(g_statedir)

    if g_statedir.find('://') > -1:
        raise AgentError("Option -s (or $HVR_LOC_STATEDIR) '{0}' cannot be a URL".format(g_statedir))
    if not os.path.exists(g_statedir):
        raise AgentError("Option -s (or $HVR_LOC_STATEDIR) '{0}' does not exist".format(g_statedir))

    if g_mani_fexpr is None:
        raise AgentError('Option -m must be specified')

def load_agent_env():
    agent_env= {}

    if 'HVR_LONG_ENVIRONMENT' in os.environ:
        hvr_long_environment= os.environ['HVR_LONG_ENVIRONMENT']
        try:
            with open(hvr_long_environment, "r") as f:
                long_env= json.loads(f.read())

            for k,v in long_env.items():
                agent_env[str(k)]= str(v)

        except Exception as e:
            sys.stderr.write( ("W_JX0E00: Warning: An error occured while "
                               "processing $HVR_LONG_ENVIRONMENT file "
                               "'{}'. Will continue without processing this "
                               "file. Error: {} {}").format(
                                   hvr_long_environment,
                                   str(e),
                                   traceback.format_exc()) )

    for k,v in os.environ.items():
        k= str(k)
        if k not in agent_env:
            agent_env[k]= str(v)

    return agent_env

def verify_env_abbr():
    # Verify important env vars against F_JG243F
    for nm in ['HVR_AGENT_BEGIN_TSTAMP',
               'HVR_AGENT_END_TSTAMP',
               'HVR_TBL_NAMES',
               'HVR_BASE_NAMES',
               'HVR_TBL_NROWS',
               'HVR_LOC_STATEDIR']:
        if nm in g_agent_env:
            vl= g_agent_env[nm]
            if vl.endswith('...'):
                # Env var is abbreviated due to long size
                if 'HVR_MANIFEST_AGENT_WARN_ABBR' in g_agent_env and \
                        nm in g_agent_env['HVR_MANIFEST_AGENT_WARN_ABBR'].split(":"):
                    sys.stderr.write( ("W_JX0E00: Warning: environment "
                                       "variable ${} is truncated at {} "
                                       "characters possibly due to long size. "
                                       "This is ignored because of "
                                       "$HVR_MANIFEST_AGENT_WARN_ABBR."
                                       ).format(nm, len(vl)) )
                else:
                    raise AgentError( ("Environment variable ${} is truncated "
                                       "at {} characters possibly due to long "
                                       "size. This environment variable is "
                                       "important for hvrmanifestagent to "
                                       "function as intended. Refusing to "
                                       "continue. Truncated value: {{ {} }}"
                                       ).format(nm, len(vl), vl) )

def main(argv):
    parse_opts(argv)

    if g_mode == 'integ_end':
        mode_integ_or_refr_end(is_refresh= False)
        return 0
    elif g_mode == 'refr_write_end':
        mode_integ_or_refr_end(is_refresh= True)
        return 0
    else:
        return 2  # ignore mode

def to_json(x):
    return json.dumps(x, indent=4, sort_keys=True)

def to_utc_tstamp(unix_time):
    # YYYY-mm-ddTHH:MM:SSZ in UTC
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(unix_time))

def to_integ_tstamp(unix_time):
    # YYYYmmddHHMMSS in UTC
    return time.strftime('%Y%m%d%H%M%S', time.gmtime(unix_time))

def fexpr_to_re(fexpr, hvr_tbl_name= None):
    has_tbl_name= False
    fexpr_re= '^'
    for part in re.split(r'({[^}]*})', fexpr):

        if len(part) > 0 and part[0] == '{' and part[-1] == '}':
            if part == '{hvr_tbl_name}' and hvr_tbl_name is not None:
                # Substitute known table name
                fexpr_re += re.escape(hvr_tbl_name)

            elif part == '{hvr_tbl_name}' and not has_tbl_name:
                # Substitute capturing group for table name
                fexpr_re += r'(.*?)' # non-greedy match
                has_tbl_name= True

            elif part.startswith('{hvr_var_') and part[1:-1] in g_hvr_vars:
                # Substitute known context variable
                fexpr_re += re.escape(g_hvr_vars[part[1:-1]])

            else:
                # Substitute any match
                fexpr_re += r'.*?' # non-greedy match

        else:
            fexpr_re += re.escape(part)

    fexpr_re += '$'

    return fexpr_re

def fexpr_per_table(s):
    return s.find('{hvr_tbl_name}') != -1

def fexpr_hvr_vars(s):
    hvr_vars= []

    for part in re.split(r'({[^}]*})', s):
        if part.startswith('{hvr_var_') and part[-1] == '}':
            hvr_vars.append(part[1:-1])

    return hvr_vars

def mode_integ_or_refr_end(is_refresh):
    now= time.time()

    verify_env_abbr()

    cycle_begin= now
    cycle_end= now
    if 'HVR_AGENT_BEGIN_TSTAMP' in g_agent_env:
        cycle_begin= int(g_agent_env['HVR_AGENT_BEGIN_TSTAMP'])
    if 'HVR_AGENT_END_TSTAMP' in g_agent_env:
        cycle_end= int(g_agent_env['HVR_AGENT_END_TSTAMP'])

    if 'HVR_AGENT_BEGIN_TSTAMP' in g_agent_env and \
            'HVR_AGENT_END_TSTAMP' in g_agent_env and \
            cycle_begin == cycle_end:
        # If this cycle took 0 seconds; we might overwrite previous cycle's
        # manifests (if it ended at cycle_begin). Increment local cycle_end to
        # prevent that. Also sleep(1) to postpone next cycle by 1 second, so it
        # can't overwrite this cycle's manifest if it takes 0 secs again
        cycle_end += 1
        time.sleep(1)

    hvr_integ_tstamp= to_integ_tstamp(cycle_end)

    hvr_tbl_names= []
    hvr_tbl_names_crosscheck= {}
    if 'HVR_TBL_NAMES' in g_agent_env and g_agent_env['HVR_TBL_NAMES'] != '':
        hvr_tbl_names= g_agent_env['HVR_TBL_NAMES'].split(":")

    hvr_base_names= []
    if 'HVR_BASE_NAMES' in g_agent_env:
        if g_agent_env['HVR_BASE_NAMES'] != '':
            hvr_base_names= g_agent_env['HVR_BASE_NAMES'].split(":")
            hvr_tbl_names_crosscheck['HVR_BASE_NAMES']= len(hvr_base_names)
        elif len(hvr_tbl_names) > 0:   # Empty value might mean 0 or 1 entries
            hvr_tbl_names_crosscheck['HVR_BASE_NAMES']= 1
        else:
            hvr_tbl_names_crosscheck['HVR_BASE_NAMES']= 0

    hvr_tbl_nrows= []
    if 'HVR_TBL_NROWS' in g_agent_env:
        if g_agent_env['HVR_TBL_NROWS'] != '':
            hvr_tbl_nrows= g_agent_env['HVR_TBL_NROWS'].split(":")
            hvr_tbl_names_crosscheck['HVR_TBL_NROWS']= len(hvr_tbl_nrows)
        elif len(hvr_tbl_names) > 0:   # Empty value might mean 0 or 1 entries
            hvr_tbl_names_crosscheck['HVR_TBL_NROWS']= 1
        else:
            hvr_tbl_names_crosscheck['HVR_TBL_NROWS']= 0

    # Cross check table names with other per-table env vars
    if 'HVR_TBL_NAMES' in g_agent_env and len(hvr_tbl_names_crosscheck) == 0:
        raise AgentError( ("Cannot find any environment variable to cross "
                           "check number of tables in HVR_TBL_NAMES. Expected "
                           "to find at least one of [HVR_BASE_NAMES, "
                           "HVR_TBL_NROWS, HVR_TBL_CAP_TSTAMP] in environment.") )
    else:
        for var,num in hvr_tbl_names_crosscheck.items():
            if num != len(hvr_tbl_names):
                raise AgentError( ("Cross checking HVR_TBL_NAMES environment "
                                   "variable to {} failed. HVR_TBL_NAMES "
                                   "mentions {} tables, {} has {} tables "
                                   "({} vs '{}').").format(var,
                                       len(hvr_tbl_names), var, num,
                                       str(hvr_tbl_names),
                                       g_agent_env[var]) )

    #Only write manifest if something was integrated
    if len(hvr_tbl_names) > 0:
        manifest= {
            'channel': g_chn,
            'integ_loc': {
                'name': g_loc
            },
            'tables': [],
            'cycle_begin': to_utc_tstamp(cycle_begin),
            'cycle_end': to_utc_tstamp(cycle_end),
            'initial_load': is_refresh
        }

        for base_name in hvr_base_names:
            manifest['tables'].append(base_name)

        if fexpr_per_table(g_mani_fexpr):
            hvr_tbl_name= hvr_tbl_names[0]
            manifest_filename= g_mani_fexpr.format(hvr_tbl_name=hvr_tbl_name,
                hvr_integ_tstamp=hvr_integ_tstamp, **g_hvr_vars)
        else:
            manifest_filename= g_mani_fexpr.format(hvr_integ_tstamp=hvr_integ_tstamp, **g_hvr_vars)

        # Write tmp manifest file
        manifest_write_tmp(manifest, manifest_filename)

        # Move <manifest_filename>_tmp to <manifest_filename>
        #
        # Note: If consumer depends on removing manifests as they are processed,
        # beware that manifests can be recreated during recovery. Consumer should
        # keep track of removed manifests until next group of manifests are ready.
        manifest_write_move(manifest_filename)

        sys.stdout.write( ("Written manifest {}\n").format(manifest_filename) )

def manifest_write_tmp(manifest, manifest_filename):
    filename= os.path.join(g_statedir, manifest_filename)

    dirname= os.path.dirname(filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    manifest_s= to_json(manifest)
    with open(filename + '_tmp', 'w') as f:
        f.write(manifest_s)
        f.flush()
        os.fsync(f.fileno())

def manifest_write_move(manifest_filename):
    filename= os.path.join(g_statedir, manifest_filename)

    if os.name == 'nt' and os.path.isfile(filename):
        os.unlink(filename) # Windows can't move over file
    os.rename(filename + '_tmp', filename)

def url_strip(url):
    # strip credentials (if URL)
    m= re.match(r'^(.*?)://([^@]*@)(.*)$', url)
    if m:
        return '{0}://{1}'.format(m.group(1), m.group(3))
    else:
        return url

if __name__ == "__main__":
    try:
        res= main(sys.argv)
        sys.exit(res)
    except Exception as err:
        sys.stdout.flush()
        sys.stderr.write("F_JX0E00: {0}\n".format(err))
        if not isinstance(err, AgentError):
            sys.stderr.write(traceback.format_exc())
        sys.stderr.flush()
        sys.exit(1)

