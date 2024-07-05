import os
import pwd

from log.log import Log, AutosubmitCritical
from autosubmit.config.basicConfig import BasicConfig
from typing import Tuple

def check_experiment_ownership(expid, basic_config, raise_error=False, logger=None):
    #Logger variable is not needed, LOG is global thus it will be read if avaliable
    my_user_ID = os.getuid()
    current_owner_ID = 0
    current_owner_name = "NA"
    try:
        current_owner_ID = os.stat(os.path.join(basic_config.LOCAL_ROOT_DIR, expid)).st_uid
        current_owner_name = pwd.getpwuid(os.stat(os.path.join(basic_config.LOCAL_ROOT_DIR, expid)).st_uid).pw_name
    except:
        if logger:
            logger.info("Error while trying to get the experiment's owner information.")
    finally:
        if current_owner_ID <= 0 and logger:
            logger.info("Current owner '{0}' of experiment {1} does not exist anymore.", current_owner_name, expid)
    is_owner = current_owner_ID == my_user_ID
    eadmin_user = os.popen('id -u eadmin').read().strip() # If eadmin no exists, it would be "" so INT() would fail.
    if eadmin_user != "":
        is_eadmin = my_user_ID == int(eadmin_user)
    else:
        is_eadmin = False
    if not is_owner and raise_error:
        raise AutosubmitCritical("You don't own the experiment {0}.".format(expid), 7012)
    return is_owner, is_eadmin, current_owner_name


def as_rsync(p, src, dest):
    finished = False
    limit = 150
    rsync_retries = 0
    # Avoid infinite loop unrealistic upper limit, only for rsync failure
    while not finished and rsync_retries < limit:
        finished = False
        pipeline_broke = False
        Log.info(
            "Rsync launched {0} times. Can take up to 150 retrials or until all data is transfered".format(
                rsync_retries + 1))
        try:
            p.send_command(
                "rsync --timeout=3600 --bwlimit=20000 -aqz --remove-source-files " + src + " " + dest)
        except BaseException as e:
            Log.debug("{0}".format(str(e)))
            rsync_retries += 1
            try:
                if p.get_ssh_output_err() == "":
                    finished = True
                elif p.get_ssh_output_err().lower().find("no such file or directory") == -1:
                    finished = True
                else:
                    finished = False
            except:
                finished = False
            pipeline_broke = True
        if not pipeline_broke:
            if p.get_ssh_output_err().lower().find("no such file or directory") == -1:
                finished = True
            elif p.get_ssh_output_err().lower().find(
                    "warning: rsync") != -1 or p.get_ssh_output_err().lower().find(
                "closed") != -1 or p.get_ssh_output_err().lower().find(
                "broken pipe") != -1 or p.get_ssh_output_err().lower().find(
                "directory has vanished") != -1:
                rsync_retries += 1
                finished = False
            elif p.get_ssh_output_err() == "":
                finished = True
            else:
                error = True
                finished = False
                break
        p.send_command("find {0} -depth -type d -empty -delete".format(src))
        Log.result(
            "Empty dirs on {0} have been successfully deleted".format(src))
    return finished