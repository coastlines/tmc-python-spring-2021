# https://platform.civisanalytics.com/spa/#/scripts/containers/86810705

from parsons import VAN, Redshift, S3, Table
from canalespy import logger, setup_environment, van_helpers, civis_helpers
import logging
import os
import ast

def main():

    setup_environment()
    rs = Redshift()
    s3 = S3()

    STATES_STR = os.environ['STATES']
    API_KEYS_STR = os.environ['VAN_API_KEYS_PASSWORD']
    SOURCE_TABLE = os.environ['SOURCE_TABLE']
    ID_TYPE = os.environ['ID_TYPE']
    DB = os.environ['DB']
    AC = os.environ['AC']
    AC_ACTION = os.environ['AC_ACTION'].lower()

    # Clean up output of Civis Parameters
    STATES = STATES_STR.replace(' ','').split(',') if ',' in STATES_STR else [STATES_STR]

    # let's grab the data
    if rs.table_exists(SOURCE_TABLE) is False:
        raise Exception(f"ERRROR: {SOURCE_TABLE} does not exist!")

    tbl = rs.query(f"select * from {SOURCE_TABLE}")

    # let's figure out the column we will use to filter on state
    # create a new column called my_state that we will use
    if len(STATES) == 1:
        tbl.add_column('my_state',STATES[0])
    else:
        state_col = [col for col in tbl.columns if 'state' in col]
        if len(state_col) == 0:
            raise Exception(f"ERROR: You have selected more than one state in STATES, but {SOURCE_TABLE} has no state column.")
        else:
            tbl.add_column('my_state',lambda row: row[state_col[0]])

    log = ""
    # Let's loop through states!
    for s in STATES:
        logger.info(f"Working on {s}...")

        # data for state
        state_tbl = tbl.select_rows(lambda row: row.my_state == s)

        if state_tbl.num_rows == 0:
            logger.info(f"No rows in {SOURCE_TABLE} for {s}...")
        else:
            # Let's initiate VAN Class for the state
            state_key = van_helpers.get_api_key(API_KEYS_STR, s)
            van = VAN(db = DB, api_key = state_key)

            # Let's do some Activist Codes!
            if AC != 'None':
                logger.info(f"Working on Activist Code: {AC}...")
                acs = van.get_activist_codes().select_rows(lambda row: str(row.activistCodeId) == AC or row.name.lower() == AC.lower())

                if acs.num_rows == 0:
                    raise Exception("ERROR: There is no Activist Code with that ID or Name!")
                else:
                    go_ac = state_tbl[ID_TYPE]
                    if AC_ACTION == 'apply':
                        for id in go_ac:
                            try:
                                van.apply_activist_code(id = id, activist_code_id = acs.first, id_type = ID_TYPE)
                            except:
                                log += f"\n {s}-{id} does not exist."
                    elif AC_ACTION == 'remove':
                        for id in go_ac:
                            try:
                                van.remove_activist_code(id = id, activist_code_id = acs.first, id_type = ID_TYPE)
                            except:
                                log += f"\n {s}-{id} does not exist."
                    else:
                        raise TypeError("Only 'apply' and 'remove' are allowed.")

    if len(log) > 0:
        civis_helpers.update_success_email(subject = "Errors from VAN AC Load",
                                           body = log)

if __name__ == '__main__':
    main()
