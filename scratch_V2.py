# Civis Container Script: https://platform.civisanalytics.com/spa/#/scripts/containers/110179204

# This script first reads a redshift source table with EveryAction contacts.
# Then, the above individuals are upserted into a destination EveryAction committee.
# Finally, a single or series of activist codes and/or supporter groups are applied or removed.

import os
import logging
import datetime
import ast
from parsons import VAN, Redshift, S3, Table
from canalespy import logger, setup_environment, van_helpers, civis_helpers

API_KEY = os.environ['VAN_PASSWORD']
SOURCE_TABLE = os.environ['SOURCE_TABLE']
ID_TYPE = os.environ['ID_TYPE']
AC_STR = os.environ['AC']
AC_ACTION = os.environ['AC_ACTION'].lower()
SG_STR = os.environ['SG']
SG_ACTION = os.environ['SG_ACTION'].lower()
LOG_TABLE = os.environ['LOG_TABLE']
TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d") #timestamp to be used for logtable

def main():

    setup_environment()
    rs = Redshift()
    s3 = S3()
    van = VAN(api_key=API_KEY, db='EveryAction')

    # Clean up output of Civis Parameters
    AC = AC_STR.replace(' ','').split(',') if ',' in AC_STR else [AC_STR]
    SG = SG_STR.replace(' ','').split(',') if ',' in SG_STR else [SG_STR]

    # setup logging
    if not rs.table_exists(LOG_TABLE): #if Log Table does not already exist, create it
        # standard sql
        sql = "(source_vanid int, dest_vanid int, date_created date, ac_load varchar(100),ac_action varchar(10), sg_load varchar(100), sg_action varchar(10));"
        rs.query(f"create table {LOG_TABLE} {sql}")
        logger.info(f"Creating {LOG_TABLE}...")

    # let's grab the source data
    if rs.table_exists(SOURCE_TABLE) is False:
        raise Exception(f"ERROR: {SOURCE_TABLE} does not exist!")

    source_tbl = rs.query(f'''
      select * from
        (select *, row_number() over(partition by vanid order by datecreated desc) as dup
        from {SOURCE_TABLE}) a
      left join {LOG_TABLE} l on l.source_vanid = a.vanid
      where l.source_vanid is null
      and dup=1
      ;
      '''
    )

    logger.info(f"{source_tbl.num_rows} rows found.")
    
    # begin upserting
    # upsert will update an existing record or insert a new record if it does not exist
    if source_tbl.num_rows > 0:
      logger.info(f"{source_tbl.num_rows} new records found. Inserting into table")
      loaded = [['source_vanid', 'dest_vanid', 'datecreated','ac_load', 'ac_action', 'sg_load', 'sg_action']] #column names for our eventual Parsons Table of what we've loaded

      for ppl in source_tbl:
        logger.info(f"Working on source_vanid={ppl['vanid']}")
        
        address = ppl['submittedaddressline1']
        if address is None:
          # upsert without address, because it is not present in the source data
          up = van.upsert_person(first_name=ppl['submittedfirstname'],
                      last_name=ppl['submittedlastname'],
                      email=ppl['submittedhomeemail'],
                      phone=ppl['submittedmobilephone'],
                      zip=ppl['submittedpostalcode'])
        else:
          #upsert with address
          address_split = address.split(' ', 1)
          street_number = address_split[0] if len(address_split)>=1 else None
          street = address_split[1] if len(address_split)>=2 else None

          up = van.upsert_person(first_name=ppl['submittedfirstname'],
                      last_name=ppl['submittedlastname'],
                      email=ppl['submittedhomeemail'],
                      phone=ppl['submittedmobilephone'],
                      phone_type='C', # todo convert to variable
                      street_number=street_number,
                      street_name=street,
                      zip=ppl['submittedpostalcode'])

        if isinstance(up, tuple):
          logger.info("Issue loading to VAN!")
          vanids = [ppl['vanid'], 0, TIMESTAMP, 'Error', 'Error', 'Error', 'Error']

        else:
          logger.info(f"Loaded to VAN as {up['vanId']}...")
          # make sure this person hasn't already been loaded
          dup_check = rs.query(f"select * from {LOG_TABLE} where dest_vanid={up['vanId']}")
          # action_check = rs.query(f''' 
          #   select * from {LOG_TABLE} 
          #   where dest_vanid={up['vanId']} 
          #   and (ac_action != {AC_ACTION} and ac_loaded != 'None')
          #   and sg_action !={SG_ACTION}
          #   ;
          #   '''
          #   )

          # Let's do some Activist Codes!
          if dup_check.num_rows == 0: # Apply the Activist Code if they are not in the log table
            for a in range(len(AC)):

                if AC[a] and AC[a] != 'None':
                    logger.info(f"Working on Activist Code: {AC[a]}...")
                    acs = van.get_activist_codes().select_rows(lambda row: str(row.activistCodeId) == AC[a] or row.name.lower() == AC[a].lower())

                    if acs.num_rows == 0:
                        raise Exception("ERROR: There is no Activist Code with that ID or Name!")
                    else:
                        if AC_ACTION == 'apply':
                            logger.info(f"Applying activist code {AC[a]}...")
                            try:
                                van.apply_activist_code(id = up['vanId'], activist_code_id = acs.first, id_type = ID_TYPE)
                            except:
                                logger.info(f"\n {ppl}-{vanid} does not exist.")
                                ac = 'Error'
                        elif AC_ACTION == 'remove':
                            #for id in go_ac:
                            try:
                                van.remove_activist_code(id = up['vanId'], activist_code_id = acs.first, id_type = ID_TYPE)
                            except:
                                logger.info(f"\n {ppl}-{vanid} does not exist.")
                                ac = 'Error'
                        else:
                            raise TypeError("Only 'apply' and 'remove' are allowed.")
                        

            for s in range(len(SG)):

                if SG[s] and SG[s] != 'None':
                    logger.info(f"Working on Supporter Group: {SG[s]}...")
                    sgs = van.get_supporter_groups().select_rows(lambda row: str(row.id) == SG[s] or row.name.lower() == SG[s].lower())

                    if sgs.num_rows == 0:
                        raise Exception("ERROR: There is no Supporter Group with that ID or Name!")
                    else:
                        if SG_ACTION == 'add':
                            logger.info(f"Applying activist code {SG[s]}...")
                            try:
                                van.add_person_supporter_group(vanid = up['vanId'], supporter_group_id = sgs.first)
                            except:
                                logger.info(f"\n {ppl}-{vanid} does not exist.")
                                sg = 'Error'
                        elif SG_ACTION == 'remove':
                            try:
                                van.delete_person_supporter_group(vanid = up['vanId'], supporter_group_id = sgs.first)
                            except:
                                logger.info(f"\n {ppl}-{vanid} does not exist.")
                                sg = 'Error'
                        else:
                            raise TypeError("Only 'apply' and 'remove' are allowed.")
                

            # Create list of key information for log table
            vanids = [ppl['vanid'], up['vanId'],TIMESTAMP,AC_STR,AC_ACTION,SG_STR,SG_ACTION]
          else:
              vanids = [ppl['vanid'], up['vanId'],TIMESTAMP,'NULL','NULL','NULL','NULL']
              logger.info(f"{up['vanId']} already loaded to VAN...")

        # Append list to log table
        loaded.append(vanids)

      logger.info(f"Finished the loop! Now to load the work we've done into {LOG_TABLE}")
      load_dict = Table(loaded).to_redshift(LOG_TABLE,if_exists='append')
    else:
        # if there were 0 records from initial query
        logger.info("No new data to send to EveryAction!")

if __name__ == '__main__':
    main()