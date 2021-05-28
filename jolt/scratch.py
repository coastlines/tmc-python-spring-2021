##############

# original logging
   # setup logging
    if not rs.table_exists(LOG_TABLE): #if Log Table does not already exist, create it
        # standard sql
        sql_1 = "(source_vanid int, vanid int, date_created date, ac_load boolean, sg_load boolean"
        # to concat
        ac_prefix = "AC_load_"
        sg_prefix = "SG_load_"
        dt = " boolean"
        # AC string
        sql_2 = ["{}{}{}".format(ac_prefix,a,dt) for a in AC]
        sql_2 = ", ".join(sql_2)
        # SG SQ
        sql_3 = ["{}{}{}".format(sg_prefix,s,dt) for S in SG]
        sql_3 = ", ".join(sql_3)
        # Join all sql strings
        sql = sql_1 + ', ' + sql_2 + ', ' + sql_3 + ';)'
        rs.query(f"create table {LOG_TABLE} {sql}")
        logger.info(f"Creating {LOG_TABLE}...")

##############
            try:
              apply_ac_1 = van.apply_activist_code(id=up['vanId'],
                                            id_type='vanid',
                                            activist_code_id=AC_1,
                                            action=AC_ACTION)

              apply_ac_2 = van.apply_activist_code(id=up['vanId'],
                                            id_type='vanid',
                                            activist_code_id=AC_2,
                                            action=AC_ACTION)

              apply_ac_3 = van.apply_activist_code(id=up['vanId'],
                                            id_type='vanid',
                                            activist_code_id=AC_3,
                                            action=AC_ACTION)
            except: # catch errors
              apply_ac_1 = 'Error'
              apply_ac_2 = 'Error'
              apply_ac_3 = 'Error'
              
            if isinstance(apply_ac_1,bool)==False: # catches ac load error
              apply_ac_1 = 'Error'
            if isinstance(apply_ac_2,bool)==False: # catches ac load error
              apply_ac_2 = 'Error'
            if isinstance(apply_ac_3,bool)==False: # catches ac load error
              apply_ac_3 = 'Error'



# AC 1
            try:
              apply_ac_1 = van.apply_activist_code(id=up['vanId'],
                                            id_type='vanid',
                                            activist_code_id=AC_1,
                                            action=AC_ACTION)
            except: # catch errors
              apply_ac_1 = 'Error'
              
            if isinstance(apply_ac_1,bool)==False: # catches ac load error
              apply_ac_1 = 'Error'

            # AC 2
            try:
              apply_ac_2 = van.apply_activist_code(id=up['vanId'],
                                            id_type='vanid',
                                            activist_code_id=AC_2,
                                            action=AC_ACTION)
            except: # catch errors
              apply_ac_2 = 'Error'
              
            if isinstance(apply_ac_2,bool)==False: # catches ac load error
              apply_ac_2 = 'Error'

            # AC 3
            try:
              apply_ac_3 = van.apply_activist_code(id=up['vanId'],
                                            id_type='vanid',
                                            activist_code_id=AC_3,
                                            action=AC_ACTION)
            except: # catch errors
              apply_ac_3 = 'Error'

            if isinstance(apply_ac_3,bool)==False: # catches ac load error
              apply_ac_3 = 'Error'

-------------------
# Let's do some Activist Codes!
    for a in range(len(AC)):
        logger.info(f"Working on Activist Code: {AC[a]}...")

        if AC != 'None':
            logger.info(f"Working on Activist Code: {AC}...")
            acs = van.get_activist_codes().select_rows(lambda row: str(row.activistCodeId) == AC or row.name.lower() == AC.lower())

            if acs.num_rows == 0:
                raise Exception("ERROR: There is no Activist Code with that ID or Name!")
            else:
                go_ac = source_tbl[ID_TYPE]
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
        civis_helpers.update_success_email(subject = "Errors from VAN Load",
                                           body = log)


----------------------------------------------------------------

            for s in range(len(SG)):
                logger.info(f"Working on Supporter Group: {SG[s]}...")

                if SG != 'None':
                    logger.info(f"Working on Supporter Group: {SG}...")
                    sgs = van.get_supporter_groups().select_rows(lambda row: str(row.supporterGroupId) == SG or row.name.lower() == SG.lower())

                    if sgs.num_rows == 0:
                        raise Exception("ERROR: There is no Supporter Group with that ID or Name!")
                    else:
                        go_sg = source_tbl[ID_TYPE]
                        if SG_ACTION == 'add':
                            for id in go_sg:
                                try:
                                    van.add_person_supporter_group(vanid = vanId, supporter_group_id = sgs.first)
                                except:
                                    log += f"\n {s}-{id} does not exist."
                        elif AC_ACTION == 'remove':
                            for id in go_ac:
                                try:
                                    van.remove_person_supporter_group(vanid = vanId, supporter_group_id = sgs.first)
                                except:
                                    log += f"\n {s}-{id} does not exist."
                        else:
                            raise TypeError("Only 'apply' and 'remove' are allowed.")
