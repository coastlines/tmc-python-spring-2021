#Civis Container Script: https://platform.civisanalytics.com/spa/#/scripts/containers/31200580
import os
import datetime
from parsons import Redshift, Table, VAN
from canalespy import logger, setup_environment

# Define some key variables
api_key = os.environ['VAN_PASSWORD']
destination_table = os.environ['DESTINATION']
log_table = os.environ['LOG']
# ct_id = 75 #contact type ID = website
# sq_id = 369653 #this is the RTV Survey Question ID - updated to the 2020 registration tracking survey question
# sr_id = 1520458 #this is the RTV Survey Response ID - updated to the 2020 registration tracking survey response
# vol_sq_id = 330200 #this is the volunteer Survey Question ID
# vol_sr_id = 1357598 #this is the volunter Survey Response ID
# stat_sq_id = 358090 #this is the Survey Question ID to track if the person completed the form
# stat_sr_id = 1473215 #this is the Survey Response ID to track if the person completed the form
timestamp = datetime.datetime.now().strftime("%Y%m%d") #timestamp to be used for logtable

def main():
    setup_environment()

    rs = Redshift()
    van = VAN(api_key=api_key, db='EveryAction')

    if not rs.table_exists(log_table): #if Log Table does not already exist, create it
        rs.query(f"create table {log_table} (rtv_id varchar(1024), vanid int, date_created date, sq_load varchar(100), vol varchar(100));")
        logger.info(f"Creating {log_table}...")

    # We are now pulling the relevant Rock the Vote data.
    # Because there are dups, we are doing a distinct, and using md5() to create a unique ID.
    # We are joining to the log file as well to pull out records already pushed into EA.
    rtv_query = f'''
    select * from

        (select *, row_number() over (partition by id order by create_time desc) as dup
        from
            (select distinct md5(create_time::date::varchar(1024)||email_address) as id
            ,create_time::date::varchar(100)||'T01:00:00' as create_time
            ,status,lang,home_zip_code
            ,us_citizen,name_title,first_name,middle_name,last_name,name_suffix
            ,home_address,home_unit,home_city,home_state,has_mailing_address
            ,mailing_address,mailing_unit,mailing_city,mailing_state
            ,mailing_zip_code,race,party
            ,case when len(regexp_replace(phone, '[^0-9]+', ''))=10
                  then regexp_replace(phone, '[^0-9]+', '')
                  when len(regexp_replace(phone, '[^0-9]+', ''))=11 and phone like '1%'
                  then regexp_replace(phone, '[^0-9]+', '')
                  else NULL end as phone
            ,phone_type,email_address,opt_in_email
            ,opt_in_sms,opt_in_volunteer,partner_opt_in_email,dob
            from {destination_table}
            where email_address is not null)) a

    left join {log_table} l on a.id=l.rtv_id
    where l.rtv_id is null
    and dup=1
    ;
    '''
    rtv = rs.query(rtv_query) #pull query results into Parsons Table
    logger.info(f"{rtv.num_rows} rows found.")

    if rtv.num_rows > 0:
        logger.info(f"{rtv.num_rows} new records found. Inserting into table.")
        loaded = [['rtv_id','vanid','date_created','sq_load','vol']] #column names for our eventual Parsons Table of what we've loaded
        for ppl in rtv:
            logger.info(f"Working on rtv_id={ppl['id']}")
            address = ppl['home_address']
            if address is None:
                # upsert with no address, because it is not present in RTV
                up = van.upsert_person(first_name=ppl['first_name'],
                              last_name=ppl['last_name'],
                              date_of_birth=ppl['dob'],
                              email=ppl['email_address'],
                              phone=ppl['phone'],
                              zip=ppl['home_zip_code'])
            else:
                # upsert with address
                address_split = address.split(' ', 1)
                street_number = address_split[0] if len(address_split)>=1 else None
                street = address_split[1] if len(address_split)>=2 else None

                up = van.upsert_person(first_name=ppl['first_name'],
                              last_name=ppl['last_name'],
                              date_of_birth=ppl['dob'],
                              email=ppl['email_address'],
                              phone=ppl['phone'],
                              street_number=street_number,
                              street_name=street,
                              zip=ppl['home_zip_code'])

            # if there is an error on the upsert person....
            if isinstance(up, tuple):
                logger.info("Issue loading to VAN!")
                ids = [ppl['id'], 0, timestamp, 'Error', 'Error']
            else:
                logger.info(f"Loaded to VAN as {up['vanId']}...")
                # make sure this person hasn't already been loaded
                dup_check = rs.query(f"select * from {log_table} where vanid={up['vanId']}")
                if dup_check.num_rows == 0: # Apply the Survey Response if they are not in log table
                    try:
                        sq = van.apply_survey_response(id=up['vanId'],
                                        id_type='vanid',
                                        survey_question_id=sq_id,
                                        survey_response_id=sr_id,
                                        contact_type_id=ct_id,
                                        date_canvassed=ppl['create_time'])
                    except: # just in case there is an error
                        sq = 'Error'

                    if isinstance(sq, bool)==False: #this is catching sq load error
                        sq = 'Error'

                    if ppl['opt_in_volunteer']=='True': #we're going to add volunteer information
                        logger.info("We have a volunteer!")
                        try:
                            vol = van.apply_survey_response(id=up['vanId'],
                                            id_type='vanid',
                                            survey_question_id=vol_sq_id,
                                            survey_response_id=vol_sr_id,
                                            contact_type_id=ct_id,
                                            date_canvassed=ppl['create_time'])
                        except: # just in case there is an error
                            vol = 'Error'

                        if isinstance(vol, bool)==False: #this is catching sq load error
                            vol = 'Error'
                    else:
                        vol = 'False'
                                     
                    if ppl['status']=='complete': #we're going to add information on if people completed the form
                        logger.info("Form completed!")
                        try:
                            stat = van.apply_survey_response(id=up['vanId'],
                                            id_type='vanid',
                                            survey_question_id=stat_sq_id,
                                            survey_response_id=stat_sr_id,
                                            contact_type_id=ct_id,
                                            date_canvassed=ppl['create_time'])
                        except: # just in case there is an error
                            stat = 'Error'

                        if isinstance(stat, bool)==False: #this is catching sq load error
                            stat = 'Error'
                    else:
                        stat = 'False'

                    # Create list of key information for log table
                     ids = [ppl['id'], up['vanId'],timestamp,sq,vol]
                else:
                    ids = [ppl['id'], up['vanId'],timestamp,'NULL','NULL']
                    logger.info(f"{up['vanId']} already loaded to VAN...")

            # Append list to log table
            loaded.append(ids)

        logger.info(f"Finished the loop! Now to load the work we've done into {log_table}")
        load_dict = Table(loaded).to_redshift(log_table,if_exists='append')
    else:
        # if there were 0 records from initial query
        logger.info("No new data to send to EveryAction!")

if __name__ == '__main__':
    main()