'''
main sampling and feed file writing--convert pool based manual customers to a file-based approach;
SFTP send function is a separate file, currently;

***TO DO:
currently, process assigns member records an ID based on what the cust_in file is named; this is a problem if the file is incorrectly named, overwrite the wrong file, etc
***

'''
import os
import datetime
from datetime import timedelta
import csv
import json
import random
import smtplib
import hashlib
import psycopg2
from email.message import EmailMessage
import pandas as pd

email_add = os.environ.get('G_USER')
email_pw = os.environ.get('G_PASS')
email_to = os.environ.get('MAIL_TO')

basePath = os.environ.get('BASE_PATH_MAN')

psql_pass = os.environ.get('PSQL_PASS')

output_headers = ['Last name', 'First name', 'Age', 'Gender', 'Member Id', 'Phone', 'Email', 'Member Status', 'Frequent Club Id', 'Date Joined', 'Last visit date', 'Membership Type']

with open('location_admin.json', 'r') as f:
    cust_data = json.load(f)

mail_me = False
###
test_mode = False  # if True: do not write quarantine, do not write feed files, do not write stats1;
###
quar_days = 90

loc_on_list = ['ABCLOCATION1', 'ABCLOCATION2']


def glob_cust_init(index, loc_map):

    global customer, location_identifier, fileBase, fileForm, customer_inFile, birthFormat, joinDateFormat, lastVisFormat, dayfirst, loc_invites, max_invites

    max_invites = 50  # global max, see "admin" file for individual location max

    # open "admin" file that contains location config/info
    customer = cust_data['customers'][index]
    location_identifier = loc_map['ABCID']
    invite_adjust = loc_map['invite_adjust']
    fileBase = location_identifier + '.csv'
    fileForm = location_identifier + '_form.csv'
    customer_inFile = basePath + 'cust_in/' + fileBase

    birthFormat = customer.get('time_format_birthday')  # value or None
    joinDateFormat = customer.get('time_format_join_date')
    lastVisFormat = customer.get('time_format_lastVisit')
    dayfirst = customer.get('dayfirst')  # True / False

    loc_invites = customer.get('max_invites_loc') + invite_adjust  # NEED TO UPDATE FROM loc_map

    if loc_invites < max_invites:  # if lower max, pull from locations admin file
        max_invites = loc_invites

    print(f'\nloc invites: {loc_invites}')
    print(f'nmax invites: {max_invites}')


def mailMe(content):
    '''main smtp mail function'''
    msg = EmailMessage()
    msg['Subject'] = 'Manual Processing TEST Reports/Errors'
    msg['From'] = email_add
    msg['To'] = email_to
    msg.set_content(content)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(email_add, email_pw)

        smtp.send_message(msg)


def hasher(stringIn):

    hash_string = stringIn.encode('utf-8')  # encode() creates 'byte' object type?
    h = hashlib.sha256(hash_string).hexdigest()
    return(h)


def ageCalc(date, date_format):
    now = datetime.datetime.now()
    then = datetime.datetime.strptime(date, date_format)  # strptime gives string > datetime
    diff = now - then
    diffYears = diff.days // 365
    return diffYears


def TF30D(date, date_format):
    # returns True/False if date 30 days or older
    now = datetime.datetime.now()
    visit = datetime.datetime.strptime(date, date_format)  # strptime gives string > datetime
    diff = now - visit
    if int(diff.days) < 31:
        return True
    else:
        return False


def clean_headers(cust_file):
    with open(cust_file, 'r', encoding='utf-8-sig') as e:
        myData = list(csv.reader(e))
        strippedHeaders = []
        for i in myData[0]:
            strippedHeaders.append(i.strip())
        return strippedHeaders


def check_headers(cust_file):
    prev_headers = customer.get('header_check')
    curr_headers = clean_headers(cust_file)
    error_message_headers = f'''\nERROR: Headers for customer {customer.get('location')}-{location_identifier} have changed;\nFILE NOT SAMPLED--check file and make corrections
    '''
    if prev_headers == curr_headers:
        return True
    else:
        return error_message_headers


def form_file(cust_file):
    '''
    weekly or biweekly format of customer file, to be used by daily sampler function
    '''
    with open(cust_file, 'r') as f:

        myDictData = list(csv.DictReader(f, fieldnames=clean_headers(cust_file)))

        lenHeaders = len(output_headers)
        row = []
        rowList = []
        ph_date = datetime.datetime(1901, 1, 1)

        for line in myDictData[1:]:

            row = [None] * lenHeaders  # build empty row with None placeholders

            for i in range(lenHeaders):
                if customer.get(output_headers[i]):  # check for value--if none, skip it
                    row[i] = line[customer.get(output_headers[i])]

            if customer.get('time_format_birthday'):  # check if bday format, if so, convert bday -> age
                if row[2]:
                    row[2] = ageCalc(row[2], birthFormat)
                    if row[2] < 0:
                        row[2] = row[2] + 100

            if not row[7]:  # mark active if not otherwise specified
                row[7] = 'Active'

            row[8] = location_identifier

            if not row[9]:  # placeholder date
                row[9] = ph_date.strftime(joinDateFormat)
            if not row[10]:  # placeholder date, in order to run TF30D in "rules" below
                row[10] = ph_date.strftime(lastVisFormat)

            rowList.append(row)

        return rowList


def write_form_file():

    with open(basePath + 'cust_in/_form/' + fileForm, 'w') as g:

        formatWriter = csv.writer(g)
        formatWriter.writerow(output_headers)

        for row in form_file(customer_inFile):

            if row[2] and not customer.get('time_format_birthday'):
                row[2] = int(row[2])  # string -> int for rule check below

            rules = [
                # AGE IS 18 OR OVER
                row[2] not in range(18),
                # HAS EMAIL
                '@' in row[6],
                # STATUS IS ACTIVE
                row[7].lower() == 'active',
                # CHECK FOR LAST 30 HERE, RATHER THAN IN SAMPLER? (FEWER _FORM EMAILS ON SERVER?)
                TF30D(row[10], lastVisFormat)
            ]

            if all(rules):

                if not row[11]:  # check for employee in membership type (if type present)
                    formatWriter.writerow(row)
                elif 'employee' not in row[11].lower():
                    formatWriter.writerow(row)
                else:
                    continue


def format_form_file():
    '''
    Format dates, opens same "_form" file written out above; maybe add this to the sampler() def instead?
    Removes dup emails;
    Converts all emails to lower case;
    '''
    df = pd.read_csv(basePath + 'cust_in/_form/' + fileForm, index_col=0, dtype={'Member Id': str, 'Age': str, 'Phone': str})
    if dayfirst:
        df["Date Joined"] = pd.to_datetime(df["Date Joined"], dayfirst=True, format=joinDateFormat)
        df['Last visit date'] = pd.to_datetime(df['Last visit date'], dayfirst=True, format=lastVisFormat)
    else:
        df["Date Joined"] = pd.to_datetime(df["Date Joined"])
        df['Last visit date'] = pd.to_datetime(df['Last visit date'])
    df['Date Joined'] = df['Date Joined'].dt.strftime('%Y-%m-%d')
    df['Last visit date'] = df['Last visit date'].dt.strftime('%Y-%m-%d')
    df['Email'] = df['Email'].str.lower()  # remove caps from _form file (customer input/records)
    df2 = df.sort_values('Last visit date', ascending=False)  # sort by last visit date
    df2.drop_duplicates('Email', inplace=True)  # remove duplicate emails
    df2.to_csv(basePath + 'cust_in/_form/' + fileForm, encoding='utf-8', sep='|')


def sampler(max_invites):
    '''
    sample the "_form" file; write records to quarantine file; write daily feed file;
    '''
    timestamp = datetime.datetime.now().strftime('%y%m%d.%H%M%S')

    emailsQuarSet = set()
    emailsPoolSet = set()
    emailsEligSet = set()

    hashList = []
    emailList = []

    try:

        with open(basePath + 'cust_in/_form/' + fileForm, 'r') as h:
            pool = list(csv.reader(h, delimiter='|'))

            for line in pool[1:]:  # check for visit in last 30 days, and not joined in last 30 days

                if TF30D(line[10], '%Y-%m-%d') and not TF30D(line[9], '%Y-%m-%d'):  # check for join date only; add 30 day lastVisit check to _form instead? (fewer emails on server?)

                    hashed_email = hasher(line[6].lower())
                    emailsPoolSet.add(hashed_email)
                    # this is adding entire pool to the hash dict/set/whatever, is that needed?
                    hashList.append(hashed_email)
                    emailList.append(line[6].lower())
                    # zip the two lists, and create a set of those resulting tuples
                    # note this will remove duplicates (of the tuple pairs)
                    zipped = zip(hashList, emailList)
                    hash_email_set = set(zipped)

            now = datetime.datetime.now()
            quar_date = now - timedelta(quar_days)
            quar_date_s = quar_date.strftime('%Y-%m-%d')

            # get quar from postgres
            connect = psycopg2.connect(
                database='manual',
                user='postgres',
                password=psql_pass
            )
            cursor = connect.cursor()
            cursor.execute("SELECT * FROM quar WHERE delivered_on_date > %s", (quar_date_s,))  # need comma after param, to create a tuple?
            data = cursor.fetchall()
            # select first item in tuple, the email hash, at index 0
            for i in data:
                emailsQuarSet.add(i[0])
            cursor.close()
            connect.close()

            dailyCount = len(emailsPoolSet) // 90  # take count before quarantine is subtracted? doesn't change? investigate...

            emailsEligSet = emailsPoolSet - emailsQuarSet

            if 0 < len(emailsEligSet) < max_invites:  # if only a couple records left (make this a try/except later?)
                max_invites = len(emailsEligSet)

            if 0 <= dailyCount < 1:  # if a small fraction, just round up to 1 for daily
                dailyCount = 1

            if dailyCount < max_invites:
                max_invites = dailyCount

            print(f'pool set: {len(emailsPoolSet)}')
            print(f'elig set: {len(emailsEligSet)}')
            print(f'daily count: {dailyCount}')
            print(f'max ivites: {max_invites}')

            # write stats to postgres
            # note this is above randsend, so will write even if count fail
            if not test_mode:
                connect2 = psycopg2.connect(
                    database='manual',
                    user='postgres',
                    password=psql_pass
                )
                cursor2 = connect2.cursor()
                cursor2.execute('INSERT INTO stats1 (identifier, loc_invites, pool_set, elig_set, daily_count, max_invites ) VALUES (%s, %s, %s, %s, %s, %s)', (location_identifier, loc_invites, len(emailsPoolSet), len(emailsEligSet), dailyCount, max_invites))
                connect2.commit()
                cursor2.close()
                connect2.close()

            randsend = random.sample(emailsEligSet, max_invites)

            # THIS IS PROBABLY SLOWING THINGS DOWN--using set of tuples, rather than dict, for bi-dir lookups
            randsend_email = []
            hash_email_set_small = set()
            for i in randsend:
                for tup in hash_email_set:
                    if i in tup:
                        randsend_email.append(tup[1])
                        hash_email_set_small.add(tup)

            # use emails/hashes to (re)build rows for feed file
            df = pd.read_csv(basePath + 'cust_in/_form/' + fileForm, sep='|', dtype={'Member Id': str, 'Age': str, 'Phone': str})
            df['Last visit date'] = pd.to_datetime(df['Last visit date'], format='%Y-%m-%d')  # convert to datetime
            df['Last visit date'] = df['Last visit date'].dt.strftime('%Y-%m-%d')  # reformat to string
            df2 = df[df['Email'].isin(randsend_email)]  # sample for JUST the rows in randsend
            if not test_mode:  # write out feed file if not test mode
                df2.to_csv(basePath + 'cust_out/' + timestamp + fileBase, encoding='utf-8', sep='|')  # save file

            mail_stats = f'''
            identifier = {location_identifier}\nelig set = {len(emailsEligSet)} *note these are pre-sampling numbers\npool set = {len(emailsPoolSet)}\nquar set = {len(emailsQuarSet)}\nrandsend = {len(randsend)}
            '''
            if mail_me:
                mailMe(mail_stats)

        # write quar to postgres
        # note this is below randsend so doesn't write if count fail
        if not test_mode:
            connect = psycopg2.connect(
                database='manual',
                user='postgres',
                password=psql_pass
            )
            cursor = connect.cursor()
            date = datetime.datetime.now()
            for i in hash_email_set_small:
                cursor.execute('INSERT INTO quar (email, delivered_on_date, identifier) VALUES (%s, %s, %s)', (i[0], date, location_identifier))
            connect.commit()
            cursor.close()
            connect.close()

        print(f'sampling finished: {location_identifier}')

    except Exception as e:
        eType = type(e)
        e_message = f'\tERROR PROCESSING FILES FOR LOCATION: {location_identifier},\n\t{eType}\n\tTHE FOLLOWING ERROR OCCURRED: ' + str(e) + '\n'
        if mail_me:
            mailMe(e_message)
        else:
            print(e_message)


def process_cust(index, loc_map):

    glob_cust_init(index, loc_map)
    clean_headers(customer_inFile)  # run weekly
    # use the following line for new club entry 'header_check'; temp comment out below functions while print:
    # print(clean_headers(customer_inFile))
    if check_headers(customer_inFile) is True:  # run weekly
        form_file(customer_inFile)  # run weekly
        write_form_file()  # run weekly
        format_form_file()  # run weekly
        sampler(max_invites)  # run daily
    else:
        if mail_me:
            mailMe(check_headers(customer_inFile))
        else:
            print(check_headers(customer_inFile))


def main():
    for c_key in to_split.keys():
        split_multi(c_key)

    for k, v in cust_data.get('customers').items():
        for loc_map in v.get('loc_ID_map').values():
            ID = loc_map.get('ABCID')
            if ID in loc_on_list:
                process_cust(k, loc_map)


if __name__ == "__main__":
    main()


'''
NOTES

need better method for "daily count", or just remove? -- //90 is too low in some cases, probably because we're only getting a few weeks or month of data

psql password not working/ not enabled? (locally, not sure about prod)

put split multi in try/except

postgres: func to remove q entries after 120 or 150 days old?

set up unique pw & cred's for M sftp xfer? or okay to use same as other importer (loc_onoff)?


'''
