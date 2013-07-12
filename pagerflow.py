#!/usr/bin/env python

import tz
import sys
import json
import time
import calendar
import requests
import HTMLParser
import ConfigParser
from BeautifulSoup import BeautifulSoup
from datetime import datetime, timedelta, tzinfo


PD_API_URL = None
PD_API_KEY = None
VIEW = None
DB_URL = None
DB_ID = None
DB_PASSWD = None
LOG_FILE = None


def config_parse(settings_file):
    config = ConfigParser.ConfigParser()
    config.read(settings_file)

    global PD_API_URL, PD_API_KEY, VIEW, DB_URL, DB_ID, DB_PASSWD, LOG_FILE

    PD_API_URL = config.get('PAGER_DUTY_API','URL')
    PD_API_KEY = config.get('PAGER_DUTY_API','KEY')
    VIEW = config.get('VIEWS','UNRESOLVED')
    DB_URL = config.get('DB','URL')
    DB_ID = config.get('DB','ID')
    DB_PASSWD = config.get('DB','PASSWD')
    LOG_FILE = config.get('LOG_FILE', 'NAME')


def _do_pagerduty_request(resource, payload=None):
    url = "%s/%s" % (PD_API_URL, '/'.join(resource))
    auth = "Token token=%s" % (PD_API_KEY)
    headers = {"content-type": 'application/json', "Authorization": auth}
    data = dict()
    if payload: 
        data = payload
    data['date_range'] = 'all'
    data['include[]'] = ['channel','service']

    cnt = 0
    while cnt<10:   # retry request for up to 10 times when it fails
        try:
            r = requests.get(url, headers=headers, params=data, verify=False)
            return json.loads(r.text)
            break
        except:
            cnt += 1
            print "retrying request......"


def get_incidents(offset=0, since=None):
    payload = dict()
    if since:
        payload['since'] = since.isoformat()
    payload['sort_by'] = "created_on:asc"
    payload['offset'] = offset
    return _do_pagerduty_request(['incidents'], payload)


def get_count(since=None):
    payload = dict()
    if since:
        payload['since'] = since.isoformat()
    r = _do_pagerduty_request(['incidents', 'count'], payload)
    return int(r['total'])


def get_all_incidents(since=None):
    offset = get_count(since=since)
    fetched = 0
    while True:
        # Get incidents at offset
        incidents = get_incidents(offset=offset,since=since)
        incidents_list = incidents['incidents']
        # yield incidents
        for i in incidents_list:
            yield i

        # Check for more incidents
        fetched += len(incidents['incidents'])
        total = int(incidents['total'])
        limit = int(incidents['limit'])
        print "fetched %d of %d" % (fetched, total)     
        if fetched < total:
            offset -= limit
        else:
            break


def pd_reader(last_run_time): 
    updates_set = set()
    updates = list()
    num_view_updates = 0
    cnt = 0
    # get the new incidents created since the last run from API
    if last_run_time:
        # get unresolved incidents from db view. 
        view = requests.get(VIEW, auth=(DB_ID, DB_PASSWD))
        view = json.loads(view.text)

        # get incidents that need updating that are unresolved, if any.
        for incident in view['rows']:
            # get matching incident from db
            api_incident = _do_pagerduty_request(['incidents', incident['id'].strip("pd:")])
            api_time = unix_time(api_incident['last_status_change_on'])
            db_time = unix_time(incident['value'])
            if db_time < api_time:
                updates_set.add(api_incident['incident_number'])

        num_view_updates = len(updates_set)
        last_run_time = datetime.utcfromtimestamp(last_run_time)
        for incident in get_all_incidents(last_run_time):
            updates_set.add(incident['incident_number'])
        print
        print "unresolved incidents needing update: %s" % num_view_updates
        print "newly created incidents: %s" % (len(updates_set) - num_view_updates) 
    else:
        # initial upload
        for i in range(1, get_count()+1):
            updates_set.add(i)
   
    for i in updates_set:
        updates.append('pd:' + str(i))
    print "total number of updates : %s \n" % len(updates)

    return {'updates':updates, 'num_view_updates':num_view_updates, 
            'num_new_updates':(len(updates) - num_view_updates)}


def get_rev(_id):
    cnt=0
    while cnt<10:
        try:
            r = requests.head(DB_URL + "/" + _id, auth=(DB_ID, DB_PASSWD))  
            if r.status_code == 200:
                return r.headers['etag'].strip('"')
            else:
                return None
        except:
            cnt+=1

        

def get_duration(incident):
    t_created = unix_time(incident['created_on'])
    t_resolved = unix_time(incident['last_status_change_on'])
    return t_resolved - t_created


def parse_html(body):
    body = ''.join(BeautifulSoup(body).findAll(text=True))
    body = body.encode('utf-8').decode('unicode_escape')
    h = HTMLParser.HTMLParser()
    return h.unescape(body)


def get_log(_id):
    try:
        log = _do_pagerduty_request(resource=['incidents', _id, 'log_entries'])
        for entry in log['log_entries']:
            e_type = entry['type']
            # Strip HTML tags from email body
            if e_type == 'trigger' and entry['channel']['type'] == 'email':
                body = entry['channel']['body']
                entry['channel']['untagged_body'] = parse_html(body)

            # Add a local time field for assign entry
            dest = {'assign': 'assigned_user', 'notify': 'user', 'acknowledge': 'agent',
                    'resolve': 'agent', 'escalate': 'assigned_user', 'annotate': 'agent'}
            if e_type in ['assign','notify','acknowledge',
                                    'resolve','escalate','annotate']:
                timezone = entry[dest[e_type]]['time_zone']
                entry['local_created_at'] = from_utc_to(entry['created_at'], timezone) 
        
        log = log['log_entries']
    except:
        log = None
    return log


def doc_builder(incident_id):
    incident = _do_pagerduty_request(resource=['incidents', incident_id.strip("pd:")])
    doc = dict(incident)
    doc['_id'] = ('pd:' + str(incident['incident_number']))
    if doc['status'] == "resolved":
        doc['duration'] = get_duration(incident)
    doc['incident_log'] =  get_log(doc['id'])

    # are we updating an existing or new incident?
    _rev = get_rev(incident_id)
    if _rev:
        doc['_rev'] = _rev

    return doc


def update_last_run(current_run, num_new_updates, num_view_updates=0, updates=None):
    log = dict()
    log['finished_at'] =  calendar.timegm(time.gmtime())
    log['started_at'] = current_run
    log['new_incidents'] = num_new_updates
    log['total_updates'] = num_view_updates + num_new_updates
    log['updates_from_unresolved_view'] = num_view_updates
    if (updates):
        log['updated_incidents'] = updates
    try:
        f = open(LOG_FILE, 'r')
        data = json.load(f)
        log['index'] = len(data['history']) + 1
        f.close()
    except:  # if its the first log(no log file)
        data = dict()
        data['history'] = []
        log['index'] = 1

    f = open(LOG_FILE, 'w')
    data['last_run'] = current_run
    data['number_of_executions'] = log['index']
    data['history'].append(log)
    json.dump(data, f)
    f.close()


def get_last_run():
    try:
        with open(LOG_FILE, 'r') as json_data:  
            data = json.load(json_data)
            # obtain current state
            last_run = data['last_run']
            json_data.close()
            return int(last_run)
    except:
        return 0


def upload(json_doc):
    headers = {"content-type": "application/json"}
    cnt = 0
    while cnt<10:   # retry request up to 10 times if fails.
        try:
            resp = requests.post(DB_URL, auth=(DB_ID, DB_PASSWD), data=json.dumps(json_doc), headers=headers)
            print resp
            return resp.status_code in [201, 202]
            break
        except:
            cnt+=1
    return None


def unix_time(timestamp):
    return calendar.timegm(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ"))


def from_utc_to(date_str, timezone):
    offset_str = tz.timezones[timezone]
    date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
    hours = int(offset_str.lstrip('+,-')[:2])
    minutes = int(offset_str.lstrip('+,-')[2:])
    sign = -1 if offset_str.startswith('-') else 1
    offset = timedelta(hours=hours, minutes=minutes) * sign
    return (date+offset).isoformat()


def main():
    failed_incidents = list()
    current_run = calendar.timegm(time.gmtime())
    last_run = get_last_run()
    print "last run : %s" % last_run

    output = pd_reader(last_run)
    for incident_id in output['updates']:
        json_doc = doc_builder(incident_id)
        print "uploading " + str(incident_id),
        if not upload(json_doc):
            failed_incidents.append(incident_id)
    if last_run:
        update_last_run(
            current_run, 
            output['num_new_updates'],
            output['num_view_updates'], 
            output['updates']
        )
    else: # initial upload
        update_last_run(current_run, get_count())
        print "initial ",

    print 'upload complete.'
    if failed_incidents:
        print "Incidents failed to upload : ",
        for i in failed_incidents:
            print i,
            print ", "


if __name__=='__main__':
    config_parse(sys.argv[-1])
    main()


