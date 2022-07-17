"""
terates over a Date range else uses todays date as default
"""
from datetime import datetime, timedelta
import time
import pytz


def daterange(start_date=None, end_date=None):
    """
    Iterates over a Date range else uses todays date as default
    """
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)
    if start_date is None:
        start_date = local_now + timedelta(-1)
    else:
        start_date = datetime.strptime(start_date, '%d-%m-%Y')
    if end_date is None:
        end_date = local_now + timedelta(1)
    else:
        end_date = datetime.strptime(end_date, '%d-%m-%Y')
    if start_date <= end_date:
        for now in range((end_date - start_date).days + 1):
            yield start_date + timedelta(now)
    else:
        for now in range((start_date - end_date).days + 1):
            yield start_date - timedelta(now)
    return
# for i in daterange():
#    print i.strftime('%d-%m-%Y')
