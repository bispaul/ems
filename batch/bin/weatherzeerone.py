#!/usr/bin/env python
"""
Wunderworld forecast and actual pull
"""
import requests
import argparse
import sys
import json
import logging
# import logging.config
import csv
from datetime import datetime
from datetime import timedelta
import copy
import time
# import types
# import os
import pytz

# path = os.path.dirname(os.path.realpath(__file__))
# logging.config.fileConfig(path + '/weatherlogging.ini')
logging.basicConfig(level=logging.INFO)

SETTINGS = {
    "api_key": "cc50a87d6e3f85a2",
    # "api_key": "572622a6beb9ce94",
    "metric": True
}


def get_max_width(table, i):
    """
        Returns the max width of any row in an array
    """
    # print table
    # print max([len(row[i]) for row in table])
    return max([len(row[i]) for row in table])


def print_table(out, table):
    """
      Aligns and prints an array into a table,
      given that the first
      row in the array contains the column names
    """
    # print len(table[0])
    col_paddings = []
    for i in range(len(table[0])):
        col_paddings.append(get_max_width(table, i))
    print >> out, table[0][0].ljust(col_paddings[0] + 1),
    for i in range(1, len(table[0])):
        col = table[0][i].rjust(col_paddings[i] + 2)
        print >> out, col,
    print >> out, ""
    print >> out, "-" * (sum(col_paddings) + 3 * len(col_paddings))

    table.pop(0)
    for row in table:
        print >> out, row[0].ljust(col_paddings[0] + 1),
        for i in range(1, len(row)):
            col = row[i].rjust(col_paddings[i] + 2)
            print >> out, col,
        print >> out


def make_str(args, options):
    """
        Takes the options and arguments and assembles the query string
    """
    url = ""

    # In the case no options are set, use the default
    if not (args.now or args.hourly or args.alerts or
            args.forecast or args.hourly10day or args.yesterday):
        args.now = True
        args.alerts = True

    if (args.now):
        url += options['now']
    if (args.hourly):
        url += options['hourly']
    if (args.forecast):
        url += options['forecast']
    if (args.alerts):
        url += options['alerts']
    if (args.hourly10day):
        url += options['hourly10day']
    if (args.yesterday):
        url += options['yesterday']
    return url


def parse_alerts(data):
    """
        Takes the returned data and parses the alert messages
    """
    logger = logging.getLogger('weatherzeerone.parse_alerts')
    for alert in data['alerts']:
        logger.info("%s Expires: %s",
                    alert['message'].rstrip("\n"), alert['expires'])


def parse_conditions(data):
    """
        Parses the current conditions from the API
    """
    print "Weather for " + data['display_location']['full']
    print "Currently: " + data['temperature_string'] + " " + data['weather']
    print "Wind: " + data['wind_string']
    print "Humidity: " + data['relative_humidity']


def parse_hourly(data, metric, location):
    """
        Parses the hourly condition data from the API
    """
    logger = logging.getLogger('weatherzeerone.parse_hourly')
    logger.info('Starting Parse Hourly')
    # logger.debug("%s", data)
    # Need to generate an array to send the print_table, first row must be
    # the keys
    val = []
    val.append(["Date", "Hour", "Temperature", "WindSpeed", "WindDir",
                "Humidity", "WindDeg", "Dewpoint", "Conditions",
                "Qpf", "Pop", "CloudCover", "Location"])
    logger.debug(val)
    for item in data:
        # Format the date and temp strings before appending to the array
        timex = item["FCTTIME"]
        # date = time["mon_abbrev"] + " " + time["mday_padded"] + ", "
        # + time["year"]
        date = timex["year"] + "-" + timex["mon_padded"] + "-" + timex["mday"]
        if SETTINGS['metric'] or metric:
            temp = item["temp"]["metric"]  # + u" \u00B0C"
            temp2 = item["wspd"]["metric"]
        else:
            temp = item["temp"]["english"]  # + u" \u00B0F"
            temp2 = item["wspd"]["english"]
        val.append([date, timex['civil'], temp, temp2, item['wdir']['dir'],
                   item['humidity'], item['wdir']['degrees'],
                   item['dewpoint']['metric'], item['condition'],
                   item['qpf']['metric'], item['pop'], item['sky'], location])
        # logger.debug(val)
    # print "\n36 Hour Hourly Forecast:"
    newval = copy.deepcopy(val)
    print_table(sys.stdout, val)
    return newval


def parse_history(data, metric, location):
    """
        Parses the hourly condition data from the API
    """
    logger = logging.getLogger('weatherzeerone.parse_history')
    logger.info('Starting Parse History')
    # logger.debug("%s", data)
    # Need to generate an array to send the print_table, first row must be
    # the keys
    val = []
    val.append(["Date", "Time", "Temperature", "Dewpoint", "Humidity",
                "WindDir", "WindSpeed", "GustSpeed", "Events", "Conditions",
                "Pressure", "WindChill", "HeatIndex", "WindDirDeg",
                "Location"])
    events = ["fog", "rain", "snow", "hail", "thunder", "tornado"]
    # logger.debug(val)
    # logger.debug(data)
    for item in data:
        # logger.debug(item)
        # Format the date and temp strings before appending to the array
        timex = item["date"]
        date = timex["year"] + "-" + timex["mon"] + "-"\
            + timex["mday"]
        timefmt = timex['hour'] + ":" + timex['min']
        rec_time = time.strftime("%I:%M %p", time.strptime(timefmt, '%H:%M'))
        if SETTINGS['metric'] or metric:
            if item["tempm"] != -9999:
                temp = item["tempm"]
            else:
                temp = None
            wspd = item["wspdm"]
            dew = item["dewptm"]
            pressure = item["pressurem"]
            windchill = item["windchillm"]
            heatindex = item["heatindexm"]
            if item["wgustm"] == -9999:
                gustspeed = item["wgustm"]
            else:
                gustspeed = None
            if item["hum"] == -9999:
                humidity = item["hum"]
            else:
                humidity = None
        else:
            if item["tempi"] != -9999:
                temp = item["tempi"]
            else:
                temp = None
            wspd = item["wspdi"]
            dew = item["dewpti"]
            pressure = item["pressurei"]
            windchill = item["windchilli"]
            heatindex = item["heatindexi"]
            if item["wgusti"] == -9999:
                gustspeed = item["wgusti"]
            else:
                gustspeed = None
        windir = item["wdire"]
        winddird = item["wdird"]
        rec_evnt = '-'.join([evnt for evnt in events
                             if int(item.get(evnt)) > 0])
        conditions = item["conds"]
        val.append([date, rec_time, temp, dew, humidity, windir, wspd,
                    gustspeed, rec_evnt, conditions, pressure,
                    windchill, heatindex, winddird, location])
        # logger.debug(val)
    return val


def create_csv(file_dir_fnm, table):
    """
        Write to a seperated flat file
    """
    with open(file_dir_fnm, "wb") as file_write:
        csvwriter = csv.writer(file_write, delimiter=',', quotechar='|',
                               quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerows((row for row in table))
    file_write.close()


def parse_forecast(data, metric, location):
    """
        Parses the forecast data from the API
    """
    # Need to generate an array to send the print_table
    # first row must be the keys
    val = []
    val.append(["Date", "Condition", "Temp (Hi/Lo)", "Wind",
                "Humidity", "Location"])
    for item in data:
        date = item['date']
        date_str = date['monthname'] + " " + str(date['day']) +\
            ", " + str(date['year'])
        if SETTINGS['metric'] or metric:
            temp = item['high']['celsius'] + u" \u00B0C / " +\
                item['low']['celsius'] + u" \u00B0C"
            wind = "~" + str(item['avewind']['kph']) +\
                "kph " + item['avewind']['dir']
        else:
            temp = item['high']['fahrenheit'] + u" \u00B0F / " +\
                item['low']['fahrenheit'] + u" \u00B0F"
            wind = "~" + str(item['avewind']['mph'])\
                   + "mph " + item['avewind']['dir']
        hum = str(item["avehumidity"]) + "%"
        val.append([date_str, item['conditions'], temp, wind, hum, location])
    print "\nWeather Forecast:"
    print_table(sys.stdout, val)


def parse_weather_data(data, args, country=None, city=None, date=None):
    """
        Parses the returned API data along with the options for proper
        formatting
    """
    logger = logging.getLogger('weatherzeerone.parse_weather_data')
    logger.info('{}_{}'.format(country, city))
    data = json.loads(data)
    if country and city:
        location = '{}_{}'.format(country, city)
    elif country and not city:
        location = '{}_{}'.format(country,
                                  ''.join(args.location).replace("/", "_"))
    else:
        location = ''.join(args.location).replace("/", "_")
    if 'error' in data['response']:
        logger.error("Error: %s", data['response']['error']['description'])
        return
    if 'results' in data['response']:
        logger.info("More than 1 city matched your query,"
                    "try being more specific")
        for result in data['response']['results']:
            logger.info("Name: %s State: %s Country %s", result['name'],
                        result['state'], result['country_name'])
        return
    if args.alerts:
        parse_alerts(data)
    if args.now:
        parse_conditions(data['current_observation'])
    if args.hourly:
        table = parse_hourly(data['hourly_forecast'], args.metric, location)
        if args.dir:
            fnm = ''.join([args.dir, "fh36h_", location, "_",
                           str(datetime.now().strftime('%d-%m-%Y%H%M')),
                           ".csv"])
            logger.debug(fnm)
            create_csv(fnm, table)
    if args.hourly10day:
        table = parse_hourly(data['hourly_forecast'], args.metric, location)
        if args.dir:
            fnm = ''.join([args.dir, "fh10d_", location, "_",
                           str(datetime.now().strftime('%d-%m-%Y%H%M')),
                           ".csv"])
            logger.debug(fnm)
            create_csv(fnm, table)
    if args.yesterday:
        # logger.error(data['history'])
        table = parse_history(data['history']['observations'],
                              args.metric, location)
        if args.dir:
            fnm = ''.join([args.dir, "Actuals_yesterday",
                           location, "_",
                           str(datetime.now().strftime('%d-%m-%Y%H%M')),
                           ".csv"])
            logger.debug(fnm)
            create_csv(fnm, table)
    if args.history:
        # logger.error(data['history'])
        table = parse_history(data['history']['observations'],
                              args.metric, location)
        if args.dir:
            fnm = ''.join([args.dir, "Actuals_", location, "_",
                           str(date.strftime('%d%m%Y')),
                           ".csv"])
            logger.debug(fnm)
            create_csv(fnm, table)
    if args.forecast:
        parse_forecast(data['forecast']['simpleforecast']['forecastday'],
                       args.metric, location)


def daterange(start_date=None, end_date=None):
    """
    Iterates over a Date range else uses todays date as default
    """
    logger = logging.getLogger('weatherzeerone.daterange')
    # if type(start_date) is types.StringType\
    #    and type(end_date) is types.StringType:
    if isinstance(start_date, str) and isinstance(end_date, str):
        start_date = datetime.strptime(start_date, '%d-%m-%Y')
        end_date = datetime.strptime(end_date, '%d-%m-%Y')
    if start_date <= end_date:
        for now in xrange(0, (end_date - start_date).days + 1):
            logger.debug(now)
            yield start_date + timedelta(now)
    else:
        raise Exception('Invalid Start and End Date')
    return


def get_country_location(base_url, location):
    """
    Uses Geolookup of Wunderground to get the country and location.
    For cases were instead of location locid:INXX2297 data is provided
    """
    logger = logging.getLogger('weatherzeerone.get_country_location')
    # http://api.wunderground.com/api/cc50a87d6e3f85a2/geolookup/q/CA/San_Francisco.json
    url = '{}geolookup/q/{}.json'.format(base_url, location)
    logger.debug(['Geolookup url: ', url])
    req = requests.get(url)
    data = json.loads(req.content)
    logger.debug(['Geolookup Data: ', data])
    if 'error' in data['response']:
        logger.error("Error: %s", data['response']['error']['description'])
        return
    if 'results' in data['response']:
        logger.info("More than 1 city matched your query,"
                    "try being more specific")
        for result in data['response']['results']:
            logger.info("Name: %s State: %s Country %s", result['name'],
                        result['state'], result['country_name'])
        return
    logger.debug(['Geolookup Data parse: ', data['location']["country_name"],
                 data['location']["city"]])
    country = data['location']["country_name"]
    city = data['location']["city"]
    return(str(country).lower(), str(city).lower())


def main(args):
    """
        Main Call
    """
    print args
    logger = logging.getLogger('weatherzeerone.main')
    logger.debug("Dir %s", args.dir)
    logger.debug("Location %s", args.location)
    # API methods
    values = {
        "now": "conditions/",
        "yesterday": "yesterday/",
        "forecast": "forecast/",
        "hourly": "hourly/",
        "hourly10day": "hourly10day/",
        "alerts": "alerts/",
        "ip": "autoip"
    }
    # Create a location string, or use geoip
    query = "q/%s.json"
    if args.location:
        query = query % "_".join(args.location)
    else:
        query = query % values['ip']
    logger.debug(query)
    base_url = "http://api.wunderground.com/api/%s/" % SETTINGS['api_key']
    logger.debug(['base_url: ', base_url])
    country, city = get_country_location(base_url, ''.join(args.location))
    logger.info([country, city])
    if args.history:
        # logger.debug(args.start_date)
        # logger.debug(args.end_date)
        for date in daterange(args.start_date, args.end_date):
            url = base_url +\
                "history_%s/" % date.strftime('%Y%m%d') +\
                query
            logger.debug(url)
            req = requests.get(url)
            logger.info([country, city])
            parse_weather_data(req.content, args, date=date,
                               country=country, city=city)
            time.sleep(8)
    else:
        url = base_url + make_str(args, values) + query
        logger.debug(url)
        # Make the API request, parse the data
        req = requests.get(url)
        logger.info([country, city])
        parse_weather_data(req.content, args, country=country, city=city)
    # r = open("C:/Users/Biswadip/Documents/
    # My WorkArea/Weather/wunderground/json23rdApril2013kota.json", 'r').read()
    # parse_weather_data(r, args)


if __name__ == "__main__":
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    PARSER = argparse.ArgumentParser(description=("Display the current weather"
                                     ", or forecast"))
    PARSER.add_argument('location', nargs='*', help=('Optional location,'
                        'by default uses geoip'))
    PARSER.add_argument('-n', '--now', help=('Get the current conditions'
                        '(Default)'), action='store_true')
    PARSER.add_argument('-f', '--forecast', help='Get the current forecast',
                        action='store_true')
    PARSER.add_argument('-ho', '--hourly', help='Get the hourly forecast',
                        action='store_true')
    PARSER.add_argument('-h10', '--hourly10day', help=('Get the hourly 10 day'
                        'forecast'), action='store_true')
    PARSER.add_argument('-y', '--yesterday', help='Get the actuals yesterday',
                        action='store_true')
    PARSER.add_argument('-a', '--alerts', help=('View any current weather'
                        'alerts (Default)'), action='store_true')
    PARSER.add_argument('-d', '--dir', dest='dir', help=('Directory were the'
                        'file will be saved'))
    PARSER.add_argument('-hi', '--history', help=('View historical weather'
                        'file will be saved'), action='store_true')
    PARSER.add_argument('-b', '--strdt', dest='start_date',
                        help=('Start of Date to crawl the data.'
                              'Optional default is todays date in IST'),
                        # default=datetime.now() + timedelta(-1))
                        default=local_now + timedelta(-1))
    PARSER.add_argument('-e', '--enddt', dest='end_date',
                        help=('End of Date to crawl the data.'
                              'Optional default is todays date in IST'),
                        # default=datetime.now())
                        default=local_now)
    if SETTINGS['metric']:
        PARSER.add_argument('-m', '--metric', help=('Use metric units instead'
                            'of English units (Default)'), action='store_true')
    else:
        PARSER.add_argument('-m', '--metric', help=('Use metric units instead'
                            'of English units'), action='store_true')
    main(PARSER.parse_args())
