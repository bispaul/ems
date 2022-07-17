"""
Crawls https://www.sldcguj.com/Operation/Generator_ScheduleNew.php.

Gets the Genrator Scheduel.
"""

import requests
from bs4 import BeautifulSoup
import re
import dateutil.parser
import petl
import dbconn
from datetime import datetime


def get_rev(soup):
    """Get the Revision."""
    for sl in soup.find_all('select'):
        if sl.get('name') == 'revision':
            return [opt["value"] for opt in sl.select('option')]
    return None


def post_data_prep(revlist, date):
    """Post Data Dict Prep."""
    if revlist:
        post_dict = {'day': date.day,
                     'month': date.month,
                     'year': date.year,
                     'revision': revlist[-1],
                     'station[]': 0,  # All Stations
                     'IsSubmit': 'true'}
    else:
        post_dict = {'day': date.day,
                     'month': date.month,
                     'year': date.year,
                     'station[]': 0,  # All Stations
                     'IsSubmit': 'true'}
    return post_dict


def list_of_toi(table_of_interest):
    """Make a List out of a HTML table."""
    data = []
    rows = table_of_interest.find_all("tr")
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.get_text(strip=True) for ele in cols]
        # Get rid of empty values
        data.append([re.sub(r"\n", "", ele, re.U, re.S)
                     for ele in cols if ele])
    return data


def genschedule_parse(schdata):
    """
    Parse the list of string data as per the keywords.

    The input is a list of lists containg string values.
    Output is a dictionary.
    """
    data = {}
    date_reg_exp = re.compile('\d{1,2}[-/]\d{1,2}[-/]\d{4}')
    headerrowindx = None
    for j, row in enumerate(schdata):
        for i, ele in enumerate(row):
            if ele.startswith('For the Date'):
                matches_list = date_reg_exp.findall(row[i + 2])
                data['DATE'] = matches_list[0]
            if ele.startswith('Revision No'):
                data['REVISION'] = row[i + 2]
            if ele.startswith('TIMEBLOCK'):
                data['HEADER'] = row
                headerrowindx = j
                break
            if headerrowindx and j > headerrowindx:
                if row[0].startswith(tuple([str(i) for i in range(1, 97)])):
                    data['DATAROW' + row[0].strip()] = row
                    break
    return data


def conv_for_db(datadict, discom, state):
    """Convert Dict to list of lists."""
    datatoload = []
    headerrow = ['DATE', 'REVISION']
    headerrow.extend(datadict.get('HEADER'))
    headerrow.extend(['DISCOM', 'STATE'])
    datatoload.append(headerrow)
    for rownum in xrange(1, 97):
        row = []
        date = dateutil.parser.parse(datadict.get('DATE'))
        row.append(date)
        row.append(int(datadict.get('REVISION')))
        datarow = [int(val) if i == 0 else float(val)
                   for i, val in
                   enumerate(datadict.get('DATAROW' + str(rownum)))]
        row.extend(datarow)
        row.extend([discom, state])
        datatoload.append(row)
    return datatoload


def update_db(dsn, data):
    """Load the data in the scada staging table."""
    sql = """INSERT INTO `power`.`tentative_schedule_staging`
             (declared_date,
              date,
              block_no,
              discom,
              state,
              generator_name,
              tentative_generation,
              generation_type,
              generation_entity_name
              )
             VALUES (CURDATE(),
                     %s,
                     %s,
                     %s,
                     %s,
                     %s,
                     %s,
                     %s,
                     %s)
        on duplicate key update
        tentative_generation = values(tentative_generation),
        processed_ind = 0,
        load_date = NULL"""
    conn = dbconn.connect(dsn)
    datacursor = conn.cursor()
    try:
        print data
        rv = datacursor.executemany(sql, data)
        print("Return Value %s" % str(rv))
        print("Load Status %s" % str(conn.info()))
        conn.commit()
        datacursor.close()
    except Exception as error:
        conn.rollback()
        datacursor.close()
        print("Error %s" % str(error))


def get_generator_schedule(dsn, date, discom='GUVNL', state='GUJARAT'):
    """Get the Generation Schedule."""
    date = datetime.strptime(date, '%d-%m-%Y')
    user_agent = ('Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36'
                  ' (KHTML, like Gecko) Chrome/41.0.2228.0'
                  ' Safari/537.36')
    head = {"User-Agent": user_agent,
            "X-Requested-With": "XMLHttpRequest"}
    url = "https://www.sldcguj.com/Operation/Generator_ScheduleNew.php"
    url2 = \
        "https://www.sldcguj.com/Operation/Generator_Schedule_report_New.php"
    url_html = requests.get(url, headers=head).text
    soup = BeautifulSoup(url_html, 'lxml')
    revlist = get_rev(soup)
    post_dict = post_data_prep(revlist, date)
    url_html2 = requests.post(url, data=post_dict).text
    soup2 = BeautifulSoup(url_html2, 'lxml')
    revlist2 = get_rev(soup2)
    post_dict2 = post_data_prep(revlist2, date)
    url_html2 = requests.post(url2, data=post_dict2, headers=head).text
    soup2 = BeautifulSoup(url_html2, 'html5lib')
    table_of_interest = soup2.body.table.tbody
    loi = list_of_toi(table_of_interest)
    doi = genschedule_parse(loi)
    schedule_data = conv_for_db(doi, discom, state)
    table = petl.cutout(schedule_data, 'Total')
    table2 = petl.melt(table, key=['DATE', 'REVISION',
                                   'TIMEBLOCK', 'DISCOM', 'STATE'])
    table3 = petl.rename(table2, {'variable': 'GENERATOR_NAME',
                                  'value': 'SCHEDULE'})
    table4 = petl.cutout(table3, 'REVISION')
    table5 = petl.addfield(table4, 'GENERATION_TYPE', None)
    table6 = petl.addfield(table5, 'GENERATION_ENTITY_NAME',
                           lambda rec: rec['GENERATOR_NAME']
                           [0: rec['GENERATOR_NAME'].find('[')])
    dbloaddata = list(table6)[1:]
    update_db(dsn, dbloaddata)


# dsn = '../config/sqldb_connection_config.txt'
# get_generator_schedule(dsn, '01-06-2017')
