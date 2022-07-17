"""
Crawls https://sldcguj.com/RealTimeData/RealTimeDemand.php.

Gets the Realtime DEmand and Generation.
"""
import requests
from bs4 import BeautifulSoup
import re
import dbconn
import argparse
import os


def parse_list(rldc_list):
    """
    Parse the list of string data as per the keywords.

    The input is a list of lists containg string values.
    Output is a dictionary.
    """
    dt_regex = '\d{1,2}[-\/]\d{1,2}[-\/]\d{4}[ ]*\d{1,2}[:]\d{1,2}[:]\d{1,2}'
    date_reg_exp = re.compile(dt_regex)
    data = {}
    for j, row in enumerate(rldc_list):
        for i, ele in enumerate(row):
            if ele == 'Grid Frequency' and len(row) == 3 and \
                    not data.get('FREQUENCY'):
                data['FREQUENCY'] = float(row[2].strip('Hz'))
            elif ele.startswith('GujaratCateredMW')and \
                    not data.get('CATERED_LOAD'):
                data['CATERED_LOAD'] = int(row[i + 1].strip())
            elif date_reg_exp.match(ele) and \
                    not data.get('DATE_TIME'):
                matches_list = date_reg_exp.findall(ele)
                data['DATE_TIME'] = matches_list[0]
            elif ele.startswith('DSM Rate(Rs./Unit):') and \
                    not data.get('UIRATE'):
                data['UIRATE'] = float(ele.split(':')[1].strip())
            elif ele.startswith('GujaratAt periphery') and \
                    not data.get('WRLDC') and \
                    not data.get('SCHEDULE') and \
                    not data.get('UI'):
                data['WRLDC'] = int(row[i + 1].strip())
                data['SCHEDULE'] = int(row[i + 2].strip())
                data['UI'] = int(row[i + 3].strip())
            elif ele.startswith('DGVCL') and not data.get('DGVCL'):
                data['DGVCL'] = int(row[i + 1].strip())
            elif ele.startswith('MGVCL') and not data.get('MGVCL'):
                data['MGVCL'] = int(row[i + 1].strip())
            elif ele.startswith('UGVCL') and not data.get('UGVCL'):
                data['UGVCL'] = int(row[i + 1].strip())
            elif ele.startswith('PGVCL') and not data.get('PGVCL'):
                data['PGVCL'] = int(row[i + 1].strip())
            elif ele.startswith('TPAECo') and not data.get('TPAECo'):
                data['TPAECo'] = int(row[i + 1].strip())
            elif ele.startswith('TPSECo') and not data.get('TPSECo'):
                data['TPSECo'] = int(row[i + 1].strip())
            elif ele.startswith('GSECL Total Generation') and \
                    not data.get('GSECLGEN'):
                data['GSECLGEN'] = int(row[i + 1].strip())
            elif ele.startswith('Public owned IPP Generation') and \
                    not data.get('PUBLICIPPGEN'):
                data['PUBLICIPPGEN'] = int(row[i + 1].strip())
            elif ele.startswith('Private IPP Generation') and \
                    not data.get('PRIVATEIPPGEN'):
                data['PRIVATEIPPGEN'] = int(row[i + 1].strip())
            elif ele.startswith('Torrent Power') and \
                    not data.get('TORRENTPOWERGEN'):
                data['TORRENTPOWERGEN'] = int(row[i + 1].strip())
            elif ele.startswith('Gujarat Total Conventional Generation') and \
                    not data.get('TOTCONVGEN'):
                data['TOTCONVGEN'] = int(row[i + 1].strip())
            elif ele.startswith('(Wind+Solar) Generation') and \
                    not data.get('WINDGEN') and not data.get('SOLARGEN'):
                wind_solar = row[i + 1].strip().split('+')
                data['WINDGEN'] = int(wind_solar[0])
                data['SOLARGEN'] = int(wind_solar[1])
    return data


def update_db(dsn, data):
    """Load the data in the scada staging table."""
    sql = """INSERT INTO `power`.`scada_staging`
            (
            `datetime`,
            `type`,
            `entity_name`,
            `quantum`,
            `unit`,
            `discom`,
            `state`,
            `source_name`
            )
            values (STR_TO_DATE(%s,'%%d-%%m-%%Y %%H:%%i:%%s'),
             %s, %s, %s, %s, %s, %s, %s)
            on duplicate key update
            quantum = values(quantum),
            processed_ind = 0,
            load_date = NULL"""
    conn = dbconn.connect(dsn)
    datacursor = conn.cursor()
    try:
        # print data
        rv = datacursor.executemany(sql, data)
        print("Return Value %s" % str(rv))
        print("Load Status %s" % str(conn.info()))
        conn.commit()
        datacursor.close()
    except Exception as error:
        conn.rollback()
        datacursor.close()
        print("Error %s" % str(error))


def con_dict_to_dblistrow(datadict, discom, state, source, stype='UNKNOWN'):
    """Convert the dict to a list of lists to load into DB."""
    datatoload = []
    unit_dict = {'FREQUENCY': 'Hz',
                 'UIRATE': 'Rs/KWh'}
    for key, value in datadict.iteritems():
        row = []
        if key == "DATE_TIME":
            continue
        else:
            row.append(datadict.get('DATE_TIME'))
            row.append(stype)
            row.append(key)
            row.append(value)
            row.append(unit_dict.get(key, 'MW'))
            row.append(discom)
            row.append(state)
            row.append(source)
            datatoload.append(row)
    return datatoload


def html_table_to_list(table_of_interest):
    """Convert the HTML Table to Python List."""
    # Get the HTML Table Data.
    data = []
    rows = table_of_interest.find_all("tr")
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.get_text(strip=True) for ele in cols]
        data.append([re.sub(r"\n", "", ele, re.U, re.S)
                     for ele in cols])
    return data


def parse_list_wind(wind_list):
    """
    Parse the list of string data as per the keywords.

    The input is a list of lists containg string values.
    Output is a dictionary.
    """
    dt_regex = '\d{1,2}[-\/]\d{1,2}[-\/]\d{4}[ ]*\d{1,2}[:]\d{1,2}[:]\d{1,2}'
    date_reg_exp = re.compile(dt_regex)
    data = {}
    plantname_indx = None
    generation_indx = None
    for row in wind_list:
        if row[0].startswith('SystemFreq'):
            matches_list = date_reg_exp.findall(row[0])
            data['DATE_TIME'] = matches_list[0]
        elif row[0].startswith('Sr No.'):
            if ' '.join(row[1].split()).startswith('Wind Plants'):
                plantname_indx = 1
            if row[3].strip().startswith('Generation(MW)'):
                generation_indx = 3
        elif row[1].startswith('TOTAL'):
            continue
        elif not row[0].startswith('Manual'):
            # Getting rid of \xa0 character
            plant_name = row[plantname_indx].encode('ascii', 'ignore')
            data[plant_name] = int(row[generation_indx])
        else:
            plant_name = row[0].encode('ascii', 'ignore')
            data[plant_name] = row[1]
    return data


def get_rtd_wind(dsn, discom='GUVNL', state='GUJARAT', source='WEB'):
    """Get the Wind Data Plant Wise."""
    url = "https://sldcguj.com/RealTimeData/wind.php"
    source_code = requests.get(url).text
    soup = BeautifulSoup(source_code, 'html5lib')
    table_of_interest = soup.body.table
    data = html_table_to_list(table_of_interest)
    dict_of_int = parse_list_wind(data)
    dbdata = con_dict_to_dblistrow(dict_of_int, discom, state, source, 'WIND')
    update_db(dsn, dbdata)


def detect_gen_name(txt):
    """Detect where Generation of Multiple Units are concatenated."""
    re1 = '(\\(I.*\\))'
    rg = re.compile(re1, re.IGNORECASE | re.DOTALL)
    m = rg.search(txt)
    if m:
        rbraces1 = m.group(1)
        return rbraces1
    else:
        return None


def fix_string(fulltxt):
    """Seperate the Generators and fix the name."""
    rbrace = detect_gen_name(fulltxt)
    st = []
    if rbrace:
        indx = fulltxt.find(rbrace)
        name_str = fulltxt[:indx]
        rb = rbrace.split('+')
        # print rb
        for r in rb:
            if r.startswith('(') and r.endswith(')'):
                st.append(name_str + r)
            elif r.startswith('(') and not r.endswith(')'):
                st.append(name_str + r + ')')
            elif not r.startswith('(') and not r.endswith(')'):
                st.append(name_str + '(' + r + ')')
            else:
                st.append(name_str + '(' + r)
        return st
    else:
        return fulltxt.strip().split('+')


def parse_list_intgen(rldc_list, gen_list):
    """
    Parse the list of string data as per the keywords.

    The input is a list of lists containg string values.
    Output is a dictionary.
    """
    dt_regex = '\d{1,2}[-\/]\d{1,2}[-\/]\d{4}[ ]*\d{1,2}[:]\d{1,2}[:]\d{1,2}'
    date_reg_exp = re.compile(dt_regex)
    data = {}
    for j, row in enumerate(rldc_list):
        for i, ele in enumerate(row):
            ele = ele.encode('ascii', 'ignore')
            if date_reg_exp.match(ele) and not data.get('DATE_TIME'):
                matches_list = date_reg_exp.findall(ele)
                data['DATE_TIME'] = matches_list[0]
            elif ele in gen_list:
                if '+' in ele and '+' in row[i + 3]:
                    # gen_name = ele.strip().split('+')
                    gen_name = fix_string(ele)
                    gen_val = row[i + 3].strip().split('+')
                    for i, gn in enumerate(gen_name):
                        if not data.get(gn):
                            data[gn.strip()] = int(gen_val[i].strip())
                elif not data.get(ele):
                    data[ele] = int(row[i + 3].strip())
    return data


def get_rtd_agg(dsn, discom='GUVNL', state='GUJARAT', source='WEB'):
    """Get the Aggregated Demand and Generation."""
    url = "https://sldcguj.com/RealTimeData/RealTimeDemand.php"
    source_code = requests.get(url).text
    soup = BeautifulSoup(source_code, 'html5lib')
    # table_of_interest = soup.body.table.tbody
    tables = soup.findAll("table")
    table_of_interest = tables[2]
    data = html_table_to_list(table_of_interest)
    try:
        dict_of_int = parse_list(data)
        dbdata = con_dict_to_dblistrow(dict_of_int, discom,
                                       state, source, 'AGGREGATE')
        update_db(dsn, dbdata)
    except Exception as err:
        print "Error AGG: " + str(err)
    try:
        int_gen = ['Ukai(1-5)+Ukai6', 'Wanakbori', 'Gandhinagar',
                   'Sikka(1-2)+Sikka(3-4)',
                   'KLTPS(1-3)+KLTPS4', 'Utran(Gas)(II)',
                   'Dhuvaran (Gas)(I)+(II)',
                   'Ukai (Hydro)', 'Kadana (Hydro)']
        dict_of_int_ig = parse_list_intgen(data, int_gen)
        dbdata = con_dict_to_dblistrow(dict_of_int_ig, discom,
                                       state, source, 'INTERNAL')
        update_db(dsn, dbdata)
    except Exception as err:
        print 'Error INT: ' + str(err)
    try:
        ipp_gen = ['GIPCL(I)', 'SLPP(I+II)', 'Akrimota',
                   'GSEG(I+II)', 'GPPC', 'CLPI', 'TPAECo',
                   'EPGL(I+II)', 'Sugen+Unosgn',
                   'Adani(I+II+III)', 'BECL(I+II)', 'CGPL',
                   'KAPP', 'KAWAS', 'JHANOR', 'SSP(RBPH)']
        dict_of_int_ipp = parse_list_intgen(data, ipp_gen)

        dbdata = con_dict_to_dblistrow(dict_of_int_ipp, discom,
                                       state, source, 'INTERNAL')
        update_db(dsn, dbdata)
    except Exception as err:
        print 'Error IPP: ' + str(err)


def main(args):
    """Main Job."""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if args.ftype == 'Main':
        filename = 'get_rtd_agg.lock'
        file = os.path.join(dir_path, filename)
        if not os.path.isfile(filename):
            try:
                open(file, 'w')
                get_rtd_agg(args.dsn)
                os.remove(file)
            except Exception as err:
                print "Error get_rtd_agg: " + str(err)
                os.remove(file)
        else:
            pass
    elif args.ftype == 'Wind':
        filename = 'get_rtd_wind.lock'
        file = os.path.join(dir_path, filename)
        if not os.path.isfile(filename):
            try:
                open(file, 'w')
                get_rtd_wind(args.dsn)
                os.remove(file)
            except Exception as err:
                print "Error get_rtd_wind: " + str(err)
                os.remove(file)
        else:
            pass
    # dsn = '../config/sqldb_connection_config.txt'
    # get_rtd_agg(dsn)
    # get_rtd_wind(dsn)


if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description="Fetches GUJSLDC data and\
                                     Uploads Staging table data")
    ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
                     help='Full path and the file name of the db config',
                     required=True)
    ARG.add_argument('-t', '--type', dest='ftype',
                     help=('Wind or Conventional Gen Schedule'
                           '["Wind", "Main"]'), required=True)
    main(ARG.parse_args())
