import ftplib
import os
import shutil
from datetime import datetime
import time
import pytz
from retrying import retry
import pandas as pd
# import numpy as np
import glob
# import dbconn
from sqlalchemy import create_engine
import timeout_decorator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_datetime():
    """Get current day."""
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        timenow = time.time()
        utc_now = datetime.utcfromtimestamp(timenow)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)
    return local_now


def get_file(host, username, password, dir_to_download, filelist):
    """Put a file in the ftp folder."""
    os.chdir(dir_to_download)
    session = ftplib.FTP(host, username, password)
    timestr = get_datetime().strftime("%Y%m%d_%H%M%S")
    for files in filelist:
        rsize = session.size(files)
        logger.info('ini file size: %s %s', files, rsize)
        fname = os.path.basename(files) + '_' + timestr
        sfile = open(fname, 'wb')
        session.retrbinary('RETR ' + files, sfile.write)  # get the file
        lsize = sfile.tell()
        time.sleep(2) # Delay to check if the file size is changing
        rsize2 = session.size(files)
        logger.info('final file size: %s %s %s', files, lsize, rsize2)
        sfile.close()  # close file and FTP
        if not rsize == lsize == rsize2:
            os.remove(fname)
            session.quit()
            raise ValueError('File Size not matching with source.')
        if not os.path.getsize(fname) > 0:
            os.remove(fname)
            session.quit()
            raise ValueError('File Size is qual to 0')
    session.quit()
    return


# def get_file_v2(host, username, password, dir_to_download, filelist):
#     from ftpsync.targets import FsTarget
#     from ftpsync.ftp_target import FtpTarget
#     from ftpsync.synchronizers import DownloadSynchronizer
#     local = FsTarget(dir_to_download)
#     opts = {"force": False, "delete_unmatched": True, "verbose": 3, "dry_run" : False}    
#     for files in filelist:
#         remote = FtpTarget(files, host, username, password)
#         s = DownloadSynchronizer(local, remote, opts)
#         s.run()


@retry(wait_fixed=5000, stop_max_attempt_number=2)
def fetchftpjob(filelist, dir_to_download):
    """FTP job for GUVNL."""
    hostname = '117.247.83.101'
    username = 'sys_anti'
    password = 'sldc@system@123'
    get_file(hostname, username, password, dir_to_download, filelist)


def filenamechange(newfile):
    """
    Change the filename to the base name.
    Exclude the dattime stamp in the file name.
    """
    filename = os.path.basename(newfile)
    findpatt = '.csv_'
    indextocutoff = filename.find(findpatt)
    newfilename = filename[:indextocutoff]
    newfilenamewext = filename[:indextocutoff + (len(findpatt) - 1)]
    return newfilenamewext, newfilename


def filediff(mvdir, archivedir, filetoloadname, newfile):
    """Extract only new rows from the new file."""
    oldfilename = filenamechange(newfile)[0]
    oldfilenamepath = os.path.join(mvdir, oldfilename)
    # print 'filediff**********', newfile, oldfilename, oldfilenamepath
    if os.path.isfile(oldfilenamepath) and os.path.isfile(newfile):
        with open(newfile, 'r') as t2, open(oldfilenamepath, 'r') as t1:
            fileone = t1.readlines()
            filetwo = t2.readlines()

        with open(filetoloadname, 'w') as outfile:
            # header Line
            for i, line in enumerate(filetwo):
                if i == 0:
                    outfile.write(line)
                    continue
                if line not in fileone:
                    outfile.write(line)
    else:
        path = os.path.dirname(newfile)
        shutil.copyfile(newfile, os.path.join(path, filetoloadname))
    filename = os.path.basename(newfile)
    archivedirfile = os.path.join(archivedir, filename[:-7])
    shutil.copy(newfile, archivedirfile)
    # shutil.move(newfile, oldfilenamepath)
    return filetoloadname, newfile, oldfilenamepath


def scada_file_upload_prep(filname, filetype, discom, state, source):
    """Prep the scada file for database load."""
    scada_data = pd.read_csv(filname, sep='\s+', 
                             parse_dates=[['DATE', 'TIME']], dayfirst= True)
    scada_data_melt = pd.melt(scada_data, id_vars=['DATE_TIME'], var_name= 'Variable')
    scada_data_melt.columns = ['Date_Time', 'Variable', 'Quantum']
    scada_data_melt['Discom'] = discom
    scada_data_melt['State'] = state
    scada_data_melt['Source'] = source
    scada_data_melt['Unit'] = 'MW'
    scada_data_melt['Date_Time'] = \
        pd.to_datetime(scada_data_melt['Date_Time'], errors='coerce')
    scada_data_melt['Date_Time'] = scada_data_melt['Date_Time'].dt.strftime('%Y-%m-%d %H:%M')
    if filetype == 'WIND':
        scada_data_melt['Type'] = 'WIND'
        scada_data_melt = scada_data_melt[scada_data_melt['Variable'] != 'FREQ']
        scada_data_melt = scada_data_melt[scada_data_melt['Variable'] != 'TOTALWIND']
    else:
        agg_type_lst = ['FREQ', 'DSM', 'GSECLTOTAL_GEN',
            'PRIVATE_GEN', 'WIND', 'SOLAR', 'CATERED', 'GUJARATPERI_DRAWL',
            'GUJARATPERI_SCH', 'GUJARATPERI_DSM']
        scada_data_melt['Type'] = ['AGGREGATE' if x in agg_type_lst else 'INTERNAL'
            for x in scada_data_melt['Variable']]
    return  scada_data_melt


# def update_db(dsn, data):
#     """Load the data in the scada staging table."""
#     sql = """INSERT INTO `power`.`scada_staging`
#             (
#             `datetime`,
#             `entity_name`,
#             `quantum`,
#             `discom`,
#             `state`,
#             `source_name`,
#             `unit`,
#             `type`
#             )
#             values (%s,
#              %s, round(%s, 2), %s, %s, %s, %s, %s)
#             on duplicate key update
#             quantum = values(quantum),
#             processed_ind = 0,
#             load_date = NULL"""
#     conn = dbconn.connect(dsn)
#     datacursor = conn.cursor()
#     try:
#         # print data
#         rv = datacursor.executemany(sql, data)
#         print("Return Value %s" % str(rv))
#         print("Load Status %s" % str(conn.info()))
#         conn.commit()
#         datacursor.close()
#     except Exception as error:
#         conn.rollback()
#         datacursor.close()
#         print("Error %s" % str(error))


def fileload(dirname, mvdirname, archivedir, config, discom, state, source):
    if dirname.endswith('/'):
        pickfile = dirname + '*.csv*'
    else:
        pickfile = dirname + '/*.csv*'
    files = [fn for fn in glob.glob(pickfile)
             if not os.path.basename(fn).endswith('.load')]
    for newfile in files:
        filetoloadname = filenamechange(newfile)[1] + '.load'
        slfile, newfile, oldfilenamepath = filediff(mvdirname, archivedir, filetoloadname, newfile)
        filename = os.path.basename(slfile)
        ftype = 'WIND' if filename.startswith('wind') else 'OTHERS'
        try:
            df_load = scada_file_upload_prep(slfile, ftype, 'GUVNL', 'GUJARAT', 'FTP')
            tablename = 'scada_tmp_{}'.format(discom)
            engine = create_engine(config, echo=False)
            df_load.to_sql(name=tablename, con=engine,
                           if_exists='replace')
            sql_str = """insert into power.scada_staging
                (`datetime`, `entity_name`, `quantum`,
                 `discom`, `state`,
                 `source_name`, `unit`, `type`)
                select a.date_time, a.variable entity_name,
                round(a.quantum, 2) quantum, a.discom, a.state,
                a.source source_name, a.unit, a.type
                from {} a
                on duplicate key
                update quantum = round(values(quantum), 2),
                processed_ind = 0,
                load_date = NULL""".format(tablename, discom)
            connection = engine.connect()
            connection.execute(sql_str)
            connection.close()
            engine.dispose()
            # list_load = df_load.values.tolist()
            # print list_load
            # update_db(dsn, list_load)
            shutil.move(newfile, oldfilenamepath)
        except Exception as err:
            logger.info("Error for file %s err: %s",newfile, str(err))


@timeout_decorator.timeout(160)
def run():
    discom = 'GUVNL'
    state = 'GUJARAT'
    source = 'FTP'
    dir_path = os.path.dirname(os.path.realpath(__file__))
    filename = 'ftp_scada_data_{}.lock'.format(discom)
    lockfile = os.path.join(dir_path, filename)
    dir_to_download = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data/scada/'
    # dir_to_download = '/opt/quenext_dev/ems/batch/data/scada/'
    archivedir = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data_archive/scada/'
    # archivedir = '/opt/quenext_dev/ems/batch/data_archive/scada/'
    mvdir = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data/scada/uploaded/'
    # mvdir = '/opt/quenext_dev/ems/batch/data/scada/uploaded/'
    # config = 'mysql+mysqldb://root:quenext@2016@107.167.184.184/power'
    config = 'mysql+mysqldb://root:quenext@2016@104.155.225.29/power'
    # filelist = ['/home/sys_anti/realtimequnext.csv', '/home/sys_anti/winddataawk.csv']
    filelist = ['/home/sys_anti/winddataawk_20_02_2018.csv']
    if not os.path.isfile(filename):
        try:
            open(lockfile, 'w')
            fetchftpjob(filelist, dir_to_download)
            fileload(dir_to_download, mvdir, archivedir, config, discom, state, source)
            os.remove(lockfile)
        except Exception as err:
            logger.info("Error in main Run of FTP: %s", str(err))
            os.remove(lockfile)
    else:
        pass

run()

