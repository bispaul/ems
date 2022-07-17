"""Email and FTP job."""
import ftplib
import os
import shutil
from datetime import datetime
import pytz
import time
from retrying import retry
from datetime import timedelta
from sqlalchemy import create_engine
import pandas as pd


def check_dir_for_files(dirname):
    """Check a DIR for files."""
    return [os.path.join(dirname, fn) for fn in next(os.walk(dirname))[2]]


@retry(wait_fixed=5000, stop_max_attempt_number=3)
def emailjob(date, filelist, ftpdir):
    """Send email for GUVNL."""
    import emailgun

    send_from = '"EnergyWatch Quenext" <noreply-ems@quenext.com>'
    send_to = ['bbm@gebmail.com', 'seopsldc.getco@gebmail.com',
               'sldcsecct.getco@gebmail.com', 'sldcregen@gmail.com',
               'sldc.getco@gmail.com']
    # send_to = ['biswadip.paul@quenext.com', 'awadhesh.kumar@quenext.com']
    # send_to = ['biswadip.paul@quenext.com']
    cc = ['biswadip.paul@quenext.com', 'awadhesh.kumar@quenext.com']
    # cc = []
    subject = \
        ("Intra Day Forecast for Total Load,"
         "Wind Generation and Solar Generation for ",
         date.strftime('%d-%m-%Y'))
    text = ("Dear Sir,",
            "Please find attached the intra day revised forecast "
            "for total load, wind generation and solar generation for "
            "Gujarat.",
            "Thanks & Regards,",
            "Quenext Energy Watch")
    mg = emailgun.emailgun()
    mg.send_email_attachement(send_from, send_to, cc,
                              subject, text, filelist)
    for f in filelist:
        newpath = os.path.join(ftpdir, os.path.basename(f))
        shutil.move(f, newpath)


def put_file(host, username, password, dir_to_upload, filelist):
    """Put a file in the ftp folder."""
    session = ftplib.FTP(host, username, password)
    session.cwd(dir_to_upload)
    for files in filelist:
        fname = os.path.basename(files)
        sfile = open(files, 'rb')
        session.storbinary('STOR ' + fname, sfile)  # send the file
        sfile.close()  # close file and FTP
    session.quit()
    return


@retry(wait_fixed=5000, stop_max_attempt_number=3)
def ftpjob(filelist, archivedir):
    """FTP job for GUVNL."""
    hostname = '117.247.83.101'
    username = 'sys_anti'
    password = 'sldc@system@123'
    dir_to_upload = '/home/sys_anti/7. Quenext Reports/'
    put_file(hostname, username, password, dir_to_upload, filelist)
    for f in filelist:
        newpath = os.path.join(archivedir, os.path.basename(f))
        shutil.move(f, newpath)


def get_datetime():
    """Get current day."""
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
        localtm = local_now.time()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)
        localtm = local_now.time()
    return local_now, localtm.strftime('%H:%M')


def get_revision(curblock):
    """Get Revision based on time."""
    from sortedcontainers import SortedDict

    block_dict = SortedDict({2: (0, 7), 9: (0, 13), 15: (0, 19),
                             21: (0, 25), 27: (0, 31), 33: (0, 37),
                             39: (0, 43), 45: (0, 49), 51: (0, 55),
                             57: (0, 61), 63: (0, 67), 69: (0, 73),
                             75: (0, 79), 81: (0, 85), 87: (0, 91),
                             93: (1, 1)})
    for key, value in block_dict.items():
        if key >= curblock:
            return value[0], value[1]
    return 1, 1


def get_curblock(config, timestr):
    """Get current block."""
    engine = create_engine(config, echo=False)
    cur_block = pd.read_sql_query("""select block_no
        from block_master
        where '{}'
        between start_time
        and end_time
        """.format(timestr),
        engine, index_col=None)
    return cur_block.iloc[0]['block_no']


def run(dirname, ftpdir, archivedir, config, dns, discom):
    """Run."""
    from intraday_forecast_report import intraday_forecast_report
    cur_date_obj, timestr = get_datetime()
    curblock = get_curblock(config, timestr)
    dateaddr, _ = get_revision(curblock)
    date = cur_date_obj + timedelta(days=dateaddr)
    intraday_forecast_report(config, discom, date.strftime('%Y-%m-%d'),
                             dns, dirname)
    ifilelist = check_dir_for_files(dirname)
    efilelist = [f for f in ifilelist if date.strftime('%d-%m-%Y') in f]
    try:
        emailjob(date, efilelist, ftpdir)
    except Exception as err:
        print str(err)
    try:
        ffilelist = check_dir_for_files(ftpdir)
        ftpjob(ffilelist, archivedir)
    except Exception as err:
        print str(err)


# main()
# dirname = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data/forecast/email'
# ftpdir = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data/forecast/ftp'
# archivedir = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data_archive/forecast'
# # dirname = '/opt/quenext_dev/ems/batch/data/forecast/email'
# # ftpdir = '/opt/quenext_dev/ems/batch/data/forecast/ftp'
# # archivedir = '/opt/quenext_dev/ems/batch/data_archive/forecast'
# config = 'mysql+mysqldb://root:quenext@2016@104.155.225.29/power'
# dns = '../config/sqldb_dev_gcloud.txt'
# discom = 'GUVNL'
# run(dirname, ftpdir, archivedir, config, dns, discom)
