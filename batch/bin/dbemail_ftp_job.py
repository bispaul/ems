"""Email and FTP job."""
import ftplib
import os
import shutil
from datetime import datetime
from datetime import timedelta
import pytz
import time
from retrying import retry


def check_dir_for_files(dirname):
    """Check a DIR for files."""
    return [os.path.join(dirname, fn) for fn in next(os.walk(dirname))[2]]


@retry(wait_fixed=5000, stop_max_attempt_number=3)
def dbemailjob(date, filelist, ftpdir):
    """Send email for GUVNL."""
    import emailgun

    send_from = '"EnergyWatch Quenext" <noreply-ems@quenext.com>'
    # send_to = ['bbm@gebmail.com', 'seopsldc.getco@gebmail.com',
    #            'sldcsecct.getco@gebmail.com', 'sldcregen@gmail.com',
    #            'sldc.getco@gmail.com']
    # send_to = ['biswadip.paul@quenext.com', 'awadhesh.kumar@quenext.com']
    send_to = ['biswadip.paul@quenext.com']
    # cc = ['biswadip.paul@quenext.com', 'awadhesh.kumar@quenext.com']
    cc = []
    subject = \
        ("Day Ahead Forecast for Total Load,"
         "Wind Generation and Solar Generation for ",
         date.strftime('%d-%m-%Y'))
    text = ("Dear Sir,",
            "Please find attached the day ahead forecast "
            "for total load, wind generation and solar generation of "
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
def dbftpjob(filelist, archivedir):
    """FTP job for GUVNL."""
    hostname = '117.247.83.101'
    username = 'sys_anti'
    password = 'sldc@system@123'
    dir_to_upload = '/home/sys_anti/7. Quenext Reports/Day_Ahead_Forecast/'
    put_file(hostname, username, password, dir_to_upload, filelist)
    for f in filelist:
        newpath = os.path.join(archivedir, os.path.basename(f))
        shutil.move(f, newpath)


def get_datetime():
    """Get current day."""
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)
    return local_now


def run(dirname, ftpdir, archivedir, config, discom):
    """Run."""
    from daybefore_forecast_report import daybefore_forecast_report
    date = get_datetime() + timedelta(days=1)
    demand_model = 'HYBRID'
    generation_model = 'NN'
    revision = 0
    daybefore_forecast_report(config, discom, date.strftime('%Y-%m-%d'),
                              demand_model, generation_model,
                              revision, dirname)
    ifilelist = check_dir_for_files(dirname)
    efilelist = [f for f in ifilelist if date.strftime('%d-%m-%Y') in f]
    try:
        dbemailjob(date, efilelist, ftpdir)
    except Exception as err:
        print str(err)
    try:
        ffilelist = check_dir_for_files(ftpdir)
        dbftpjob(ffilelist, archivedir)
    except Exception as err:
        print str(err)


# main()
# dirname = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data/dbforecast/email'
# ftpdir = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data/dbforecast/ftp'
# archivedir = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data_archive/forecast'
dirname = '/opt/quenext_dev/ems/batch/data/dbforecast/email'
ftpdir = '/opt/quenext_dev/ems/batch/data/dbforecast/ftp'
archivedir = '/opt/quenext_dev/ems/batch/data_archive/forecast'
config = 'mysql+mysqldb://root:quenext@2016@104.155.225.29/power'
discom = 'GUVNL'
run(dirname, ftpdir, archivedir, config, discom)
