"""
Import SCADA data
"""
import os
from sets import Set
from ftplib import FTP
import argparse
import sql_load_lib
import cPickle
import csv
import shutil


def getpicklefile(picklefile, remotepath):
    """Get the picklefile dictionary data"""
    # pickle_file_name = localpath + 'directory_list.pkl'
    try:
        pklfile = open(picklefile, 'rb')
        last_ftp_list = cPickle.load(pklfile)
        pklfile.close()
    except Exception:
        last_ftp_list = {}
        last_ftp_list[remotepath] = []
    return last_ftp_list


def ftpfiles(servername, username, password,
             remotepath, localpath, filenamepattern='*.*', onlydiff=False):
    """Connect to an FTP server and bring down files to a local directory"""
    # print servername, username, password,\
    #          remotepath, localpath, filenamepattern, onlydiff
    pickle_file_name = os.path.join(localpath, 'directory_list.pkl')
    # print os.path.join(localpath, 'directory_list.pkl')
    try:
        ftp = FTP(servername)
    except Exception, err:
        print "Couldn't find server" + str(err)
        raise
    ftp.login(username, password)
    ftp.cwd(remotepath)
    last_ftp_list = getpicklefile(pickle_file_name, remotepath)
    try:
        print "Connecting..."
        if onlydiff:
            lfileset = Set(os.listdir(localpath))
            rfileset = Set(ftp.nlst(filenamepattern))
            transferlist = list(rfileset - lfileset)
            print "Missing: " + str(len(transferlist))
            # print last_ftp_list[remotepath]
            #finding chnages to file since last ftp time
            #if last_ftp_list.get(remotepath):
            data = []
            ftp.dir(data.append)
            # print data
            filechanged = list(Set(last_ftp_list[remotepath]) - Set(data))
            if len(filechanged) > 0:
                [transferlist.append(filenm.split()[-1])
                 for filenm in filechanged]
            last_ftp_list[remotepath] = data
            cPickle.dump(last_ftp_list, open(pickle_file_name, "wb"))
        else:
            transferlist = ftp.nlst(filenamepattern)

        filesmoved = 0
        transferlist = list(set(transferlist))
        print transferlist
        for fil in transferlist:
            # create a full local filepath
            print "Transferring File" + ": " + fil
            localfile = os.path.join(localpath, fil)
            grabfile = True
            if grabfile:
                #open a the local file
                fileobj = open(localfile, 'wb')
                # Download the file a chunk at a time using RETR
                ftp.retrbinary('RETR ' + fil, fileobj.write)
                # Close the file
                fileobj.close()
                filesmoved += 1
                
        print "Files Moved" + ": " + str(filesmoved) + " on " + timestamp()
    except Exception:
        print "Connection Error - " + timestamp()
        raise
    ftp.close()  # Close FTP connection
    ftp = None
    return transferlist


def timestamp():
    """returns a formatted current time/date"""
    import time
    return str(time.strftime("%a %d %b %Y %I:%M:%S %p"))


def csvdiff(infile1, infile2, outfile):
    """
    Generate the difference between two csv infile2 - infile1
    """
    reader1 = set(open(infile1, 'rb'))
    reader2 = set(open(infile2, 'rb'))
    with open(outfile, 'wb') as csv_writer:
        csv_writer.writelines(open(infile1, 'rb').readline())
        csv_writer.writelines(reader2 - reader1)


def csvclean(infile, outfile):
    """
    Clean csv files. That is delete blank rows which do not have any data.
    Basically it is just ,,,,,,,,,,,,,,,,,,,,,
    """
    # input = open(os.path.join(args.ldir, fil), 'rb')
    # output = open(os.path.join(args.ldir, fil[:4]), 'wb')
    # writer = csv.writer(output)
    firstrow_flg = True
    with open(infile, 'rb') as csv_reader:
        with open(outfile, 'wb') as csv_writer:
            for row in csv.reader(csv_reader):
                if firstrow_flg:
                    first_row = row
                    firstrow_flg = False
                if any(row):
                    new_row = [val if val not in ('', '#N/A')
                               else r"\N" for val in row]\
                        + ([r"\N"] * (len(first_row) - len(row)))
                    csv.writer(csv_writer).writerow(new_row)


def main(args):
    """
    Main function
    """
    #--- constant connection values
    print "\n-- Retreiving Files----\n"
    
    onlynewfile = True  # set to true to grab & overwrite all files locally

    fname = ftpfiles(args.host, args.userid, args.password, args.remdir,
                     args.ldir, args.filenm, onlynewfile)
    for fil in fname:
        print 'Processing : ' + fil
        if fil[:5] != 'BSEB_':
            if os.path.exists(os.path.join(args.ldir, fil[:4])):
                csvclean(os.path.join(args.ldir, fil),
                         os.path.join(args.ldir, fil[:4] + '.tmp'))
                csvdiff(os.path.join(args.ldir, fil[:4]),
                        os.path.join(args.ldir, fil[:4] + '.tmp'),
                        os.path.join(args.ldir, fil[:4] + '.load'))
                shutil.copy2(os.path.join(args.ldir, fil[:4]) + '.tmp',
                             os.path.join(args.ldir, fil[:4]))
            else:
                csvclean(os.path.join(args.ldir, fil),
                         os.path.join(args.ldir, fil[:4]))
                shutil.copy2(os.path.join(args.ldir, fil[:4]),
                             os.path.join(args.ldir, fil[:4] + '.load'))

            sql_load_lib.sql_table_load_exec(args.dsn,
                                             args.tabnm,
                                             os.path.join(args.ldir, fil[:4]
                                                          + '.load'))
        else:
            sql_load_lib.sql_table_load_exec(args.dsn,
                                             args.tabnm,
                                             os.path.join(args.ldir, fil))
        sql_load_lib.sql_bseb_realtime_forecast_surrender(args.dsn, None)


def test():
    """
    Test
    """
    print "Start"
    # import csv
    # input = open('F:/nrldc/Data/bihar/BSEB11132014.csv', 'rb')
    # output = open('F:/nrldc/Data/bihar/BSEB11132014.csv.1', 'wb')
    # writer = csv.writer(output)
    # for row in csv.reader(input):
    #     if any(row):
    #         writer.writerow(row)
    # input.close()
    # output.close()
    # load_tup = fileobj_read("F:/nrldc/Data/BSEB11042014.csv")
    # print load_tup
    import glob
    
    x = []
    for filex in glob.iglob("F:/nrldc/Data/bihar/BSEB????????.csv"):
        # print  os.path.normpath(filex + '.tmp2')
        csvclean(filex, filex + '.tmp2')
        try:
            sql_load_lib.sql_table_load_exec('F:/nrldc/config/sqldb_connection_config.txt',
                                             'bseb_scada_stg',
                                              os.path.abspath(filex + '.tmp2').replace("\\","/"))
        except Exception, err:
            print err
            x.append(filex)
        print "done: " + filex

    for f in x:
        print f


# main()
#test()
# csvclean('F:/nrldc/Data/bihar/BSEB11132014.csv', 'F:/nrldc/Data/bihar/test.csv')
# csvdiff('F:/nrldc/Data/bihar/BSEB', 'F:/nrldc/Data/bihar/BSEB.tmp', 'F:/nrldc/Data/bihar/BSEB1.csv')

if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description="Fetches BPDCL data and\
                                     Uploads Staging table data")
    ARG.add_argument('-ho', '--host', dest='host',
                     help='FTP Server Name to fetch data from',
                     required=True)
    ARG.add_argument('-uid', '--userid', dest='userid',
                     help='Userid for the FTP Server',
                     required=True)
    ARG.add_argument('-pwd', '--password', dest='password',
                     help='Password for the FTP Server',
                     required=True)
    ARG.add_argument('-rd', '--remdir', dest='remdir',
                     help='Remote Directory Path',
                     required=True)
    ARG.add_argument('-ld', '--ldir', dest='ldir',
                     help='Local directory to save data to',
                     required=True)
    ARG.add_argument('-f', '--file', dest='filenm',
                     help='Pattern of file name to fetch.',
                     required=True)
    # ARG.add_argument('-b', '--strdt', dest='start_date',
    #                  help='Start of Date to crawl the data. \
    #                  Optional default is todays date'
    #                  )
    # ARG.add_argument('-e', '--enddt', dest='end_date',
    #                  help='Start of Date to crawl the data. \
    #                  Optional default is todays date'
    #                  )
    ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
                     help='Full path and the file name of the db config',
                     required=True)
    ARG.add_argument('-t', '--tabnm', dest='tabnm',
                     help='Name of the table to be loaded',
                     required=True)
    main(ARG.parse_args())
