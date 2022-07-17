# coding: utf-8
"""
Import SCADA data
"""
# import os
import pysftp
import datetime
import json


def create_ini_json(filename):
    refdict = {"Kamuthi Plant Report/WF_15MIN AGETL Data":
               "2017-06-18 00:00:00",
               "Kamuthi Plant Report/WF_15MIN KERL Data":
               "2017-06-18 00:00:00",
               "Kamuthi Plant Report/WF_15MIN KSPL Data":
               "2017-06-18 00:00:00",
               "Kamuthi Plant Report/WF_15MIN RREL Data":
               "2017-06-18 00:00:00",
               "Kamuthi Plant Report/WF_15MIN RSPL Data":
               "2017-06-18 00:00:00"}
    with open(filename, 'w') as fp:
        json.dump(refdict, fp)

# create_ini_json('ftpfileindx.json')


def read_json_file(filename):
    with open(filename) as data_file:
        data = json.load(data_file)
    return data


def save_ftpfileindx_json(filename, indxdict):
    with open(filename, 'w') as fp:
        json.dump(indxdict, fp)


def get_files(basedir, localdir):
    ftpfileindx = read_json_file(basedir + 'ftpfileindx.json')
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.cd(localdir):
        with pysftp.Connection("117.239.35.240", username="Weather_FV",
                               password="W#ath#r123", cnopts=cnopts) as sftp:
            newdict = {}
            for d, value in ftpfileindx.iteritems():
                dt_indx = \
                    datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                newdict[d] = value
                for file in sftp.listdir(d):
                    year = int(file[:4])
                    month = int(file[5:7])
                    date = int(file[8:10])
                    hour = int(file[11:13])
                    minute = int(file[14:16])
                    second = int(file[17:19])
                    fdatetime = datetime.datetime(year, month, date,
                                                  hour, minute, second)
                    # print fdatetime.strftime("%Y-%m-%d %H:%M:%S")
                    if fdatetime > dt_indx:
                        sftp.get(d + "/" + file)
                        newdict[d] = fdatetime.strftime("%Y-%m-%d %H:%M:%S")
            if len(newdict):
                save_ftpfileindx_json(basedir + 'ftpfileindx.json', newdict)
                print newdict
    return


def main():
    # localdir = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data/demandpowercut/'
    basedir = '/opt/quenext_dev/ems/batch/bin/'
    localdir = '/opt/quenext_dev/ems/batch/data/demandpowercut/'
    get_files(basedir, localdir)


if __name__ == "__main__":
    main()
