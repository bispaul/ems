"""
Credentials to access databases as the webdb user.
Also creates a function to replace the MySQL.connect method and
reassigns the error class, so that we reduce the number of dependencies
on MySQLdb
"""
import MySQLdb
import ast
import logging
import sys

# this is essentially a static variable of this package
ERROR = MySQLdb.Error
THE_DATABASE_CONNECTION = False
DEFAULTS = dict(user='default-user-name', passwd='passwd', port=5000,
                host='default-host-name', db='db')


def connect(config_file):
    """Returns a database connection/handle given the dsn (a dictionary)
    This function saves the database connection, so if you invoke this again,
    it gives you the same one, rather than making a second connection.  This
    is the so-called Singleton pattern."""
    logger = logging.getLogger("dbconn.connect")
    logger.info("****DB CONFIG FILE*************** %s",config_file)
    global THE_DATABASE_CONNECTION
    global DEFAULTS
    # print "Start2 Connected to DB***************"
    # "U" mode is used so that no matter what newline
    # styles you have in the file,
    # they all become \n in memory.
    with open(config_file) as params_fo:
        # print "Start3 Connected to DB***************"
        params = DEFAULTS
        params.update(ast.literal_eval(params_fo.read()))
        # print "Start4 Connected to DB***************"
    logger.debug("DB Status %s %s", THE_DATABASE_CONNECTION, params)
    if not THE_DATABASE_CONNECTION:
        try:
            THE_DATABASE_CONNECTION = MySQLdb.connect(**params)
            # so each modification takes effect automatically
            THE_DATABASE_CONNECTION.autocommit(False)
            logger.debug("************Connected to DB*************** %s",THE_DATABASE_CONNECTION)
            db = THE_DATABASE_CONNECTION
            cursor = db.cursor()
            cursor.execute("SELECT VERSION()")
            results = cursor.fetchone()
            # Check if anything at all is returned
            if results:
                logger.debug("DB Connection Status: True")
            else:
                logger.debug("DB Connection Status: False")
        except ERROR as err:
            logger.error("Couldn't connect to database. MySQL error %d: %s"
                         % (err.args[0], err.args[1]))
            print("Couldn't connect to database. MySQL error %d: %s"
                  % (err.args[0], err.args[1]))
            #sys.exit(1)
    else:
        try:
            THE_DATABASE_CONNECTION.ping(True)
        except ERROR as err:
            logger.error("Couldn't revive connection to database."
                         " MySQL error %d: %s. Retrying for a new connection."
                         % (err.args[0], err.args[1]))
            print ("Couldn't revive connection to database. MySQL error %d: %s"
                   "Retrying for a new connection."
                   % (err.args[0], err.args[1]))
            THE_DATABASE_CONNECTION = MySQLdb.connect(**params)
            THE_DATABASE_CONNECTION.autocommit(False)
    return THE_DATABASE_CONNECTION


if __name__ == '__main__':
    print('starting test code')
    DSNFILE = sys.argv[1]
    print(DSNFILE)
    CONN = connect(DSNFILE)
    print('successfully connected')
    # results as Dictionaries
    CURS = CONN.cursor(MySQLdb.cursors.DictCursor)
    CURS.execute('select user() as user, database() as db')
    ROW = CURS.fetchone()
    print('connected to %s as %s' % (ROW['db'], ROW['user']))
    # CURS.execute('insert into test(id) values(0);')
    # CONN.commit()
    #CONN.close()
    # errm = ''
    # errs = ''
    # # CURS = CONN.cursor()
    # CURS.callproc('sp_realtime_data_fetch',
    #               ('2017-06-22', 'GUVNL', 'UNIT_NAME', errm, errs))
    # ROW = CURS.fetchone()
    # print ROW
