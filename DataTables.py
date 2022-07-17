"""
DataTableServer to pupulate the jquery datatable object in the HTML web pages
"""
# from MySQLdb import cursors
import logging


logging.basicConfig(level=logging.DEBUG)
logging = logging.getLogger(__name__)


class DataTablesServer(object):
    def __init__(self, request, columns, index, table, cursor, where, order):
        # print "Here1"
        self.columns = columns
        self.index = index
        self.table = table
        # values specified by the datatable for filtering, sorting, paging
        self.request_values = request.values

        # pass MysqlDB cursor
        self.dbh = cursor

        # results from the db
        self.resultData = None

        # total in the table after filtering
        self.cadinalityFiltered = 0

        # total in the table unfiltered
        self.cadinality = 0
        # print '**********vvvv*****', where, order
        self.where = where
        self.order = order
        # print "Init2"
        self.run_queries()

    def output_result(self):
        # return output
        # print 'xxx0', self.request_values['sEcho'], self.cardinality , self.cadinalityFiltered
        output = {}
        # output['sEcho'] = str(int(self.request_values['sEcho']))
        # output['iTotalRecords'] = str(self.cardinality)
        # output['iTotalDisplayRecords'] = str(self.cadinalityFiltered)
        aaData_rows = []
        logging.info('Datatables output_result start')
        for row in self.resultData:
            aaData_row = []
            # print self.columns
            # print self.columns[0]
            for i in range(len(self.columns)):
                aaData_row.append(str(row[self.columns[i]])
                                  .replace('"', '\\"'))

            # add additional rows here that are not represented in the database
            # aaData_row.append(('''<input id='%s' type='checkbox'></input>''' % (str(row[ self.index ]))).replace('\\', ''))

            aaData_rows.append(aaData_row)
        logging.info('Datatables output_result end')
        output['data'] = aaData_rows
        return output

    def run_queries(self):
        logging.debug('Starting run_queries')
        # dataCursor = self.dbh.cursor(cursors.DictCursor)  # replace the standard cursor with a dictionary cursor only for this query
        # print "Hell1", dataCursor
        dataCursor = self.dbh
        logging.debug("""
                SELECT SQL_CALC_FOUND_ROWS %(columns)s
                FROM  %(table)s
                %(where)s
                %(order)s""", dict(columns=', '.join(self.columns),
                                   table=self.table,
                                   where=self.where,
                                   order=self.order
                                   ))
        # dataCursor.execute( """
        #    SELECT SQL_CALC_FOUND_ROWS %(columns)s
        #    FROM   %(table)s %(where)s %(order)s %(limit)s""" % dict(
        #        columns=', '.join(self.columns), table=self.table, where=self.filtering(), order=self.ordering(),
        #        limit=self.paging()
        #    ) )
        dataCursor.execute("""SELECT SQL_CALC_FOUND_ROWS
                              %(columns)s
                              FROM %(table)s
                              %(where)s
                              %(order)s""",
                           dict(columns=', '.join(self.columns),
                                table=self.table,
                                where=self.where,
                                order=self.order)
                           )
        self.resultData = dataCursor.fetchall()
        logging.debug(['Datatables', self.resultData])
        # cadinalityFilteredCursor = self.dbh.cursor()
        cadinalityFilteredCursor = self.dbh
        cadinalityFilteredCursor.execute("""
            SELECT FOUND_ROWS()""")
        self.cadinalityFiltered = cadinalityFilteredCursor.fetchone()[0]
        # print "Hell2"
        # cadinalityCursor = self.dbh.cursor()
        cadinalityCursor = self.dbh
        # print "Hell3"
        cadinalityCursor.execute("""SELECT COUNT(%s) FROM %s""",
                                 (self.index, self.table))
        # print "Hell4"
        self.cardinality = cadinalityCursor.fetchone()[0]
        # print "Hell5"
        # print "Hell5"

    def filtering(self):
        # build your filter spec
        filter = ""
        if (self.request_values in ('sSearch'))\
                and (self.request_values['sSearch'] != ""):
            filter = "WHERE "
            for i in range(len(self.columns)):
                filter += "{} LIKE '%%{}%%' OR ".format(
                    self.columns[i], self.request_values['sSearch'])
            filter = filter[:-3]
        return filter

        # individual column filtering if needed
        # and_filter_individual_columns = []
        # for i in range(len(columns)):
        #    if (request_values.has_key('sSearch_%d' % i) and request_values['sSearch_%d' % i] != ''):
        #        individual_column_filter = {}
        #        individual_column_filter[columns[i]] = {'$regex': request_values['sSearch_%d' % i], '$options': 'i'}
        #        and_filter_individual_columns.append(individual_column_filter)

        # if and_filter_individual_columns:
        #    filter['$and'] = and_filter_individual_columns
        # return filter

    def ordering(self):
        """
        Ordering
        """
        order = ""
        if (self.request_values['iSortCol_0'] != "") \
                and (self.request_values['iSortingCols'] > 0):
            order = "ORDER BY  "
            for i in range(int(self.request_values['iSortingCols'])):
                order += "{} {}, ".format(
                    self.columns[int(self.request_values['iSortCol_' +
                                                         str(i)])],
                    self.request_values['sSortDir_' + str(i)])
        return order[:-2]

    def paging(self):
        """
        Paging
        """
        limit = ""
        if (self.request_values['iDisplayStart'] != "") \
                and (self.request_values['iDisplayLength'] != -1):
            limit = "LIMIT {}, {}".format(
                self.request_values['iDisplayStart'],
                self.request_values['iDisplayLength'])
        return limit
