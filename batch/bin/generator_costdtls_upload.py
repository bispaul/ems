import datetime
import dbconn
import xlrd
import argparse

CRT_STMT = """CREATE TABLE `power`.`generator_cost_temp`
              ( `Sl` int(15),
                `Generators` varchar(32),
                `GS_name` varchar(50),
                `Description` varchar(150),
                `Surrender` float(6,2),
                `Unit_No` tinyint(4),
                `Type` varchar(32),
                `FUEL` varchar(32),
                `Capacity_charges` float(6,2),
                `Variable_Charges` float(6,2),
                `Pool_Charges` float(6,2),
                `Valid_from_Date` date,
                `Availibility` tinyint(2),
                `Discom` varchar(32),
                `State` varchar(64)
                )"""

SQL_STMT = """INSERT INTO `power`.`generator_cost_temp`
              ( `Sl`,
                `Generators`,
                `GS_name`,
                `Description`,
                `Surrender`,
                `Unit_No`,
                `Type`,
                `FUEL`,
                `Capacity_charges`,
                `Variable_Charges`,
                `Pool_Charges`,
                `Valid_from_Date`,
                `Availibility`,
                `Discom`,
                `State`
                )
                VALUES
                (%s, %s, %s, %s,  ROUND(%s,2), ROUND(%s), %s, %s,
                 ROUND(%s,2),
                 ROUND(%s,2),
                 ROUND(%s,2), %s, %s, %s, %s)"""


def read_xls(filenm):
    workbook = xlrd.open_workbook(filenm)
    sheet = workbook.sheet_by_index(0)
    data = []
    for i in xrange(sheet.nrows):
        if i == 0:
            continue
        else:
            temp = []
            for j in xrange(sheet.ncols):
                if sheet.cell(i, j).value == 'NA':
                    temp.append("")
                elif j == 11:
                    year, month, day, hour, minute, second = \
                        xlrd.xldate_as_tuple(sheet.cell(i, j).value,
                                             workbook.datemode)
                    date_dt = datetime.date(year, month, day)
                    temp.append(date_dt.strftime('%Y-%m-%d'))
                elif j == 12:
                    if sheet.cell(i, j).value == '':
                        temp.append(1)
                    else:
                        temp.append(sheet.cell(i, j).value)
                else:
                    temp.append(sheet.cell(i, j).value)
                # print temp
        data.append(temp)
    return data


def main(args):
    """
    Main Function. Based on filename call to different functions
    """
    dsnfile = "../config/sqldb_connection_config.txt"
    # dsnfile = "../config/sqldb_dev_gcloud.txt"
    try:
        connection = dbconn.connect(dsnfile)
        cursor = connection.cursor()
        # data = read_xls("C:/Users/Biswadip/Documents/My WorkArea/Bihar_Data/pool charges _May'15.xlsx")
        data = read_xls(args.filenm)
        print data
        cursor.execute(CRT_STMT)
        cursor.execute("""create table `power`.`cost_temp`
                       (id int(11), sl int(11))""")
        cursor.executemany(SQL_STMT, data)
        print "Load Status {0}".format(str(connection.info()))
        connection.commit()
        # Add the logic
        cursor.execute("""INSERT into power.cost_temp
                        select b.id , a.sl
                        from power.generator_cost_dtls b,
                             power.generator_cost_temp a
                        where a.generators= b.generation_entity_name
                        and a.gs_name = b.generator_name
                        and round(a.capacity_charges,2)  = b.fixed_cost
                        and round(a.variable_charges,2) = b.variable_cost
                        and round(a.pool_charges,2) = b.pool_cost
                        and round(a.surrender,2) = b.percentage_surrender
                        and a.discom = b.discom
                        and b.valid_from_date <= date(now())
                        and b.valid_to_date >= date(now())""")
        connection.commit()
        cursor.execute("""UPDATE power.generator_cost_dtls
                        set valid_to_date = '2018-01-16'
                        where id not in ( select id from power.cost_temp)
                        and valid_from_date <= date(now())
                        and valid_to_date >= date(now())""")
        connection.commit()
        cursor.execute("""INSERT INTO `power`.`generator_cost_dtls`
                        (
                        `generation_entity_name`,
                        `generator_name`,
                        `station_description`,
                        `percentage_surrender`,
                        `station_unit`,
                        `generator_type`,
                        `generator_fuel`,
                        `fixed_cost`,
                        `variable_cost`,
                        `pool_cost`,
                        `state`,
                        `discom`,
                        `valid_from_date`,
                        `valid_to_date`)
                        select generators, gs_name, description, surrender,
                        unit_no,
                        type, fuel,
                        round(capacity_charges,2) fixed_cost,
                        round(variable_charges,2) variable_cost,
                        round(pool_charges,2) pool_cost,
                        state,
                        discom,
                        valid_from_date,
                        '2100-12-31' valid_to_date
                        from power.generator_cost_temp
                        where sl not in (select sl from power.cost_temp)""")
        cursor.execute("drop table power.cost_temp")
        cursor.execute("drop table `power`.`generator_cost_temp`")
        connection.commit()
        cursor.close()
    except Exception, err:
        cursor.execute("drop table power.cost_temp")
        cursor.execute("drop table power.generator_cost_temp")
        connection.rollback()
        cursor.close()
        print "Load Failed {0}".format(str(err))
        raise
    return


if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description=("Uploads Generator"
                                               " cost to temp table"))
    ARG.add_argument('-f', '--file', dest='filenm',
                     help=('Local file name to save data to.'
                           'The name will be appended with '
                           'date for which crawling is done'),
                     required=True)
    main(ARG.parse_args())
