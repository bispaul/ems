# coding: utf-8

import dbconn
import petl
import os
import shutil
# from openpyxl import load_workbook


def dir_file_walk(path):
    return (os.path.join(root, name)
            for root, dirs, files in os.walk(path)
            for name in files
            if name.endswith((".xlsx")))


def solar_load(file, dsn):
    xls_file = petl.io.xlsx.fromxlsx(file)
    xls_file = xls_file.skip(1)
    header_list = list(xls_file.head(3))
    plant_name = header_list[0][4]
    subplant_name, capacity = tuple(header_list[2][2].split())
    capacity = capacity.strip('(MW)')
    if subplant_name != 'KSPL':
        print file, plant_name, subplant_name, capacity
    hdr = ['Location_Code', 'Unit_Code', 'Datetime', 'Solar_Radiation',
           'Module_Temp', 'Ambient_Temp', 'Wind_Speed', 'Present_Gen',
           'Export_Meter', 'Gen_Meter', 'Todays_Export', 'Total_Inv_Nos',
           'Running_Inv_Nos', 'Wind_Direction', 'Rainfall',
           'Relative_Humidity', 'Performance_Ratio']
    table1 = petl.setheader(xls_file, hdr)
    table2 = table1.tail(2)
    table3 = petl.filldown(table2, 'Location_Code', 'Unit_Code')
    table4 = petl.addfield(table3, 'Capacity', capacity)
    table5 = petl.addfield(table4, 'Plant_Code', subplant_name)
    table6 = petl.addfield(table5, 'Plant_Name', plant_name)
    table7 = petl.addfield(table6, 'Org_Name', 'ADANI')
    # print table6
    if table7:
        sql = """INSERT INTO `power`.`solar_generation_scada_staging`
                (`location_code`,
                `unit_code`,
                `datetime`,
                `solar_radiation`,
                `module_temp`,
                `ambient_temp`,
                `wind_speed`,
                `present_gen`,
                `export_meter`,
                `gen_meter`,
                `todays_export`,
                `total_inv_nos`,
                `running_inv_nos`,
                `wind_direction`,
                `rainfall`,
                `relative_humidity`,
                `performance_ratio`,
                `capacity`,
                `plant_code`,
                `plant_name`,
                `org_name`)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s)
                on duplicate key update
                location_code = values(location_code),
                unit_code = values(unit_code),
                solar_radiation = values(solar_radiation),
                module_temp = values(module_temp),
                ambient_temp = values(ambient_temp),
                wind_speed = values(wind_speed),
                present_gen = values(present_gen),
                export_meter = values(export_meter),
                gen_meter = values(gen_meter),
                todays_export = values(todays_export),
                total_inv_nos = values(total_inv_nos),
                running_inv_nos = values(running_inv_nos),
                wind_direction = values(wind_direction),
                rainfall = values(rainfall),
                relative_humidity = values(relative_humidity),
                performance_ratio = values(performance_ratio),
                capacity = values(capacity),
                plant_code = values(plant_code),
                plant_name = values(plant_name),
                org_name = values(org_name),
                processed_ind = 0,
                load_date = NULL
                """
        # colname = petl.header(table14)
        data = list(table7)[1:]
        conn = dbconn.connect(dsn)
        datacursor = conn.cursor()
        try:
            rv = datacursor.executemany(sql, data)
            print("Return Value %s" % str(rv))
            print("Load Status %s" % str(conn.info()))
            conn.commit()
            datacursor.close()
        except Exception as error:
            conn.rollback()
            datacursor.close()
            print("Error %s" % str(error))


def run():
    # dsn = '/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt'
    # dirname = '/Users/biswadippaul/Downloads/Kamuthi plant report/'
    dirname = '/opt/quenext_dev/ems/batch/data/demandpowercut'
    arc = '/opt/quenext_dev/ems/batch/data_archive/demandpowercut/'
    dsn = '/opt/quenext_dev/ems/batch/config/sqldb_dev_gcloud.txt'
    # cnt = 0
    for filename in sorted(dir_file_walk(dirname)):
        filen = os.path.basename(filename)
        try:
            solar_load(filename, dsn)
            dst_file = os.path.join(arc, filen)
            if os.path.exists(dst_file):
                os.remove(dst_file)
            shutil.move(filename, arc)
        except Exception as error:
            print 'Error in file name: ' + filename + 'Error: ' + str(error)
        # cnt = cnt + 1
        # if cnt > 20:
        #     break
# run()
