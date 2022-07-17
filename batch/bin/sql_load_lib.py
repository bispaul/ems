"""
The SQL statements to load the respective table as well as the methods
"""
try:
    from . import dbconn
except:
    import dbconn
import logging
import MySQLdb

SQL_STMT = {'nrldc_state_drawl_schedule_stg':
            """INSERT INTO power.nrldc_state_drawl_schedule_stg
            (Date,
             State,
             Discom,
             Issue_Date_Time,
             Revision,
             Drawl_Type,
             Block_No,
             Station_Name,
             Schedule,
             Appr_No)
             VALUES(
                    STR_TO_DATE(%s,'%%d-%%b-%%Y'),
                    %s, %s,
                    STR_TO_DATE(%s,'%%e/%%m/%%Y %%H:%%i'),
                    %s, %s,
                    %s, %s,
                    ROUND(%s,3), %s)
            ON DUPLICATE KEY
            UPDATE Schedule = VALUES(Schedule),
            load_ind = NULL""",
            'nrldc_entitlements_stg':
            """INSERT INTO power.nrldc_entitlements_stg
            (Date,
             State,
             Discom,
             Issue_Date_Time,
             Revision,
             Block_No,
             Station_Name,
             Schedule)
             VALUES(
                    STR_TO_DATE(%s,'%%d-%%b-%%Y'),
                    %s, %s,
                    STR_TO_DATE(%s,'%%e/%%m/%%Y %%H:%%i'),
                    %s,
                    %s, %s,
                    ROUND(%s,3))
            ON DUPLICATE KEY
            UPDATE Schedule = VALUES(Schedule),
            load_ind = NULL""",
            'internal_declared_capacity_stg':
            """INSERT INTO internal_declared_capacity_stg
             (date,
              state,
              discom,
              revision,
              block_no,
              entity_name,
              entity_type,
              schedule
              )
             VALUES (%s,
                     %s,
                     %s,
                     %s,
                     %s,
                     %s,
                     %s,
                     %s
                    )
            on duplicate key update
            schedule = values(schedule),
            processed_ind = 0""",
            'internal_drawl_schedule_stg':
            """INSERT INTO internal_drawl_schedule_stg
             (date,
              state,
              discom,
              revision,
              block_no,
              entity_name,
              entity_type,
              schedule
              )
             VALUES (%s,
                     %s,
                     %s,
                     %s,
                     %s,
                     %s,
                     %s,
                     %s
                    )
            on duplicate key update
            schedule = values(schedule),
            processed_ind = 0""",                                   
            # 'nrldc_state_drawl_schedule_stg':
            # """INSERT INTO nrldc_state_drawl_schedule_stg
            # (ForDate,
            # State,
            # Issue_Date_Time,
            # Revision,
            # Drawl_Type,
            # Station_Name,
            # Periphery,
            # Head1,
            # Head2,
            # Head3,
            # Head4,
            # Head5,
            # Head6,
            # Head7,
            # Block96,
            # Block95,
            # Block94,
            # Block93,
            # Block92,
            # Block91,
            # Block90,
            # Block89,
            # Block88,
            # Block87,
            # Block86,
            # Block85,
            # Block84,
            # Block83,
            # Block82,
            # Block81,
            # Block80,
            # Block79,
            # Block78,
            # Block77,
            # Block76,
            # Block75,
            # Block74,
            # Block73,
            # Block72,
            # Block71,
            # Block70,
            # Block69,
            # Block68,
            # Block67,
            # Block66,
            # Block65,
            # Block64,
            # Block63,
            # Block62,
            # Block61,
            # Block60,
            # Block59,
            # Block58,
            # Block57,
            # Block56,
            # Block55,
            # Block54,
            # Block53,
            # Block52,
            # Block51,
            # Block50,
            # Block49,
            # Block48,
            # Block47,
            # Block46,
            # Block45,
            # Block44,
            # Block43,
            # Block42,
            # Block41,
            # Block40,
            # Block39,
            # Block38,
            # Block37,
            # Block36,
            # Block35,
            # Block34,
            # Block33,
            # Block32,
            # Block31,
            # Block30,
            # Block29,
            # Block28,
            # Block27,
            # Block26,
            # Block25,
            # Block24,
            # Block23,
            # Block22,
            # Block21,
            # Block20,
            # Block19,
            # Block18,
            # Block17,
            # Block16,
            # Block15,
            # Block14,
            # Block13,
            # Block12,
            # Block11,
            # Block10,
            # Block9,
            # Block8,
            # Block7,
            # Block6,
            # Block5,
            # Block4,
            # Block3,
            # Block2,
            # Block1)
            # VALUES (%s,%s,STR_TO_DATE(%s,'%%d/%%m/%%Y %%H:%%i Hrs'),
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s)""",
            # 'nrldc_entitlements_stg':
            # """INSERT INTO nrldc_entitlements_stg
            # (ForDate,
            # State,
            # Issue_Date_Time,
            # Revision,
            # Station_Name,
            # Block96,
            # Block95,
            # Block94,
            # Block93,
            # Block92,
            # Block91,
            # Block90,
            # Block89,
            # Block88,
            # Block87,
            # Block86,
            # Block85,
            # Block84,
            # Block83,
            # Block82,
            # Block81,
            # Block80,
            # Block79,
            # Block78,
            # Block77,
            # Block76,
            # Block75,
            # Block74,
            # Block73,
            # Block72,
            # Block71,
            # Block70,
            # Block69,
            # Block68,
            # Block67,
            # Block66,
            # Block65,
            # Block64,
            # Block63,
            # Block62,
            # Block61,
            # Block60,
            # Block59,
            # Block58,
            # Block57,
            # Block56,
            # Block55,
            # Block54,
            # Block53,
            # Block52,
            # Block51,
            # Block50,
            # Block49,
            # Block48,
            # Block47,
            # Block46,
            # Block45,
            # Block44,
            # Block43,
            # Block42,
            # Block41,
            # Block40,
            # Block39,
            # Block38,
            # Block37,
            # Block36,
            # Block35,
            # Block34,
            # Block33,
            # Block32,
            # Block31,
            # Block30,
            # Block29,
            # Block28,
            # Block27,
            # Block26,
            # Block25,
            # Block24,
            # Block23,
            # Block22,
            # Block21,
            # Block20,
            # Block19,
            # Block18,
            # Block17,
            # Block16,
            # Block15,
            # Block14,
            # Block13,
            # Block12,
            # Block11,
            # Block10,
            # Block9,
            # Block8,
            # Block7,
            # Block6,
            # Block5,
            # Block4,
            # Block3,
            # Block2,
            # Block1)
            # VALUES (%s,%s,STR_TO_DATE(%s,'%%d/%%m/%%Y %%H:%%i Hrs'),
            #         %s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            #         %s,%s,%s,%s,%s)""",
            'nrldc_declared_capability_stg':
            """INSERT INTO nrldc_declared_capability_stg
            (ForDate,
            Issue_Date_Time,
            Revision,
            Station_Name,
            Block96,
            Block95,
            Block94,
            Block93,
            Block92,
            Block91,
            Block90,
            Block89,
            Block88,
            Block87,
            Block86,
            Block85,
            Block84,
            Block83,
            Block82,
            Block81,
            Block80,
            Block79,
            Block78,
            Block77,
            Block76,
            Block75,
            Block74,
            Block73,
            Block72,
            Block71,
            Block70,
            Block69,
            Block68,
            Block67,
            Block66,
            Block65,
            Block64,
            Block63,
            Block62,
            Block61,
            Block60,
            Block59,
            Block58,
            Block57,
            Block56,
            Block55,
            Block54,
            Block53,
            Block52,
            Block51,
            Block50,
            Block49,
            Block48,
            Block47,
            Block46,
            Block45,
            Block44,
            Block43,
            Block42,
            Block41,
            Block40,
            Block39,
            Block38,
            Block37,
            Block36,
            Block35,
            Block34,
            Block33,
            Block32,
            Block31,
            Block30,
            Block29,
            Block28,
            Block27,
            Block26,
            Block25,
            Block24,
            Block23,
            Block22,
            Block21,
            Block20,
            Block19,
            Block18,
            Block17,
            Block16,
            Block15,
            Block14,
            Block13,
            Block12,
            Block11,
            Block10,
            Block9,
            Block8,
            Block7,
            Block6,
            Block5,
            Block4,
            Block3,
            Block2,
            Block1)
            VALUES (%s,STR_TO_DATE(%s,'%%d/%%m/%%Y %%H:%%i Hrs'),
                    %s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s)""",
            'isgs_injsch_schedule_stg':
            """INSERT INTO isgs_injsch_schedule_stg
            (ForDate,
            Issue_Date_Time,
            Revision,
            StationName,
            Head,
            Block1,
            Block2,
            Block3,
            Block4,
            Block5,
            Block6,
            Block7,
            Block8,
            Block9,
            Block10,
            Block11,
            Block12,
            Block13,
            Block14,
            Block15,
            Block16,
            Block17,
            Block18,
            Block19,
            Block20,
            Block21,
            Block22,
            Block23,
            Block24,
            Block25,
            Block26,
            Block27,
            Block28,
            Block29,
            Block30,
            Block31,
            Block32,
            Block33,
            Block34,
            Block35,
            Block36,
            Block37,
            Block38,
            Block39,
            Block40,
            Block41,
            Block42,
            Block43,
            Block44,
            Block45,
            Block46,
            Block47,
            Block48,
            Block49,
            Block50,
            Block51,
            Block52,
            Block53,
            Block54,
            Block55,
            Block56,
            Block57,
            Block58,
            Block59,
            Block60,
            Block61,
            Block62,
            Block63,
            Block64,
            Block65,
            Block66,
            Block67,
            Block68,
            Block69,
            Block70,
            Block71,
            Block72,
            Block73,
            Block74,
            Block75,
            Block76,
            Block77,
            Block78,
            Block79,
            Block80,
            Block81,
            Block82,
            Block83,
            Block84,
            Block85,
            Block86,
            Block87,
            Block88,
            Block89,
            Block90,
            Block91,
            Block92,
            Block93,
            Block94,
            Block95,
            Block96,
            FuelShortage,
            DC,
            ExpectedEnergy,
            MeteredEnergy
            )
            VALUES (%s,STR_TO_DATE(%s,'%%d/%%m/%%Y %%H:%%i Hrs'),
                    %s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            'nrldc_state_trans_cons_stg':
            """INSERT INTO nrldc_state_trans_cons_stg
            (ForDate,
            Issue_Date_Time
            Revision,
            StationName,
            State,
            Block1,
            Block2,
            Block3,
            Block4,
            Block5,
            Block6,
            Block7,
            Block8,
            Block9,
            Block10,
            Block11,
            Block12,
            Block13,
            Block14,
            Block15,
            Block16,
            Block17,
            Block18,
            Block19,
            Block20,
            Block21,
            Block22,
            Block23,
            Block24,
            Block25,
            Block26,
            Block27,
            Block28,
            Block29,
            Block30,
            Block31,
            Block32,
            Block33,
            Block34,
            Block35,
            Block36,
            Block37,
            Block38,
            Block39,
            Block40,
            Block41,
            Block42,
            Block43,
            Block44,
            Block45,
            Block46,
            Block47,
            Block48,
            Block49,
            Block50,
            Block51,
            Block52,
            Block53,
            Block54,
            Block55,
            Block56,
            Block57,
            Block58,
            Block59,
            Block60,
            Block61,
            Block62,
            Block63,
            Block64,
            Block65,
            Block66,
            Block67,
            Block68,
            Block69,
            Block70,
            Block71,
            Block72,
            Block73,
            Block74,
            Block75,
            Block76,
            Block77,
            Block78,
            Block79,
            Block80,
            Block81,
            Block82,
            Block83,
            Block84,
            Block85,
            Block86,
            Block87,
            Block88,
            Block89,
            Block90,
            Block91,
            Block92,
            Block93,
            Block94,
            Block95,
            Block96)
            VALUES (%s,STR_TO_DATE(%s,'%%d/%%m/%%Y %%H:%%i Hrs'),
                    %s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s)""",
            'nrldc_station_trans_cons_stg':
            """INSERT INTO nrldc_station_trans_cons_stg
            (ForDate,
            Issue_Date_Time
            Revision,
            StationName,
            Station,
            Block1,
            Block2,
            Block3,
            Block4,
            Block5,
            Block6,
            Block7,
            Block8,
            Block9,
            Block10,
            Block11,
            Block12,
            Block13,
            Block14,
            Block15,
            Block16,
            Block17,
            Block18,
            Block19,
            Block20,
            Block21,
            Block22,
            Block23,
            Block24,
            Block25,
            Block26,
            Block27,
            Block28,
            Block29,
            Block30,
            Block31,
            Block32,
            Block33,
            Block34,
            Block35,
            Block36,
            Block37,
            Block38,
            Block39,
            Block40,
            Block41,
            Block42,
            Block43,
            Block44,
            Block45,
            Block46,
            Block47,
            Block48,
            Block49,
            Block50,
            Block51,
            Block52,
            Block53,
            Block54,
            Block55,
            Block56,
            Block57,
            Block58,
            Block59,
            Block60,
            Block61,
            Block62,
            Block63,
            Block64,
            Block65,
            Block66,
            Block67,
            Block68,
            Block69,
            Block70,
            Block71,
            Block72,
            Block73,
            Block74,
            Block75,
            Block76,
            Block77,
            Block78,
            Block79,
            Block80,
            Block81,
            Block82,
            Block83,
            Block84,
            Block85,
            Block86,
            Block87,
            Block88,
            Block89,
            Block90,
            Block91,
            Block92,
            Block93,
            Block94,
            Block95,
            Block96)
            VALUES (%s,STR_TO_DATE(%s,'%%d/%%m/%%Y %%H:%%i Hrs'),
                    %s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s)""",
            'nrldc_schedule_est_trans_loss_stg':
            """INSERT INTO nrldc_schedule_est_trans_loss_stg
            (Start_Date,
            End_Date,
            Loss_Perc)
            VALUES (%s,%s,%s)
            ON DUPLICATE KEY UPDATE Loss_Perc = VALUES(Loss_Perc)""",
            'imdaws_stg':
            """LOAD DATA INFILE '%s' IGNORE
            INTO TABLE imdaws_stg
            FIELDS TERMINATED BY ';'
            ESCAPED BY '\\\\'
            LINES TERMINATED BY '\\r\\n'
            IGNORE 1 LINES
            (
            Sl_No ,
            STATION_NAME ,
            @Obs_Date ,
            TIME_UTC ,
            LATITUDE_N ,
            LONGITUDE_E ,
            @SLP_hPa ,
            MSLP ,
            @RAINFALL_mm ,
            @TEMPERATURE_DEGC ,
            @DEWPOINT_DEGC ,
            @WINDSPEED_Kt ,
            @WINDDIR_Deg ,
            @TMAX_DegC ,
            @TMIN_DegC ,
            @PTEND_hPa ,
            @SSHM ,
            State
            )
            SET Load_Date=NULL,
            Obs_Date = str_to_date(@Obs_Date,'%%d-%%b-%%Y'),
            SLP_hPa= IF(@SLP_hPa='NA',NULL,@SLP_hPa),
            RAINFALL_mm= IF(@RAINFALL_mm='NA',NULL,@RAINFALL_mm),
            TEMPERATURE_DEGC=IF(@TEMPERATURE_DEGC='NA',NULL,@TEMPERATURE_DEGC),
            DEWPOINT_DEGC=IF(@DEWPOINT_DEGC='NA',NULL,@DEWPOINT_DEGC) ,
            WINDSPEED_Kt=IF(@WINDSPEED_Kt='NA',NULL,@WINDSPEED_Kt) ,
            WINDDIR_Deg=IF(@WINDDIR_Deg='NA',NULL,@WINDDIR_Deg) ,
            TMAX_DegC=IF(@TMAX_DegC='NA',NULL,@TMAX_DegC),
            TMIN_DegC=IF(@TMIN_DegC='NA',NULL,@TMIN_DegC),
            PTEND_hPa=IF(@PTEND_hPa='NA',NULL,@PTEND_hPa),
            SSHM=IF(@SSHM='NA',NULL,@SSHM)""",
            'exchange_areaprice_stg':
            """INSERT INTO exchange_areaprice_stg
            (Delivery_Date,
            Block,
            Block_Time,
            A1_Price,
            A2_Price,
            E1_Price,
            E2_Price,
            N1_Price,
            N2_Price,
            N3_Price,
            S1_Price,
            S2_Price,
            S3_Price,
            W1_Price,
            W2_Price,
            W3_Price,
            Unconstraint_MCP,
            Exchange_Name
            )
            VALUES (%s, %s, REPLACE(%s, ' ', ''),
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                   )
            ON DUPLICATE KEY
            UPDATE A1_Price = VALUES(A1_Price),
                   A2_Price = VALUES(A2_Price),
                   E1_Price = VALUES(E1_Price),
                   E2_Price = VALUES(E2_Price),
                   N1_Price = VALUES(N1_Price),
                   N2_Price = VALUES(N2_Price),
                   N3_Price = VALUES(N3_Price),
                   S1_Price = VALUES(S1_Price),
                   S2_Price = VALUES(S2_Price),
                   S3_Price = VALUES(S3_Price),
                   W1_Price = VALUES(W1_Price),
                   W2_Price = VALUES(W2_Price),
                   W3_Price = VALUES(W3_Price),
                   Unconstraint_MCP = VALUES(Unconstraint_MCP)""",
            'sldc_scada_snapshot':
            """INSERT INTO power.sldc_scada_snapshot
            (Snapshot_Date_Time,
            Station_Name,
            Station_Type,
            Unit_No,
            MW,
            Additional_Details,
            MVAR)
            VALUES (
                    STR_TO_DATE(%s,'%%d-%%m-%%Y %%H:%%i:%%S'),
                    REPLACE(%s, '\\n', ''),
                    %s, %s, %s, %s, %s
                    )""",
            'sldc_scada_mis':
            """INSERT INTO power.sldc_scada_mis
            (Date,
             Time,
             Frequency,
             UI_Rate,
             NR_OD_UD,
             Drawl)
            VALUES (
                    STR_TO_DATE(%s,'%%d-%%m-%%Y'),
                    %s, %s, %s, %s, %s
                    )""",
            'gss_schedule_stg':
            """INSERT INTO power.gss_schedule_stg
            (Date,
             Date_Issue,
             Attribute_Name,
             Hr1,
             Hr2,
             Hr3,
             Hr4,
             Hr5,
             Hr6,
             Hr7,
             Hr8,
             Hr9,
             Hr10,
             Hr11,
             Hr12,
             Hr13,
             Hr14,
             Hr15,
             Hr16,
             Hr17,
             Hr18,
             Hr19,
             Hr20,
             Hr21,
             Hr22,
             Hr23,
             Hr24,
             LU)
            VALUES (
                    %s, %s, %s,
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3)
                    )""",
            'tentative_schedule_stg':
            """INSERT INTO power.tentative_schedule_stg
            (Date,
             Date_Issue,
             Attribute_Name,
             Hr1,
             Hr2,
             Hr3,
             Hr4,
             Hr5,
             Hr6,
             Hr7,
             Hr8,
             Hr9,
             Hr10,
             Hr11,
             Hr12,
             Hr13,
             Hr14,
             Hr15,
             Hr16,
             Hr17,
             Hr18,
             Hr19,
             Hr20,
             Hr21,
             Hr22,
             Hr23,
             Hr24,
             LU)
            VALUES (
                    %s, %s, %s,
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3)
                    )""",
            'rldc_actual_solar_wind':
            """INSERT INTO power.rldc_actual_solar_wind
            (Date,
             Block_No,
             Wind,
             Solar
             )
            VALUES (
                    %s, %s,
                    ROUND(%s,3), ROUND(%s,3)
                    )""",
            'dc_scada_mis':
            """INSERT INTO power.dc_scada_mis
            (Date,
            Time,
            Snapshot_Date_Time,
            Discom_Name,
            Feeder_Name,
            Schedule,
            Drawl,
            OD_UD
            )
            VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                    )""",
            'forecast_stg':
            """INSERT INTO power.forecast_stg
            (Date,
            Block_No,
            Discom_Name,
            Demand_Forecast,
            Bilateral,
            Internal_Generation,
            Wind_Gen_Forecast,
            Solar_Gen_Forecast,
            State
            )
            VALUES (
                    %s, %s, 'RVVNL',
                    ROUND(%s,3), ROUND(%s,3), ROUND(%s,3),
                    ROUND(%s,3), ROUND(%s,3), %s
                    )
            ON DUPLICATE KEY
            UPDATE Demand_Forecast = VALUES(Demand_Forecast),
                   Bilateral = VALUES(Bilateral),
                   Internal_Generation = VALUES(Internal_Generation),
                   Wind_Gen_Forecast = VALUES(Wind_Gen_Forecast),
                   Solar_Gen_Forecast = VALUES(Solar_Gen_Forecast)""",
            'wrldc_state_drawl_schedule_stg':
            """INSERT INTO power.wrldc_state_drawl_schedule_stg
            (Date,
             State,
             Discom,
             Issue_Date_Time,
             Revision,
             Drawl_Type,
             Block_No,
             Station_Name,
             Schedule,
             Appr_No)
             VALUES(
                    STR_TO_DATE(%s,'%%d-%%b-%%Y'),
                    %s, %s,
                    STR_TO_DATE(%s,'%%e/%%m/%%Y %%H:%%i'),
                    %s, %s,
                    %s, %s,
                    ROUND(%s,3), %s)
            ON DUPLICATE KEY
            UPDATE Schedule = VALUES(Schedule),
            load_ind = NULL,
            load_date = NULL""",
            'wrldc_declared_capability_stg':
            """INSERT INTO power.wrldc_declared_capability_stg
            (Date,
            Issue_Date_Time,
            Revision,
            Block_No,
            Station_Name,
            Schedule)
            VALUES(
                   STR_TO_DATE(%s,'%%d-%%b-%%Y'),
                   STR_TO_DATE(%s,'%%e/%%m/%%Y %%H:%%i'),
                   %s, %s,
                   %s,
                   ROUND(%s,3))""",
            'wrldc_entitlements_stg':
            """INSERT INTO power.wrldc_entitlements_stg
            (Date,
             State,
             Discom,
             Issue_Date_Time,
             Revision,
             Block_No,
             Station_Name,
             Schedule)
             VALUES(
                    STR_TO_DATE(%s,'%%d-%%b-%%Y'),
                    %s, %s,
                    STR_TO_DATE(%s,'%%e/%%m/%%Y %%H:%%i'),
                    %s,
                    %s, %s,
                    ROUND(%s,3))
            ON DUPLICATE KEY
            UPDATE Schedule = VALUES(Schedule),
            load_ind = NULL,
            load_date = NULL""",
            'wrldc_isgs_injsch_schedule_stg':
            """INSERT INTO power.wrldc_isgs_injsch_schedule_stg
            (Date,
            Issue_Date_Time,
            Revision,
            Block_No,
            Station_Name,
            Schedule)
            VALUES(
                   STR_TO_DATE(%s,'%%d-%%b-%%Y'),
                   STR_TO_DATE(%s,'%%e/%%m/%%Y %%H:%%i'),
                   %s, %s,
                   %s,
                   ROUND(%s,3))""",
            'erldc_state_drawl_schedule_stg':
            """INSERT INTO power.erldc_state_drawl_schedule_stg
            (Date,
             State,
             Discom,
             Revision,
             Issue_Date_Time,
             Drawl_Type,
             Block_No,
             Station_Name,
             Schedule)
             VALUES(
                    STR_TO_DATE(%s,'%%Y-%%m-%%d'),
                    %s, %s,
                    %s,
                    STR_TO_DATE(%s,'%%Y-%%m-%%d %%H:%%i:%%s'),
                    %s,
                    %s, %s,
                    ROUND(%s,3))""",
            'bseb_isgs_stg':
            """LOAD DATA LOCAL INFILE '%s'
            REPLACE INTO TABLE `power`.`bseb_isgs_stg`
            FIELDS TERMINATED BY ','
            ESCAPED BY '\\\\'
            LINES TERMINATED BY '\\r\\n'
            IGNORE 1 LINES
            (@for_date,
             revision,
             @issue_date_time,
             block,
             teesta,
             tala,
             chpc,
             rangit,
             khep,
             fstpp_i_ii,
             fstpp_iii,
             khstpp_i,
             khstpp_ii,
             tstpp_i,
             barh,
             dadari,
             bilat,
             iex,
             pxi,
             net)
            SET for_date = str_to_date(@for_date,'%%Y-%%m-%%d'),
            issue_date_time= \
            str_to_date(@issue_date_time,'%%Y-%%m-%%d %%H:%%i:%%s')""",
            'bseb_scada_stg':
            """LOAD DATA LOCAL INFILE '%s'
            REPLACE INTO TABLE `power`.`bseb_scada_stg`
            FIELDS TERMINATED BY ','
            ESCAPED BY '\\\\'
            LINES TERMINATED BY '\\r\\n'
            IGNORE 1 LINES
           ( @date,
             @time,
             schedule,
             total_drawal,
             ui_pow,
             freq,
             ui_rate,
             purnea_pg_purnea_bseb,
             purnea_pg_kishangunj,
             muzaffarpur_pg_mtps_kanti,
             kafen_pg_hajipur,
             purnea_pg_madhepura_bseb,
             kb_gen,
             begusarai_b_sharif,
             b_sharif_pg_b_sharif_bseb,
             pusauli_pg_dehri_bseb,
             pusauli_pg_arah_pg,
             patna_pg_khagaul_bseb,
             patna_pg_fatwa_bseb,
             k_gaon_ntpc_k_gaon_bseb,
             k_gaon_ntpc_sabour_bseb,
             lalmatia_sabour_sult,
             b_sharif_barhi_barhi_end,
             barhi_rajgir,
             b_sharif_tenughat,
             sonenar_garhwa,
             sultangunj_deoghar,
             sonenagar_rihand,
             karmanasa_sahupuri,
             gaya_pg_bodhgaya_bseb,
             gaya_pg_dehri_bseb,
             patna_pg_sipara_bseb,
             banka_pg_banka_bseb,
             lakhisarai_lakhisarai_pg,
             lakhisarai_jamui,
             @dummy
             )
            SET date = str_to_date(@date,'%%d/%%m/%%Y'),
            time = str_to_date(@time,'%%H:%%i:%%s')""",
            'forecast_stg_bseb':
            """INSERT INTO power.forecast_stg
            (State,
            Date,
            Block_No,
            Discom_Name,
            Demand_Forecast
            )
            VALUES (
                    UCASE(%s), %s,
                    %s, %s, ROUND(%s,3)
                    )
            ON DUPLICATE KEY
            UPDATE Demand_Forecast = VALUES(Demand_Forecast)""",
            'drawl_staging':
            """INSERT INTO power.drawl_staging
            (Date,
            Block_No,
            Frequency,
            Discom,
            Constrained_load,
            Schedule,
            State
            )
            VALUES (
                    %s, %s, ROUND(%s,2),
                    %s, ROUND(%s,2), ROUND(%s,2),
                    %s
                    )
            ON DUPLICATE KEY
            UPDATE Frequency = VALUES(Frequency),
                   Constrained_load = VALUES(Constrained_load),
                   Schedule = VALUES(Schedule),
                   Processed_ind = 0,
                   Load_date = NULL""",
           #  'powercut_staging':
           #  """LOAD DATA LOCAL INFILE '%s'
           #  REPLACE INTO TABLE `power`.`powercut_staging`
           #  FIELDS TERMINATED BY ','
           #  ESCAPED BY '\\\\'
           #  LINES TERMINATED BY '\\r\\n'
           #  IGNORE 1 LINES
           # ( @date,
           #   station,
           #   block_no,
           #   powercut
           #   )
           #  SET state = '%s',
           #  date = str_to_date(@date,'%%m/%%d/%%Y'),
           #  load_date = NULL""",
            'powercut_staging':
            """INSERT INTO `power`.`powercut_staging`
            (date,
             station,
             block_no,
             powercut,
             state
             )
            VALUES (
                    %s, %s, %s,
                    ROUND(%s,2),
                    %s
                    )
            ON DUPLICATE KEY
            UPDATE powercut =  VALUES(powercut),
                   processed_ind = 0,
                   load_date = NULL""",
           #  'actual_weather_staging':
           #  """LOAD DATA INFILE '%s'
           #  REPLACE INTO TABLE `power`.`actual_weather_staging`
           #  FIELDS TERMINATED BY ','
           #  ESCAPED BY '\\\\'
           #  LINES TERMINATED BY '\\r\\n'
           #  IGNORE 1 LINES
           # ( date,
           #   @time,
           #   @temperature,
           #   @dewpoint,
           #   @humidity,
           #   @wind_direction,
           #   @wind_speed,
           #   @gust_speed,
           #   @events,
           #   @conditions,
           #   @pressure,
           #   @wind_chill,
           #   @heat_index,
           #   @wind_dir_deg
           #   )
           #  SET time = str_to_date(@time,'%%h:%%i %%p'),
           #  temperature = if(@temperature = '',NULL, @temperature),
           #  dewpoint = if(@dewpoint = '',NULL, @dewpoint),
           #  humidity = if(@humidity = '',NULL, @humidity),
           #  wind_direction = if(@wind_direction = '',NULL, @wind_direction),
           #  wind_speed = if(@wind_speed = '',NULL, @wind_speed),
           #  gust_speed = if(@gust_speed = '',NULL, @gust_speed),
           #  events = if(@events = '',NULL, @events),
           #  conditions = if(@conditions = '',NULL, @conditions),
           #  pressure = if(@pressure = '',NULL, @pressure),
           #  wind_chill = if(@wind_chill = '',NULL, @wind_chill),
           #  heat_index  = if(@heat_index = '',NULL, @heat_index),
           #  wind_dir_deg = if(@wind_dir_deg = '',NULL, @wind_dir_deg),
           #  state = '%s',
           #  location = '%s',
           #  load_date = NULL""",
            'actual_weather_staging':
            """INSERT INTO `power`.`actual_weather_staging`
            (date,
             time,
             temperature,
             dewpoint,
             humidity,
             wind_direction,
             wind_speed,
             gust_speed,
             events,
             conditions,
             pressure,
             wind_chill,
             heat_index,
             wind_dir_deg,
             state,
             location
             )
            VALUES (
                    %s, TIME_FORMAT(%s, '%%T'),
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                    )
            ON DUPLICATE KEY
            UPDATE temperature = values(temperature),
                   dewpoint = values(dewpoint),
                   humidity = values(humidity),
                   wind_direction = values(wind_direction),
                   wind_speed = values(wind_speed),
                   gust_speed = values(gust_speed),
                   events = values(events),
                   conditions = values(conditions),
                   pressure = values(pressure),
                   wind_chill = values(wind_chill),
                   heat_index = values(heat_index),
                   wind_dir_deg = values(wind_dir_deg),
                   processed_ind = 0,
                   load_date = NULL""",
           #  'forecast_weather_staging':
           #  """LOAD DATA INFILE '%s'
           #  REPLACE INTO TABLE `power`.`forecast_weather_staging`
           #  FIELDS TERMINATED BY ','
           #  ESCAPED BY '\\\\'
           #  LINES TERMINATED BY '\\r\\n'
           #  IGNORE 1 LINES
           # ( date,
           #   @time,
           #   @temperature,
           #   @wind_speed,
           #   @wind_direction,
           #   @humidity,
           #   @wind_dir_deg,
           #   @dewpoint,
           #   @conditions,
           #   @qpf,
           #   @pop,
           #   @cloud_cover
           #   )
           #  SET
           #  temperature = if(@temperature = '',NULL, @temperature),
           #  wind_speed = if(@wind_speed = '',NULL, @wind_speed),
           #  wind_direction = if(@wind_direction = '',NULL, @wind_direction),
           #  humidity = if(@humidity = '',NULL, @humidity),
           #  wind_dir_deg = if(@wind_dir_deg = '',NULL, @wind_dir_deg),
           #  dewpoint = if(@dewpoint = '',NULL, @dewpoint),
           #  conditions = if(@conditions = '',NULL, @conditions),
           #  qpf = if(@qpf = '',NULL, @qpf),
           #  pop = if(@pop = '',NULL, @pop),
           #  cloud_cover = if(@cloud_cover = '',NULL, @cloud_cover),
           #  time = str_to_date(@time,'%%h:%%i %%p'),
           #  state = '%s',
           #  location = '%s',
           #  forecast_date = '%s',
           #  load_date = NULL""",
            'forecast_weather_staging':
            """INSERT INTO `power`.`forecast_weather_staging`
            (date,
             time,
             temperature,
             wind_speed,
             wind_direction,
             humidity,
             wind_dir_deg,
             dewpoint,
             conditions,
             qpf,
             pop,
             cloud_cover,
             state,
             location,
             forecast_date
             )
            VALUES (
                    %s, TIME_FORMAT(%s, '%%T'),
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                    )
            ON DUPLICATE KEY
            UPDATE temperature = values(temperature),
                   wind_speed = values(wind_speed),
                   humidity = values(humidity),
                   wind_direction = values(wind_direction),
                   wind_dir_deg = values(wind_dir_deg),
                   dewpoint = values(dewpoint),
                   conditions = values(conditions),
                   qpf = values(qpf),
                   pop = values(pop),
                   cloud_cover = values(cloud_cover),
                   processed_ind = 0,
                   load_date = NULL""",
            # 'imdaws_weather_stg':
            # """LOAD DATA INFILE '%s' REPLACE
            # INTO TABLE imdaws_weather_stg
            # FIELDS TERMINATED BY ';'
            # ESCAPED BY '\\\\'
            # LINES TERMINATED BY '\\r\\n'
            # IGNORE 1 LINES
            # (
            # Sl_No ,
            # STATION_NAME ,
            # @Date ,
            # Time_UTC ,
            # Latitude_N ,
            # Longitude_E ,
            # @SLP_hPa ,
            # MSLP ,
            # @Rainfall_mm ,
            # @Temperature_DegC ,
            # @Dewpoint_DegC ,
            # @Windspeed_Kt ,
            # @Winddir_Deg ,
            # @Tmax_DegC ,
            # @Tmin_DegC ,
            # @Ptend_hPa ,
            # @SSHM_hPa ,
            # State,
            # @DateTime_IST
            # )
            # SET Load_Date=NULL,
            # Date = str_to_date(@Date,'%%d-%%b-%%Y'),
            # SLP_hPa= IF(@SLP_hPa='NA',NULL,@SLP_hPa),
            # Rainfall_mm= IF(@Rainfall_mm='NA',NULL,@Rainfall_mm),
            # Temperature_DegC=IF(@Temperature_DegC='NA',NULL,@Temperature_DegC),
            # Dewpoint_DegC=IF(@Dewpoint_DegC='NA',NULL,@Dewpoint_DegC) ,
            # Windspeed_Kt=IF(@Windspeed_Kt='NA',NULL,@Windspeed_Kt) ,
            # Winddir_Deg=IF(@Winddir_Deg='NA',NULL,@Winddir_Deg) ,
            # Tmax_DegC=IF(@Tmax_DegC='NA',NULL,@Tmax_DegC),
            # Tmin_DegC=IF(@Tmin_DegC='NA',NULL,@Tmin_DegC),
            # Ptend_hPa=IF(@Ptend_hPa='NA',NULL,@Ptend_hPa),
            # SSHM_hPa=IF(@SSHM_hPa='NA',NULL,@SSHM_hPa),
            # DateTime_IST=addtime(timestamp(Date,Time_UTC),'05:30:00')"""
            'imdaws_weather_stg':
            """INSERT INTO imdaws_weather_stg
            (
            Sl_No ,
            Station_name ,
            Date ,
            Time_UTC ,
            Latitude_N ,
            Longitude_E ,
            SLP_hPa ,
            MSLP ,
            Rainfall_mm ,
            Temperature_DegC ,
            Dewpoint_DegC ,
            Windspeed_Kt ,
            Winddir_Deg ,
            Tmax_DegC ,
            Tmin_DegC ,
            Ptend_hPa ,
            SSHM_hPa ,
            State,
            DateTime_IST
            )
            VALUES (%s, %s, str_to_date(%s, '%%d-%%b-%%Y'),
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                   )
            ON DUPLICATE KEY
            UPDATE Latitude_N = values(Latitude_N) ,
                   Longitude_E = values(Longitude_E) ,
                   SLP_hPa = values(SLP_hPa) ,
                   MSLP = values(MSLP) ,
                   Rainfall_mm = values(Rainfall_mm) ,
                   Temperature_DegC = values(Temperature_DegC) ,
                   Dewpoint_DegC = values(Dewpoint_DegC) ,
                   Windspeed_Kt = values(Windspeed_Kt) ,
                   Winddir_Deg = values(Winddir_Deg) ,
                   Tmax_DegC = values(Tmax_DegC) ,
                   Tmin_DegC = values(Tmin_DegC) ,
                   Ptend_hPa = values(Ptend_hPa) ,
                   SSHM_hPa = values(SSHM_hPa),
                   Load_Ind = 0,
                   Load_Date = NULL"""}


def sql_table_insert_exec(dsnfile, tabname, data):
    """Loading data in the mysql table using bulk insert"""
    logger = logging.getLogger("sql_load_lib.sql_table_insert_exec")
    # print SQL_STMT[tabname]
    # print data
    # print 'INSIDEJOB', dsnfile
    # print SQL_STMT[tabname] % data[0]
    try:
        connection = dbconn.connect(dsnfile)
        cursor = connection.cursor()
        cursor.executemany(SQL_STMT[tabname], data)
        logger.info("Load Status %s", str(connection.info()))
        connection.commit()
        cursor.close()
    except Exception as err:
        logger.error("Load Failed %s", str(err))
        if connection.open:
            connection.rollback()
            cursor.close()
        raise
    return


def sql_table_load_exec(dsnfile, tabname, data):
    """Loading data in the mysql table using file loader"""
    logger = logging.getLogger("sql_load_lib.sql_table_load_exec")
    # print SQL_STMT[tabname] % data
    # print data
    # logger = logging.getLogger("sql_load_lib.sql_table_insert_exec")
    if not isinstance(data, tuple) and not isinstance(data, list):
        data = [data]
    try:
        # print data
        # print SQL_STMT[tabname] % tuple(data)
        connection = dbconn.connect(dsnfile)
        cursor = connection.cursor()
        # logger.debug(SQL_STMT[tabname], data)
        cursor.execute(SQL_STMT[tabname] % tuple(data))
        logger.info("Load Status %s", str(connection.info()))
        connection.commit()
        cursor.close()
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            connection.rollback()
            cursor.close()
        raise
    return


def sql_sp_load_exec(dsnfile, indate, inrevision,
                     instate, insubtype, instatus, type):
    """
        Calls stored procedure or sql statement
    """
    logger = logging.getLogger("sql_load_lib.sql_sp_load_exec")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    if type == 'UPINSPNT':
        try:
            #print( indate, inrevision, instate, insubtype, instatus, errm)
            logger.debug("%s %s %s %s %s %s", indate, inrevision, instate,
                         insubtype, instatus, errm)
            cursor.callproc('udef_schedule_tracker',
                            (indate, inrevision, instate, insubtype, instatus,
                             errm))
            cursor.execute("SELECT @_udef_schedule_tracker_5")
            data = cursor.fetchone()
            logger.info("SP Ended %s", data)
            #print("SP Ended", data)
        except Exception as err:
            logger.error("SP Failed %s", str(err))
            if connection.open:
                cursor.close()
            raise
    elif type == 'CHKPNT':
        sql = ("select a.revision, a.status, "
               "TIMESTAMPDIFF(MINUTE, a.modified_date, sysdate()) "
               "as mins_elapsed from job_schedule_tracker a, "
               "(select max(revision) revision from job_schedule_tracker " 
               "where date = %s and state = %s and sub_type = %s) b "
               "where a.revision = b.revision and a.date = %s " 
               "and a.state = %s and a.sub_type = %s")
        try:
            # print str(indate), instate, insubtype
            # logger.debug(sql, str(indate), instate)
            cursor.execute(sql, (str(indate), instate, insubtype, str(indate), instate, insubtype))
            data = cursor.fetchone()
            connection.commit()
            cursor.close()
        except Exception as err:
            logger.error("Failed %s", str(err))
            if connection.open:
                cursor.close()
            raise
    if connection.open:
        cursor.close()
    if type == 'UPINSPNT' and data != 'SUCCESS':
        logger.info('UPINSPT %s', data)
    elif type == 'CHKPNT' and data is None:
        return (-1, '', None)
    elif type == 'CHKPNT' and data is not None:
        return data
    else:
        return


def sql_nrldc_state_sch(dsnfile, indate, instate, indrawltype,
                        inrevision):
    """
        Calls stored procedure or sql statement
    """
    logger = logging.getLogger("sql_load_lib.sql_nrldc_state_sch")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        print(dsnfile, indate, instate, indrawltype, inrevision)
        logger.debug("%s %s %s %s %s %s", indate, instate, indrawltype,
                     inrevision, errm, errs)
        cursor.callproc('load_nrldc_state_drawl_schedule',
                        (indate, instate, indrawltype, inrevision,
                         errm, errs))
        cursor.execute("SELECT {}, {}".format(
            '@_load_nrldc_state_drawl_schedule_4',
            '@_load_nrldc_state_drawl_schedule_5'))
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('load_nrldc_state_drawl_schedule',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        raise
    return


def sql_nrldc_state_sch_revz(dsnfile, indate, instate, indrawltype,
                             inrevision):
    """
        Calls stored procedure or sql statement
    """
    logger = logging.getLogger("sql_load_lib.sql_nrldc_state_sch_revz")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        print(dsnfile, indate, instate, indrawltype, inrevision)
        logger.debug("%s %s %s %s %s %s", indate, instate, indrawltype,
                     inrevision, errm, errs)
        cursor.callproc('load_nrldc_state_drawl_sch_revz',
                        (indate, instate, indrawltype, inrevision,
                         errm, errs))
        cursor.execute("SELECT @_load_nrldc_state_drawl_sch_revz_4,"
                       "@_load_nrldc_state_drawl_sch_revz_5")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('load_nrldc_state_drawl_sch_revz',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        raise
    return


def sql_nrldc_entitlements(dsnfile, indate, instate, inrevision):
    """
        Calls stored procedure or sql statement
    """
    logger = logging.getLogger("sql_load_lib.sql_nrldc_entitlements")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        logger.debug("%s %s %s %s %s", indate, instate,
                     inrevision, errm, errs)
        cursor.callproc('load_nrldc_entitlements',
                        (indate, instate, inrevision,
                         errm, errs))
        cursor.execute("SELECT @_load_nrldc_entitlements_3,"
                       "@_load_nrldc_entitlements_4")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('load_nrldc_entitlements',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        raise
    return


def sql_nrldc_declared_capability(dsnfile, indate, inrevision):
    """
        Calls stored procedure or sql statement
    """
    logger = logging.getLogger("sql_load_lib.sql_nrldc_declared_capability")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        logger.debug("%s %s %s %s", indate,
                     inrevision, errm, errs)
        cursor.callproc('load_nrldc_declared_capability',
                        (indate, inrevision,
                         errm, errs))
        cursor.execute("SELECT @_load_nrldc_declared_capability_2,"
                       "@_load_nrldc_declared_capability_3")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('load_nrldc_declared_capability',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        raise
    return


def sql_bseb_realtime_forecast_surrender(dsnfile, indate):
    """
        Calls stored procedure sp_bseb_realtime_forecast_surrender
    """
    logger = logging.getLogger("sql_load_lib."
                               "sql_bseb_realtime_forecast_surrender")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        logger.debug("%s %s %s", indate, errm, errs)
        # cursor.execute("call sp_bseb_realtime_forecast_surrender(%s, %s, %s)",
        #                (indate, errm, errs))
        cursor.callproc('sp_bseb_realtime_forecast_surrender',
                        (indate, errm, errs))
        cursor.execute("SELECT @_sp_bseb_realtime_forecast_surrender_1,"
                       "@_sp_bseb_realtime_forecast_surrender_2")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_bseb_realtime_forecast_surrender',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        raise
    return


def sql_wtr_forecast_blk_ins_upd(dsnfile):
    """
        Calls stored procedure sp_wtr_forecast_blk_ins_upd
    """
    logger = logging.getLogger("sql_load_lib.sql_wtr_forecast_blk_ins_upd")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        logger.debug("%s %s", errm, errs)
        # cursor.execute("call sp_bseb_realtime_forecast_surrender(%s, %s, %s)",
        #                (indate, errm, errs))
        cursor.callproc('sp_wtr_forecast_blk_ins_upd',
                        (errm, errs))
        cursor.execute("SELECT @_sp_wtr_forecast_blk_ins_upd_0,"
                       "@_sp_wtr_forecast_blk_ins_upd_1")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        # connection.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_wtr_forecast_blk_ins_upd',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        # connection.close()
        raise
    return


def sql_wtr_forecast_hrblk_ins_upd(dsnfile):
    """
        Calls stored procedure sql_wtr_forecast_hrblk_ins_upd
    """
    logger = logging.getLogger("sql_load_lib.sql_wtr_forecast_hrblk_ins_upd")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        logger.debug("%s %s", errm, errs)
        # cursor.execute("call sp_bseb_realtime_forecast_surrender(%s, %s, %s)",
        #                (indate, errm, errs))
        cursor.callproc('sp_wtr_forecast_hrblk_ins_upd',
                        (errm, errs))
        cursor.execute("SELECT @_sp_wtr_forecast_hrblk_ins_upd_0,"
                       "@_sp_wtr_forecast_hrblk_ins_upd_1")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        # connection.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_wtr_forecast_hrblk_ins_upd',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        # connection.close()
        raise
    return


def sql_wtr_actual_blk_ins_upd(dsnfile):
    """
        Calls stored procedure sp_wtr_actual_blk_ins_upd
    """
    logger = logging.getLogger("sql_load_lib.sql_wtr_actual_blk_ins_upd")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        # cursor.execute("call sp_bseb_realtime_forecast_surrender(%s, %s, %s)",
        #                (indate, errm, errs))
        cursor.callproc('sp_wtr_actual_blk_ins_upd',
                        (errm, errs))
        cursor.execute("SELECT @_sp_wtr_actual_blk_ins_upd_0,"
                       "@_sp_wtr_actual_blk_ins_upd_1")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        # connection.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_wtr_actual_blk_ins_upd',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        # connection.close()
        raise
    return


def sql_wtr_unified_ins_upd(dsnfile, discom):
    """
        Calls stored procedure sql_wtr_forecast_hrblk_ins_upd
    """
    logger = logging.getLogger("sql_load_lib.sql_wtr_unified_ins_upd")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    truncate = False
    # logger.info("DB connection status %s %s", connection, connection.open)
    try:
        logger.debug("Parameters *%s*%s*%s*%s", truncate, discom, errm, errs)
        cursor.callproc('sp_wtr_unified_ins_upd',
                        (truncate, discom, errm, errs))
        cursor.execute("SELECT @_sp_wtr_unified_ins_upd_2,"
                       "@_sp_wtr_unified_ins_upd_3")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        #     connection.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_wtr_unified_ins_upd',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        #     connection.close()
        raise
    return


def sql_sp_wtr_unified_ins_upd_v2(dsnfile, state):
    """
        Calls stored procedure sql_wtr_forecast_hrblk_ins_upd
    """
    logger = logging.getLogger("sql_load_lib.sql_wtr_unified_ins_upd")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    truncate = False
    # logger.info("DB connection status %s %s", connection, connection.open)
    try:
        logger.debug("Parameters *%s*%s*%s*%s", truncate, state, errm, errs)
        cursor.callproc('sp_wtr_unified_ins_upd_v2',
                        (truncate, discom, errm, errs))
        cursor.execute("SELECT @_sp_wtr_unified_ins_upd_v2_2,"
                       "@_sp_wtr_unified_ins_upd_v2_3")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        #     connection.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_wtr_unified_ins_upd_v2',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        #     connection.close()
        raise
    return


def sql_sp_wtr_unified2_ins_upd(dsnfile, discom, state):
    """Call stored procedure sp_wtr_unified2_ins_upd."""
    logger = logging.getLogger("sql_load_lib.sp_wtr_unified2_ins_upd")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    fullupdate = False
    # logger.info("DB connection status %s %s", connection, connection.open)
    try:
        logger.debug("Parameters *%s*%s*%s*%s", fullupdate, state, errm, errs)
        cursor.callproc('sp_wtr_unified2_ins_upd',
                        (fullupdate, state, errm, errs))
        cursor.execute("SELECT @_sp_wtr_unified2_ins_upd_2,"
                       "@_sp_wtr_unified2_ins_upd_3")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        #     connection.close()
        if data[1] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_wtr_unified2_ins_upd',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        #     connection.close()
        raise
    return


def sql_sp_wtr_ibm_actualhrblk_ins_upd(dsnfile, state):
    """
        Calls stored procedure sql_wtr_forecast_hrblk_ins_upd.
    """
    logger = logging.getLogger("sql_load_lib."
                               "sql_sp_wtr_ibm_actualhrblk_ins_upd")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    # logger.info("DB connection status %s %s", connection, connection.open)
    try:
        logger.debug("Parameters *%s*%s*%s*%s", state, errm, errs)
        cursor.callproc('sp_wtr_ibm_actualhrblk_ins_upd',
                        (state, errm, errs))
        cursor.execute("SELECT @_sp_wtr_ibm_actualhrblk_ins_upd_1,"
                       "@_sp_wtr_ibm_actualhrblk_ins_upd_2")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        #     connection.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_wtr_ibm_actualhrblk_ins_upd',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        #     connection.close()
        raise
    return


def sql_sp_wtr_ibm_forecast_hrblk_ins_upd(dsnfile, state):
    """
        Calls stored procedure sql_wtr_forecast_hrblk_ins_upd.
    """
    logger = logging.getLogger("sql_load_lib."
                               "sql_sp_wtr_ibm_forecast_hrblk_ins_upd")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    # logger.info("DB connection status %s %s", connection, connection.open)
    try:
        logger.debug("Parameters *%s*%s*%s*%s", state, errm, errs)
        cursor.callproc('sp_wtr_ibm_forecast_hrblk_ins_upd',
                        (state, errm, errs))
        cursor.execute("SELECT @_sp_wtr_ibm_forecast_hrblk_ins_upd_1,"
                       "@_sp_wtr_ibm_forecast_hrblk_ins_upd_2")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        #     connection.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_wtr_ibm_forecast_hrblk_ins_upd',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        #     connection.close()
        raise
    return


def sql_powercut_station_discom_upd(dsnfile):
    """
        Calls stored procedure sp_powercut_station_discom_upd
    """
    logger = logging.getLogger("sql_load_lib.sql_powercut_station_discom_upd")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        # logger.debug("%s %s", errm, errs)
        # cursor.execute("call sp_bseb_realtime_forecast_surrender(%s, %s, %s)",
        #                (indate, errm, errs))
        cursor.callproc('sp_powercut_station_discom_upd',
                        (errm, errs))
        cursor.execute("SELECT @_sp_powercut_station_discom_upd_0,"
                       "@_sp_powercut_station_discom_upd_1")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        #     connection.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             '{} other than SUCCESS'
                             .format('sp_powercut_station_discom_upd',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        #     connection.close()
        raise
    return


def sql_weather_actual_load(dsnfile, state=None):
    """
        Calls stored procedure sp_wtr_imdaws_actualhrblk_ins_upd
    """
    logger = logging.getLogger("sql_load_lib.sql_weather_actual_load")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    # logger.info("DB connection status %s %s", connection, connection.open)
    try:
        cursor.callproc('sp_wtr_imdaws_actualhrblk_ins_upd',
                        (state, errm, errs))
        cursor.execute("SELECT @_sp_wtr_imdaws_actualhrblk_ins_upd_1,"
                       "@_sp_wtr_imdaws_actualhrblk_ins_upd_2")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        #     connection.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             '{} other than SUCCESS'
                             .format('sp_wtr_imdaws_actualhrblk_ins_upd',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        #     connection.close()
        raise
    return


def sql_sp_realtime_data_fetch(dsnfile, date, discom, level):
    """Call stored procedure sp_wtr_unified2_ins_upd."""
    logger = logging.getLogger("sql_load_lib.sql_sp_realtime_data_fetch")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    errm = ''
    errs = ''
    # logger.info("DB connection status %s %s", connection, connection.open)
    try:
        logger.debug("Parameters *%s*%s*%s*%s*%s",
                     date, discom, level, errm, errs)
        cursor.callproc('sp_realtime_data_fetch',
                        (date, discom, level, errm, errs))
        results = cursor.fetchall()
        cursor.close()
        connection.close()
        # cursor.execute("SELECT @_sp_realtime_data_fetch_3,"
        #                "@_sp_realtime_data_fetch_4")
        # data = cursor.fetchone()
        # logger.info("SP Ended %s %s", data[0], data[1])
        # cursor.close()
        # #     connection.close()
        # if data[0] != 'SUCCESS':
        #     raise ValueError('Stored Procedure {} exited with status'
        #                      ' {} other than SUCCESS'
        #                      .format('sp_realtime_data_fetch',
        #                              data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
            connection.close()
        raise
    return results


def sql_sp_internal_sch_update(dsnfile, discom):
    """
        Calls stored procedure sql_wtr_forecast_hrblk_ins_upd
    """
    logger = logging.getLogger("sql_load_lib.sql_sp_internal_sch_update")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    # logger.info("DB connection status %s %s", connection, connection.open)
    try:
        logger.debug("Parameters *%s*%s*%s*%s", discom, errm, errs)
        cursor.callproc('sp_internal_sch_update',
                        (discom, errm, errs))
        cursor.execute("SELECT @_sp_internal_sch_update_1,"
                       "@_sp_internal_sch_update_2")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        #     connection.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_internal_sch_update',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        #     connection.close()
        raise
    return


def sql_sp_adani_scada_blk(dsnfile, discom):
    """Call stored procedure sp_wtr_unified2_ins_upd."""
    logger = logging.getLogger("sql_load_lib.sql_sp_adani_scada_blk")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    fullupdate = False
    # logger.info("DB connection status %s %s", connection, connection.open)
    try:
        logger.debug("Parameters *%s*%s*%s*%s", fullupdate, discom, errm, errs)
        cursor.callproc('sp_adani_scada_blk',
                        (fullupdate, discom, errm, errs))
        cursor.execute("SELECT @_sp_adani_scada_blk_2,"
                       "@_sp_adani_scada_blk_3")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        #     connection.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_adani_scada_blk',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        #     connection.close()
        raise
    return


def sql_sp_load_realtime_v2(dsnfile, discom):
    """Call stored procedure sp_wtr_unified2_ins_upd."""
    logger = logging.getLogger("sql_load_lib.sql_sp_load_realtime_v2")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    # logger.info("DB connection status %s %s", connection, connection.open)
    try:
        logger.debug("Parameters *%s*%s*%s*%s*%s",
                     discom)
        cursor.callproc('sp_load_realtime_v2',
                        (discom, errm, errs))
        cursor.execute("SELECT @_sp_load_realtime_v2_1,"
                       "@_sp_load_realtime_v2_2")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_realtime_data_fetch',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        #     connection.close()
        raise
    return


def sql_sp_dayahead_int_dc_load(dsnfile, discom):
    """Call stored procedure sp_wtr_unified2_ins_upd."""
    logger = logging.getLogger("sql_load_lib.sql_sp_dayahead_int_dc_load")
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    # logger.info("DB connection status %s %s", connection, connection.open)
    try:
        logger.debug("Parameters *%s*%s*%s*%s*%s",
                     discom)
        cursor.callproc('sp_dayahead_int_dc_load',
                        (discom, errm, errs))
        cursor.execute("SELECT @_sp_dayahead_int_dc_load_1,"
                       "@_sp_dayahead_int_dc_load_2")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_dayahead_int_dc_load',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        #     connection.close()
        raise
    return
