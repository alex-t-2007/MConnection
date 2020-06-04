package org.jmethod.mconnection;

import static org.jmethod.mconnection.MConnection.DEFAULT;
import static org.jmethod.mconnection.MConnection.ID;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {

    // Сервер H2            : "jdbc:h2:tcp://localhost:9094/./dbH2/bank"
    // Embeded файл './h2db': "jdbc:h2:./h2db";
    // DB in memory:
    public static final String URL = "jdbc:h2:mem:ib;\\MODE=PostgreSQL;\\INIT=CREATE SCHEMA IF NOT EXISTS audc\\;SET SCHEMA audc\\;create domain if not exists JSONB as other;\\DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";

    public static final String DRIVER = "org.h2.Driver";
    public static final String DATA_SOURCE = "org.h2.jdbcx.JdbcDataSource";
    public static final String LOGIN = "sa";
    public static final String PASSWORD = "";
    public static final String TABLE_NAME = "AUDC_PARAM";
    public static final String INDEX_NAME = "IDX__" + TABLE_NAME + "__PARAM_NAME";
    public static final String SEQUENCE = TABLE_NAME + "_ID_SEQ";
    public static final Map<String, String> ID_NAMES = genIdNames();
    public static final Map<String, String> SEQUENCES = genSequences();

    public static void testCreateTables(MConnection mc) {
        //    INT
        //    BOOLEAN
        //    TINYINT
        //    SMALLINT
        //    BIGINT      -9223372036854775808 to 9223372036854775807
        //    IDENTITY
        //    DECIMAL
        //    DOUBLE
        //    REAL
        //    TIME
        //    TIME WITH TIME ZONE
        //    DATE
        //    TIMESTAMP
        //    TIMESTAMP WITH TIME ZONE
        //    BINARY
        //    OTHER
        //    VARCHAR
        //    VARCHAR_IGNORECASE
        //    CHAR
        //    BLOB
        //    CLOB
        //    UUID
        //    ARRAY
        //    ENUM
        //    GEOMETRY
        //    JSON
        //    INTERVAL

        //"  O_TIME_TZ        TIME WITH TIME ZONE,\n" +
        String sqlScript =
            //"CREATE SCHEMA AUDC AUTHORIZATION sa;\n" +
            "CREATE TABLE " + TABLE_NAME + " (\n" +
            "  ID               BIGINT PRIMARY KEY,\n" +
            "  CURATOR_ACTION   VARCHAR(20),\n" +
            "  DEFAULT_VALUE    VARCHAR(100),\n" +
            "  PARAM_DESCR      VARCHAR,\n" +
            "  PARAM_NAME       VARCHAR(50),\n" +
            "  PARAM_TYPE       VARCHAR(10),\n" +
            "  O_DATE           DATE,\n" +
            "  O_TIME           TIME,\n" +
            "  O_TIME_STAMP     TIMESTAMP,\n" +
            "  O_TIME_STAMP_TZ  TIMESTAMP WITH TIME ZONE,\n" +
            "  O_LONG           BIGINT,\n" +
            "  O_INT            INT,\n" +
            "  O_SHORT          SMALLINT,\n" +
            "  O_BYTE           TINYINT,\n" +
            "  O_BOOL           BOOLEAN,\n" +
            "  SUMMA            NUMERIC,\n" +
            "  SUM_CUR          NUMERIC\n" +
            ");\r\n" +

            "CREATE INDEX " + INDEX_NAME + " ON " + TABLE_NAME + " (PARAM_NAME);\r\n" +

            "CREATE SEQUENCE " + SEQUENCE + "\r\n" +
            "  INCREMENT 1\r\r\n" +
            "  START 1\r\n" +
            "  MINVALUE 1\r\n" +
            "  MAXVALUE 9223372036854775807\r\n" +
            "  CACHE 1;";

        boolean ok = mc.executeSqlScript(sqlScript);
        Utils.outln(ok ? "sqlScript=\r\n" + sqlScript : "");
        Utils.outln("--------------------------------------------------------------------------------");
    }

    public static void testDropTables(MConnection mc) {
        String sqlScript =
            "DROP SEQUENCE " + SEQUENCE + ";\r\n" +
            "DROP INDEX " + INDEX_NAME + ";\r\n" +
            "DROP TABLE " + TABLE_NAME + ";";
        boolean ok = mc.executeSqlScript(sqlScript);
        Utils.outln(ok ? "sqlScript=\r\n" + sqlScript : "");
        Utils.outln("--------------------------------------------------------------------------------");
    }

    private static DbData testCreateRow(MConnection mc, String name, String val) {
        Date cd = new Date();
        java.sql.Date sqlCd = new java.sql.Date(cd.getTime());
        Time ct = new Time(cd.getTime());
        Timestamp cts = new Timestamp(cd.getTime());

        DbData dbd = new DbData(TABLE_NAME);
        dbd.setString ("CURATOR_ACTION", "ARCHIVE");
        dbd.setString ("DEFAULT_VALUE", val);
        dbd.setString ("PARAM_DESCR", "Period of new index creation");
        dbd.setString ("PARAM_NAME", name);
        dbd.setString ("PARAM_TYPE", "INT");
        dbd.setSqlDate("O_DATE", sqlCd);
        dbd.setTime   ("O_TIME", ct);
        dbd.setTimestamp("O_TIME_STAMP", cts);
        dbd.setObject ("O_TIME_STAMP_TZ", sqlCd); // for check setObject
        dbd.setLong   ("O_LONG", Long.MAX_VALUE);
        dbd.setInt    ("O_INT", Integer.MAX_VALUE);
        dbd.setShort  ("O_SHORT", Short.MAX_VALUE);
        dbd.setByte   ("O_BYTE", Byte.MAX_VALUE);
        dbd.setBoolean("O_BOOL", true);
        dbd.setBigDecimal("SUMMA", BigDecimal.valueOf(Long.MAX_VALUE, 2));
        dbd.setDouble ("SUM_CUR", 12345678901234567.89);

        dbd = mc.create(dbd, true);
        if (dbd.getDone()) {
            Utils.outln("-- Created row: dbd=" + dbd.toStr());
        } else {
            Utils.outln("?? Can't create row: dbd=" + dbd.toStr());
        }
        return dbd;
    }

    private static DbData testCreateRowId(MConnection mc, Object id, String name, String val) {
        DbData dbd = new DbData(TABLE_NAME);
        dbd.setObject(mc.getIdName(dbd.getTableName()), id);
        dbd.setString("CURATOR_ACTION", "ARCHIVE");
        dbd.setString("DEFAULT_VALUE", val);
        dbd.setString("PARAM_DESCR", "Period of new index creation");
        dbd.setString("PARAM_NAME", name);
        dbd.setString("PARAM_TYPE", "INT");
        dbd = mc.create(dbd, true);
        if (dbd.getDone()) {
            Utils.outln("-- Created row: dbd=" + dbd.toStr());
        } else {
            Utils.outln("?? Can't create row: dbd=" + dbd.toStr());
        }
        return dbd;
    }

    public static List<DbData> deleteAll(MConnection mc) {
        FindData fd = mc.find("SELECT ID FROM " + TABLE_NAME + " ORDER BY ID", false);

        List<DbData> listResult = new ArrayList<>(3);
        for (DbData dbData : fd.getDbDatas()) {
            if (dbData.getDone()) {
                DbData dbdResult = mc.delete(TABLE_NAME, dbData.getObject(0), true);
                if (dbdResult.getDone()) {
                    Utils.outln("-- deleteAll: Deleted row: dbdResult=" + dbdResult.toStr());
                } else {
                    Utils.outln("?? deleteAll: Can't delete row: dbdResult=" + dbdResult.toStr());
                }
                listResult.add(dbdResult);
            }
        }
        return listResult;
    }

    public static List<DbData> testCreateRows(MConnection mc) {
        List<DbData> list = new ArrayList<>(3);
        list.add(testCreateRowId(mc, 1001L, "rollover_period_1", "1"));
        list.add(testCreateRowId(mc, 1002L, "rollover_period_2", "2"));
        list.add(testCreateRowId(mc, 1003L, "rollover_period_3", "3"));

        list.add(testCreateRow(mc, "rollover_period_1", "1"));
        list.add(testCreateRow(mc, "rollover_period_2", "2"));
        list.add(testCreateRow(mc, "rollover_period_3", "3"));
        return list;
    }

    public static List<DbData> testReadRows(MConnection mc, List<DbData> list) {
        List<DbData> listResult = new ArrayList<>(3);
        for (DbData dbData : list) {
            if (dbData.getDone()) {
                //DbData dbdResult = mc.read(dbData.getTableName(), dbData.getId(), "*");
                DbData dbdResult = mc.read(TABLE_NAME, dbData.getId(), "*");
                if (dbdResult.getDone()) {
                    Utils.outln("-- Readed row: dbdResult=" + dbdResult.toStr());
                } else {
                    Utils.outln("?? Can't read row: dbdResult=" + dbdResult.toStr());
                }
                listResult.add(dbdResult);
            }
        }
        return listResult;
    }

    private static DbData testUpdateRow(MConnection mc, Object id, String name, String val, String type) {
        DbData dbd = new DbData(TABLE_NAME, id);
        dbd.setString("DEFAULT_VALUE", val);
        dbd.setString("PARAM_NAME", name);
        dbd.setString("PARAM_TYPE", type);
        dbd = mc.update(dbd, true);
        if (dbd.getDone()) {
            Utils.outln("-- Updated row: dbd=" + dbd.toStr());
        } else {
            Utils.outln("?? Can't update row: dbd=" + dbd.toStr());
        }
        return dbd;
    }

    public static List<DbData> testUpdateRows(MConnection mc, List<DbData> list) {
        List<DbData> listRes = new ArrayList<>(3);
        listRes.add(testUpdateRow(mc, list.get(0).getId(), "rollover_period_1_@", "11", "TYPE1"));
        listRes.add(testUpdateRow(mc, list.get(1).getId(), "rollover_period_2_@", "21", "TYPE2"));
        listRes.add(testUpdateRow(mc, list.get(2).getId(), "rollover_period_3_@", "31", "TYPE3"));
        return list;
    }

    public static List<DbData> testFindRows(MConnection mc, String sql, boolean tnFlag) {
        FindData fd = mc.find(sql, tnFlag);

        if (fd.getQuant() <= 0) {
            Utils.outln("?? Rows not found: sql=" + sql);
            return fd.getDbDatas();
        }
        for (DbData dbData : fd.getDbDatas()) {
            Utils.outln("-- Found row: dbData=" + dbData.toStr());
        }
        return fd.getDbDatas();
    }

    public static List<DbData> testFindRows(MConnection mc, String sql, List<Object> params, boolean tnFlag) {
        FindData fd = mc.find(sql, params, tnFlag);

        if (fd.getQuant() <= 0) {
            Utils.outln("?? Rows not found: sql=" + sql);
            return fd.getDbDatas();
        }
        for (DbData dbData : fd.getDbDatas()) {
            Utils.outln("-- Found row: dbData=" + dbData.toStr());
        }
        return fd.getDbDatas();
    }

    public static List<DbData> testDeleteRows(MConnection mc, List<DbData> list) {
        List<DbData> listResult = new ArrayList<>(3);
        for (DbData dbData : list) {
            if (dbData.getDone()) {
                DbData dbdResult = mc.delete(dbData.getTableName(), dbData.getId(), true);
                if (dbdResult.getDone()) {
                    Utils.outln("-- Deleted row: dbdResult=" + dbdResult.toStr());
                } else {
                    Utils.outln("?? Can't delete row: dbdResult=" + dbdResult.toStr());
                }
                listResult.add(dbdResult);
            }
        }
        return listResult;
    }

    private static Map<String, String> genIdNames() {
        Map<String, String> idNames = new HashMap<>();
        idNames.put(DEFAULT, ID);
        idNames.put(TABLE_NAME, ID);
        return idNames;
    }

    private static Map<String, String> genSequences() {
        Map<String, String> sequences = new HashMap<>();
        sequences.put(TABLE_NAME, SEQUENCE);
        return sequences;
    }
}
