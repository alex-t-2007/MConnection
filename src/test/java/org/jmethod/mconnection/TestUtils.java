package org.jmethod.mconnection;

import static org.jmethod.mconnection.MConnection.DEFAULT;
import static org.jmethod.mconnection.MConnection.ID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {

    // Сервер H2            : "jdbc:h2:tcp://localhost:9094/./dbH2/bank"
    // Embeded файл './h2db': "jdbc:h2:./h2db";
    // DB n memory:
    public static final String URL = "jdbc:h2:mem:ib;\\MODE=PostgreSQL;\\INIT=CREATE SCHEMA IF NOT EXISTS audc\\;SET SCHEMA audc\\;create domain if not exists JSONB as other;\\DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";

    public static final String DRIVER = "org.h2.Driver";
    public static final String DATA_SOURCE = "org.h2.jdbcx.JdbcDataSource";
    public static final String LOGIN = "sa";
    public static final String PASSWORD = "";
    public static final Map<String, String> ID_NAMES = genIdNames();
    public static final Map<String, String> SEQUENCES = genSequences();

    public static void testCreateTables(MConnection mc) {
        String sqlScript =
                //"CREATE SCHEMA AUDC AUTHORIZATION sa;\r\n" +
                "CREATE TABLE AUDC_PARAM (\r\n" +
                        "  ID             BIGINT PRIMARY KEY,\r\n" +
                        "  CURATOR_ACTION VARCHAR(20),\r\n" +
                        "  DEFAULT_VALUE  VARCHAR(100),\r\n" +
                        "  PARAM_DESCR    VARCHAR(500),\r\n" +
                        "  PARAM_NAME     VARCHAR(50),\r\n" +
                        "  PARAM_TYPE     VARCHAR(10)\r\n" +
                        ");\r\n" +
                        "CREATE INDEX IDX__AUDC_PARAM__PARAM_NAME ON AUDC_PARAM (PARAM_NAME)";
        boolean ok = mc.executeSqlScript(sqlScript);
        Utils.outln(ok ? "sqlScript=" + sqlScript : "");
        Utils.outln("--------------------------------------------------------------------------------");
    }

    public static void testDropTables(MConnection mc) {
        String sqlScript =
            "DROP INDEX IDX__AUDC_PARAM__PARAM_NAME;\r\n" +
            "DROP TABLE AUDC_PARAM;";
        boolean ok = mc.executeSqlScript(sqlScript);
        Utils.outln(ok ? "sqlScript=" + sqlScript : "");
        Utils.outln("--------------------------------------------------------------------------------");
    }

    private static DbData testCreateRow(MConnection mc, String name, String val) {
        DbData dbd = new DbData("AUDC_PARAM");
        dbd.setString("CURATOR_ACTION", "ARCHIVE");
        dbd.setString("DEFAULT_VALUE", val);
        dbd.setString("PARAM_DESCR", "Период создания нового индекса");
        dbd.setString("PARAM_NAME", name);
        dbd.setString("PARAM_TYPE", "INT");
        dbd = mc.create(dbd, true);
        if (dbd.getDone()) {
            Utils.outln("-- Создана строка: dbd=" + dbd.toStr());
        } else {
            Utils.outln("?? Не могу создать строку: dbd=" + dbd.toStr());
        }
        return dbd;
    }

    public static List<DbData> deleteAll(MConnection mc) {
        FindData fd = mc.find("SELECT ID FROM AUDC_PARAM ORDER BY ID", false);

        List<DbData> listResult = new ArrayList<>(3);
        for (DbData dbData : fd.getDbDatas()) {
            if (dbData.getDone()) {
                DbData dbdResult = mc.delete("AUDC_PARAM", dbData.getObject(0), true);
                if (dbdResult.getDone()) {
                    Utils.outln("-- deleteAll: Удалена строка: dbdResult=" + dbdResult.toStr());
                } else {
                    Utils.outln("?? deleteAll: Не могу удалить строку: dbdResult=" + dbdResult.toStr());
                }
                listResult.add(dbdResult);
            }
        }
        return listResult;
    }

    private static DbData testCreateRowId(MConnection mc, Object id, String name, String val) {
        DbData dbd = new DbData("AUDC_PARAM");
        dbd.setObject(mc.getIdName(dbd.getTableName()), id);
        dbd.setString("CURATOR_ACTION", "ARCHIVE");
        dbd.setString("DEFAULT_VALUE", val);
        dbd.setString("PARAM_DESCR", "Период создания нового индекса");
        dbd.setString("PARAM_NAME", name);
        dbd.setString("PARAM_TYPE", "INT");
        dbd = mc.create(dbd, true);
        if (dbd.getDone()) {
            Utils.outln("-- Создана строка: dbd=" + dbd.toStr());
        } else {
            Utils.outln("?? Не могу создать строку: dbd=" + dbd.toStr());
        }
        return dbd;
    }

    public static List<DbData> testCreateRows(MConnection mc) {
        List<DbData> list = new ArrayList<>(3);
        list.add(testCreateRowId(mc, 1L, "rollover_period_1", "1"));
        list.add(testCreateRowId(mc, 2L, "rollover_period_2", "2"));
        list.add(testCreateRowId(mc, 3L, "rollover_period_3", "3"));
        return list;
    }

    public static List<DbData> testReadRows(MConnection mc, List<DbData> list) {
        List<DbData> listResult = new ArrayList<>(3);
        for (DbData dbData : list) {
            if (dbData.getDone()) {
                //DbData dbdResult = mc.read(dbData.getTableName(), dbData.getId(), "*");
                DbData dbdResult = mc.read("AUDC_PARAM", dbData.getId(), "*");
                if (dbdResult.getDone()) {
                    Utils.outln("-- Прочитана строка: dbdResult=" + dbdResult.toStr());
                } else {
                    Utils.outln("?? Не могу прочитать строку: dbdResult=" + dbdResult.toStr());
                }
                listResult.add(dbdResult);
            }
        }
        return listResult;
    }

    private static DbData testUpdateRow(MConnection mc, Object id, String name, String val, String type) {
        DbData dbd = new DbData("AUDC_PARAM", id);
        dbd.setString("DEFAULT_VALUE", val);
        dbd.setString("PARAM_NAME", name);
        dbd.setString("PARAM_TYPE", type);
        dbd = mc.update(dbd, true);
        if (dbd.getDone()) {
            Utils.outln("-- Модифицирована строка: dbd=" + dbd.toStr());
        } else {
            Utils.outln("?? Не могу модифицировать строку: dbd=" + dbd.toStr());
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
            Utils.outln("?? Строки не найдены: sql=" + sql);
            return fd.getDbDatas();
        }
        for (DbData dbData : fd.getDbDatas()) {
            Utils.outln("-- Найденная строка: dbData=" + dbData.toStr());
        }
        return fd.getDbDatas();
    }

    public static List<DbData> testFindRows(MConnection mc, String sql, List<Object> params, boolean tnFlag) {
        FindData fd = mc.find(sql, params, tnFlag);

        if (fd.getQuant() <= 0) {
            Utils.outln("?? Строки не найдены: sql=" + sql);
            return fd.getDbDatas();
        }
        for (DbData dbData : fd.getDbDatas()) {
            Utils.outln("-- Найденная строка: dbData=" + dbData.toStr());
        }
        return fd.getDbDatas();
    }

    public static List<DbData> testDeleteRows(MConnection mc, List<DbData> list) {
        List<DbData> listResult = new ArrayList<>(3);
        for (DbData dbData : list) {
            if (dbData.getDone()) {
                DbData dbdResult = mc.delete(dbData.getTableName(), dbData.getId(), true);
                if (dbdResult.getDone()) {
                    Utils.outln("-- Удалена строка: dbdResult=" + dbdResult.toStr());
                } else {
                    Utils.outln("?? Не могу удалить строку: dbdResult=" + dbdResult.toStr());
                }
                listResult.add(dbdResult);
            }
        }
        return listResult;
    }

    private static Map<String, String> genIdNames() {
        Map<String, String> idNames = new HashMap<>();
        idNames.put(DEFAULT, ID);
        idNames.put("AUDC_PCLIB_BUSINESS_OBJECT", "SYSNAME");
        idNames.put("AUDC_PCLIB_BUSINESS_OPERATIONS", "SYSNAME");
        idNames.put("AUDC_PCLIB_BUSINESS_SERVICE", "SYSNAME");
        idNames.put("AUDC_TASK_PARAM", "SYSNAME");
        return idNames;
    }

    private static Map<String, String> genSequences() {
        Map<String, String> sequences = new HashMap<>();
        sequences.put("AUDC_ACLIB_ACTOR_ACCOUNT", "audc_aclib_actor_account_id_seq");
        sequences.put("AUDC_ACLIB_CLIENT_IP_RANGE", "audc_aclib_client_ip_range_id_seq");
        sequences.put("AUDC_ACLIB_ROLE_WORKPLACE", "audc_aclib_role_workplace_id_seq");
        sequences.put("AUDC_PARAM", "audc_param_seq");
        sequences.put("AUDC_TASK", "audc_task_seq");
        sequences.put("AUDC_USER", "audc_user_seq");
        return sequences;
    }
}
