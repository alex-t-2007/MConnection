package org.jmethod.mconnection;

import static org.jmethod.mconnection.MConnection.DEFAULT;
import static org.jmethod.mconnection.MConnection.ID;
import static org.jmethod.mconnection.MConnection.LIMIT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DriverConnectionExample {

    private static MConnection testCreateMc() {
        Map<String, String> idNames = new HashMap<>();
        idNames.put(DEFAULT, ID);
        idNames.put("AUDC_PCLIB_BUSINESS_OBJECT", "SYSNAME");
        idNames.put("AUDC_PCLIB_BUSINESS_OPERATIONS", "SYSNAME");
        idNames.put("AUDC_PCLIB_BUSINESS_SERVICE", "SYSNAME");
        idNames.put("AUDC_TASK_PARAM", "SYSNAME");

        Map<String, String> sequences = new HashMap<>();
        sequences.put("AUDC_ACLIB_ACTOR_ACCOUNT", "audc_aclib_actor_account_id_seq");
        sequences.put("AUDC_ACLIB_CLIENT_IP_RANGE", "audc_aclib_client_ip_range_id_seq");
        sequences.put("AUDC_ACLIB_ROLE_WORKPLACE", "audc_aclib_role_workplace_id_seq");
        sequences.put("AUDC_PARAM", "audc_param_seq");
        sequences.put("AUDC_TASK", "audc_task_seq");
        sequences.put("AUDC_USER", "audc_user_seq");

        MConnection mc = MConnection.createMConnection(
                "org.postgresql.Driver",
                "jdbc:postgresql://127.0.0.1:5432/ib",
                "audc",
                "audc_psw",
                LIMIT,
                idNames,
                sequences
        );
        return mc;
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

    private static List<DbData> testCreateRows(MConnection mc) {
        List<DbData> list = new ArrayList<>(3);
        list.add(testCreateRow(mc, "rollover_period_1", "1"));
        list.add(testCreateRow(mc, "rollover_period_2", "2"));
        list.add(testCreateRow(mc, "rollover_period_3", "3"));
        return list;
    }

    private static List<DbData> testReadRows(MConnection mc, List<DbData> list) {
        List<DbData> listResult = new ArrayList<>(3);
        for (DbData dbData : list) {
            if (dbData.getDone()) {
                DbData dbdResult = mc.read(dbData.getTableName(), dbData.getId(), "*");
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

    private static List<DbData> testUpdateRows(MConnection mc, List<DbData> list) {
        List<DbData> listRes = new ArrayList<>(3);
        listRes.add(testUpdateRow(mc, list.get(0).getId(), "rollover_period_1_@", "11", "TYPE1"));
        listRes.add(testUpdateRow(mc, list.get(1).getId(), "rollover_period_2_@", "21", "TYPE2"));
        listRes.add(testUpdateRow(mc, list.get(2).getId(), "rollover_period_3_@", "31", "TYPE3"));
        return list;
    }

    private static List<DbData> testFindRows(MConnection mc, String sql, boolean tnFlag) {
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

    private static List<DbData> testFindRows(MConnection mc, String sql, List<Object> params, boolean tnFlag) {
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

    private static List<DbData> testDeleteRows(MConnection mc, List<DbData> list) {
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

    private static void test() {
        MConnection mc = testCreateMc();
        if (!mc.isConnectionActivate()) {
            Utils.outln("Can't create Driver connection mc=" + mc);
            Utils.outln("mc.getConnectionError()=" + mc.getConnectionError());
            Utils.outln("--------------------------------------------------------------------------------");
            return;
        }

        Utils.outln("mc=" + mc);
        Utils.outln("mc.connection=" + mc.getConnection());
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> createdList = testCreateRows(mc);
        Utils.outln("createdList=" + createdList);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> readList = testReadRows(mc, createdList);
        Utils.outln("readList=" + readList);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> updateList = testUpdateRows(mc, createdList);
        Utils.outln("updateList=" + updateList);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> readList2 = testReadRows(mc, updateList);
        Utils.outln("readList2=" + readList2);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> findList = testFindRows(mc, "SELECT * FROM AUDC_PARAM ORDER BY ID", false);
        Utils.outln("findList=" + findList);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> findList2 = testFindRows(mc,
                "SELECT * FROM AUDC_PARAM " +
                     "WHERE "+
                       "PARAM_NAME LIKE '%_@' "+
                     "ORDER BY ID",
                true
        );
        Utils.outln("findList2=" + findList2);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> findList3 = testFindRows(mc,
                "SELECT " +
                        "CURATOR_ACTION, "+
                        "DEFAULT_VALUE, "+
                        "PARAM_DESCR "+
                     "FROM AUDC_PARAM " +
                        "WHERE "+
                     "PARAM_NAME LIKE '%_@' "+
                     "ORDER BY ID",
                false
        );
        Utils.outln("findList3=" + findList3);
        Utils.outln("--------------------------------------------------------------------------------");

        List<Object> params = new ArrayList<>();
        params.add("%_@");
        List<DbData> findList4 = testFindRows(mc,
        "SELECT " +
                "CURATOR_ACTION, "+
                "DEFAULT_VALUE, "+
                "PARAM_DESCR "+
              "FROM AUDC_PARAM " +
              "WHERE "+
                "PARAM_NAME NOT LIKE ? "+
              "ORDER BY ID",
              params,
             false
        );
        Utils.outln("findList4=" + findList4);
        Utils.outln("--------------------------------------------------------------------------------");

        String sql =
            "SELECT " +
                "AUDC_TASK_PARAM.PARAM_VALUE, AUDC_TASK.*, AUDC_PARAM.* " +
            "FROM AUDC_TASK_PARAM " +
            "LEFT JOIN AUDC_TASK  ON AUDC_TASK.ID  = AUDC_TASK_PARAM.TASK_ID " +
            "LEFT JOIN AUDC_PARAM ON AUDC_PARAM.ID = AUDC_TASK_PARAM.PARAM_ID";
        List<DbData> findList5 = testFindRows(mc, sql, true);
        Utils.outln("findList5=" + findList5);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> deletedList = testDeleteRows(mc, createdList);
        Utils.outln("deletedList=" + deletedList);
        Utils.outln("--------------------------------------------------------------------------------");
    }

    public static void main(String[] args) {
        test();
    }
}
