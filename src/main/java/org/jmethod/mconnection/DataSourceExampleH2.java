package org.jmethod.mconnection;

import static org.jmethod.mconnection.ExampleUtils.DRIVER;
import static org.jmethod.mconnection.ExampleUtilsH2.DATA_SOURCE;
import static org.jmethod.mconnection.ExampleUtilsH2.ID_NAMES;
import static org.jmethod.mconnection.ExampleUtilsH2.LOGIN;
import static org.jmethod.mconnection.ExampleUtilsH2.PASSWORD;
import static org.jmethod.mconnection.ExampleUtilsH2.SEQUENCES;
import static org.jmethod.mconnection.ExampleUtilsH2.URL;
import static org.jmethod.mconnection.ExampleUtilsH2.testCreateRows;
//import static org.jmethod.mconnection.ExampleUtilsH2.testDeleteH2Db;
//import static org.jmethod.mconnection.ExampleUtilsH2.testCreateTable;
import static org.jmethod.mconnection.ExampleUtilsH2.testDeleteRows;
import static org.jmethod.mconnection.ExampleUtilsH2.testReadRows;
import static org.jmethod.mconnection.ExampleUtilsH2.testUpdateRows;
//import static org.jmethod.mconnection.ExampleUtilsH2.testCreateTable;
import static org.jmethod.mconnection.MConnection.LIMIT;
import static org.jmethod.mconnection.MConnection.createDataSource;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DataSourceExampleH2 {

    //    private static MConnection testDsCreateMConnection(){
    //        return MConnection.createMConnection(
    //            createDataSource(DATA_SOURCE, URL),
    //            LOGIN,
    //            PASSWORD,
    //            LIMIT,
    //            ID_NAMES,
    //            SEQUENCES
    //        );
    //    }

    private static MConnection testCreateMc() {
        MConnection mc = MConnection.createMConnection(
            ExampleUtilsH2.DRIVER,
            ExampleUtilsH2.URL,
            ExampleUtilsH2.LOGIN,
            ExampleUtilsH2.PASSWORD,
            LIMIT,
            ExampleUtilsH2.ID_NAMES,
            ExampleUtilsH2.SEQUENCES
        );
        return mc;
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

    //    private static boolean testDeleteH2Db() {
    //        //    02.06.2020  19:43               400 h2db.3.log.db
    //        //    02.06.2020  19:41           131я120 h2db.data.db
    //        //    02.06.2020  19:41                96 h2db.index.db
    //        //    02.06.2020  19:43               103 h2db.lock.db
    //        //    02.06.2020  19:42            21я933 h2db.trace.db
    //
    //        //    new File("h2db.3.log.db").delete();
    //        //    new File("h2db.data.db").delete();
    //        //    new File("h2db.index.db").delete();
    //        //    new File("h2db.lock.db").delete();
    //        //    new File("h2db.trace.db").delete();
    //        return true;
    //    }

    public static void testCreateTable(MConnection mc) {
        String sqlScript1 = "CREATE SCHEMA AUDC AUTHORIZATION sa;";
        boolean ok = mc.executeSqlScript(sqlScript1);
        Utils.outln(ok ? "sqlScript1=" + sqlScript1 : "");
        Utils.outln("--------------------------------------------------------------------------------");

        String sqlScript2 =
            "CREATE TABLE AUDC.AUDC_PARAM (\r\n" +
            "  ID             BIGINT PRIMARY KEY,\r\n" +
            "  CURATOR_ACTION VARCHAR(20),\r\n" +
            "  DEFAULT_VALUE  VARCHAR(100),\r\n" +
            "  PARAM_DESCR    VARCHAR(500),\r\n" +
            "  PARAM_NAME     VARCHAR(50),\r\n" +
            "  PARAM_TYPE     VARCHAR(10)\r\n" +
            ")";
        ok = mc.executeSqlScript(sqlScript2);
        Utils.outln(ok ? "sqlScript2=" + sqlScript2 : "");
        Utils.outln("--------------------------------------------------------------------------------");

        String sqlScript3 = "CREATE INDEX IDX__AUDC_PARAM__PARAM_NAME ON AUDC.AUDC_PARAM (PARAM_NAME)";
        ok = mc.executeSqlScript(sqlScript3);
        Utils.outln(ok ? "sqlScript3=" + sqlScript3 : "");
        Utils.outln("--------------------------------------------------------------------------------");
    }

    private static void testDs() {
        //    boolean ok = testDeleteH2Db();
        //    Utils.outln("!!!!!!!!!!!!!!!!!!! testDeleteH2Db: ok=" + ok);
        //    Utils.outln("--------------------------------------------------------------------------------");

        MConnection mc = testCreateMc();
        if (!mc.isConnectionActivate()) {
            Utils.outln("Can't create MConnection mc=" + mc);
            Utils.outln("mc.getConnection()=" + mc.getConnection());
            Utils.outln("--------------------------------------------------------------------------------");

            //    ok = testDeleteH2Db();
            //    Utils.outln("?????????????????? testDeleteH2Db: ok=" + ok);
            return;
        }

        Utils.outln("mc=" + mc);
        Utils.outln("mc.getConnection()=" + mc.getConnection());
        Utils.outln("--------------------------------------------------------------------------------");

        testCreateTable(mc);
        Utils.outln("testCreateTable");
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> delAllList = deleteAll(mc);
        Utils.outln("delAllList=" + delAllList);
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

        List<DbData> findList = testFindRows(mc, "SELECT * FROM AUDC.AUDC_PARAM ORDER BY ID", false);
        Utils.outln("findList=" + findList);
        Utils.outln("--------------------------------------------------------------------------------");

        List<DbData> findList2 = testFindRows(mc,
            "SELECT * FROM AUDC.AUDC_PARAM " +
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
                "FROM AUDC.AUDC_PARAM " +
                "WHERE "+
                    "PARAM_NAME LIKE '%_@' "+
                "ORDER BY ID",
                false
        );
        Utils.outln("findList3=" + findList3);
        Utils.outln("--------------------------------------------------------------------------------");

        delAllList = deleteAll(mc);
        Utils.outln("delAllList=" + delAllList);
        Utils.outln("--------------------------------------------------------------------------------");

        try {
            mc.getConnection().close();
            Utils.outln("mc.getConnection().close(): mc.getConnection()=" + mc.getConnection());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //ok = testDeleteH2Db();
        //Utils.outln(">>>>>>>>>>>>>>>>>> testDeleteH2Db: ok=" + ok);
    }

    private static List<DbData> deleteAll(MConnection mc) {
        FindData fd = mc.find("SELECT ID FROM AUDC.AUDC_PARAM ORDER BY ID", false);

        List<DbData> listResult = new ArrayList<>(3);
        for (DbData dbData : fd.getDbDatas()) {
            if (dbData.getDone()) {
                DbData dbdResult = mc.delete("AUDC.AUDC_PARAM", dbData.getObject(0), true);
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

    public static void main(String[] args) {
        long t0 = System.currentTimeMillis();
        testDs();
        long t1 = System.currentTimeMillis();

        Utils.outln("dt=" + (t1 - t0));
    }
}
