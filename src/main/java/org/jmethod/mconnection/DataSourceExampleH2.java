package org.jmethod.mconnection;

import static org.jmethod.mconnection.ExampleUtilsH2.deleteAll;
import static org.jmethod.mconnection.ExampleUtilsH2.testCreateRows;
import static org.jmethod.mconnection.ExampleUtilsH2.testCreateTables;
import static org.jmethod.mconnection.ExampleUtilsH2.testReadRows;
import static org.jmethod.mconnection.ExampleUtilsH2.testUpdateRows;
import static org.jmethod.mconnection.MConnection.LIMIT;

import java.sql.SQLException;
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
        Utils.outln("ExampleUtilsH2.URL=" + ExampleUtilsH2.URL);
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

    private static void testDs() {
        MConnection mc = testCreateMc();
        if (!mc.isConnectionActivate()) {
            Utils.outln("Can't create MConnection mc=" + mc);
            Utils.outln("mc.getConnection()=" + mc.getConnection());
            Utils.outln("--------------------------------------------------------------------------------");
            return;
        }

        Utils.outln("mc=" + mc);
        Utils.outln("mc.getConnection()=" + mc.getConnection());
        Utils.outln("--------------------------------------------------------------------------------");

        testCreateTables(mc);
        Utils.outln("testCreateTables");
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
    }

    public static void main(String[] args) {
        long t0 = System.currentTimeMillis();
        testDs();
        long t1 = System.currentTimeMillis();

        Utils.outln("dt=" + (t1 - t0));
    }
}
