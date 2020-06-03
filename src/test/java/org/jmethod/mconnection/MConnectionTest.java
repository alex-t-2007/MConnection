package org.jmethod.mconnection;

import static org.jmethod.mconnection.ExampleUtilsH2.deleteAll;
import static org.jmethod.mconnection.ExampleUtilsH2.testCreateRows;
import static org.jmethod.mconnection.ExampleUtilsH2.testCreateTables;
import static org.jmethod.mconnection.ExampleUtilsH2.testDropTables;
import static org.jmethod.mconnection.ExampleUtilsH2.testReadRows;
import static org.jmethod.mconnection.ExampleUtilsH2.testUpdateRows;
import static org.jmethod.mconnection.MConnection.LIMIT;
import static org.jmethod.mconnection.MConnection.createDataSource;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;

public class MConnectionTest {

    @Test
    public void mainTest() {
        long t0 = System.currentTimeMillis();

        // Data source mode:
        boolean act1 = true;
        test(ModeType.DATA_SOURCE, act1);
        long t1 = System.currentTimeMillis();

        boolean act2 = false;
        test(ModeType.DATA_SOURCE, act2);
        long t2 = System.currentTimeMillis();

        boolean act3 = true;
        test(ModeType.DATA_SOURCE, act3);
        long t3 = System.currentTimeMillis();

        boolean act4 = false;
        test(ModeType.DATA_SOURCE, act4);
        long t4 = System.currentTimeMillis();

        boolean act5 = true;
        test(ModeType.DATA_SOURCE, act5);
        long t5 = System.currentTimeMillis();

        boolean act6 = false;
        test(ModeType.DATA_SOURCE, act6);
        long t6 = System.currentTimeMillis();

        // Driver mode:
        test(ModeType.DRIVER, false);
        long t7 = System.currentTimeMillis();

        test(ModeType.DRIVER, false);
        long t8 = System.currentTimeMillis();

        test(ModeType.DRIVER, false);
        long t9 = System.currentTimeMillis();

        Utils.outln("--------------------------------------------------------------------------------");

        Utils.outln("");
        Utils.outln("## Data source mode: ##########");
        Utils.outln("1. act=" + act1 + " time (ms)=" + (t1 - t0));
        Utils.outln("2. act=" + act2 + " time (ms)=" + (t2 - t1));
        Utils.outln("3. act=" + act3 + " time (ms)=" + (t3 - t2));
        Utils.outln("4. act=" + act4 + " time (ms)=" + (t4 - t3));
        Utils.outln("5. act=" + act5 + " time (ms)=" + (t5 - t4));
        Utils.outln("6. act=" + act6 + " time (ms)=" + (t6 - t5));
        Utils.outln("");
        Utils.outln("## Diriver mode: ##############");
        Utils.outln("7. time (ms)=" + (t7 - t6));
        Utils.outln("8. time (ms)=" + (t8 - t7));
        Utils.outln("9. time (ms)=" + (t9 - t8));
    }

    private static MConnection testDSCreateMConnection(){
        return MConnection.createMConnection(
            createDataSource(ExampleUtilsH2.DATA_SOURCE, ExampleUtilsH2.URL),
            ExampleUtilsH2.LOGIN,
            ExampleUtilsH2.PASSWORD,
            LIMIT,
            ExampleUtilsH2.ID_NAMES,
            ExampleUtilsH2.SEQUENCES
        );
    }

    private static MConnection testDriverCreateMConnection() {
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

    private static enum ModeType {
        DRIVER,
        DATA_SOURCE
    }

    private static void test(ModeType modeType, boolean actType) {
        MConnection mc;
        boolean actived = false;

        if (modeType == ModeType.DRIVER) {
            mc = testDriverCreateMConnection();
            if (!mc.isConnectionActivate()) {
                Utils.outln("Can't create MConnection mc=" + mc);
                Utils.outln("mc.getConnection()=" + mc.getConnection());
                Utils.outln("--------------------------------------------------------------------------------");
                return;
            }
        } else {
            mc = testDSCreateMConnection();
            if (mc.getDataSource() == null) {
                Utils.outln("Can't create DataSource connection mc=" + mc);
                Utils.outln("mc.getDataSource()=" + mc.getDataSource());
                Utils.outln("--------------------------------------------------------------------------------");
                return;
            }
            if (actType) {
                actived = mc.activateDSConnection();
            }
        }

        Utils.outln("modeType=" + modeType);
        Utils.outln("actType=" + actType);
        Utils.outln("mc=" + mc);
        Utils.outln("mc.getDataSource()=" + mc.getDataSource());
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

        testDropTables(mc);
        Utils.outln("testDropTables");
        Utils.outln("--------------------------------------------------------------------------------");

        if (modeType == ModeType.DRIVER) {
            try {
                mc.getConnection().close();
                Utils.outln("mc.getConnection().close(): mc.getConnection()=" + mc.getConnection());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            if (actived) {
                mc.deactivateDSConnection();
                Utils.outln("mc.deactivateDSConnection(): mc.getConnection()=" + mc.getConnection());
            }
        }
    }

    //    public static void main(String[] args) {
    //        long t0 = System.currentTimeMillis();
    //
    //        // Data source mode:
    //        boolean act1 = true;
    //        test(ModeType.DATA_SOURCE, act1);
    //        long t1 = System.currentTimeMillis();
    //
    //        boolean act2 = false;
    //        test(ModeType.DATA_SOURCE, act2);
    //        long t2 = System.currentTimeMillis();
    //
    //        boolean act3 = true;
    //        test(ModeType.DATA_SOURCE, act3);
    //        long t3 = System.currentTimeMillis();
    //
    //        boolean act4 = false;
    //        test(ModeType.DATA_SOURCE, act4);
    //        long t4 = System.currentTimeMillis();
    //
    //        boolean act5 = true;
    //        test(ModeType.DATA_SOURCE, act5);
    //        long t5 = System.currentTimeMillis();
    //
    //        boolean act6 = false;
    //        test(ModeType.DATA_SOURCE, act6);
    //        long t6 = System.currentTimeMillis();
    //
    //        // Driver mode:
    //        test(ModeType.DRIVER, false);
    //        long t7 = System.currentTimeMillis();
    //
    //        test(ModeType.DRIVER, false);
    //        long t8 = System.currentTimeMillis();
    //
    //        test(ModeType.DRIVER, false);
    //        long t9 = System.currentTimeMillis();
    //
    //        Utils.outln("--------------------------------------------------------------------------------");
    //
    //        Utils.outln("");
    //        Utils.outln("## Data source mode: ##########");
    //        Utils.outln("1. act=" + act1 + " time (ms)=" + (t1 - t0));
    //        Utils.outln("2. act=" + act2 + " time (ms)=" + (t2 - t1));
    //        Utils.outln("3. act=" + act3 + " time (ms)=" + (t3 - t2));
    //        Utils.outln("4. act=" + act4 + " time (ms)=" + (t4 - t3));
    //        Utils.outln("5. act=" + act5 + " time (ms)=" + (t5 - t4));
    //        Utils.outln("6. act=" + act6 + " time (ms)=" + (t6 - t5));
    //        Utils.outln("");
    //        Utils.outln("## Diriver mode: ##############");
    //        Utils.outln("7. time (ms)=" + (t7 - t6));
    //        Utils.outln("8. time (ms)=" + (t8 - t7));
    //        Utils.outln("9. time (ms)=" + (t9 - t8));
    //    }
}
