package org.jmethod.mconnection;

import static org.jmethod.mconnection.ExampleUtils.DRIVER;
import static org.jmethod.mconnection.ExampleUtils.ID_NAMES;
import static org.jmethod.mconnection.ExampleUtils.LOGIN;
import static org.jmethod.mconnection.ExampleUtils.PASSWORD;
import static org.jmethod.mconnection.ExampleUtils.SEQUENCES;
import static org.jmethod.mconnection.ExampleUtils.URL;
import static org.jmethod.mconnection.ExampleUtils.testCreateRows;
import static org.jmethod.mconnection.ExampleUtils.testDeleteRows;
import static org.jmethod.mconnection.ExampleUtils.testFindRows;
import static org.jmethod.mconnection.ExampleUtils.testReadRows;
import static org.jmethod.mconnection.ExampleUtils.testUpdateRows;
import static org.jmethod.mconnection.MConnection.LIMIT;

import java.util.ArrayList;
import java.util.List;

public class DriverConnectionExample {

    private static MConnection testCreateMc() {
        MConnection mc = MConnection.createMConnection(
            DRIVER,
            URL,
            LOGIN,
            PASSWORD,
            LIMIT,
            ID_NAMES,
            SEQUENCES
        );
        return mc;
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
