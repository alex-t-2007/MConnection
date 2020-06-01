package org.jmethod.mconnection;

import static org.jmethod.mconnection.MConnection.DEFAULT;
import static org.jmethod.mconnection.MConnection.ID;
import static org.jmethod.mconnection.MConnection.LIMIT;

import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataSourceExample {

    public static DataSource makeDataSource(
        String user, String password, String databaseName, String serverName, int port
    ) {
        PGSimpleDataSource ods = new PGSimpleDataSource();
        ods.setServerNames(new String[]{serverName});
        ods.setPortNumbers(new int[]{port});
        ods.setDatabaseName(databaseName);
        ods.setUser(user);
        ods.setPassword(password);

        return ods;
    }

    public static DataSource makeDataSource(String url, String user, String password) {
        PGSimpleDataSource ods = new PGSimpleDataSource();
        ods.setURL(url);
        ods.setUser(user);
        ods.setPassword(password);
        return ods;
    }

    //    private static InitialContext makeContext(String dataSourceName, DataSource dataSource) {
    //        Hashtable env = new Hashtable();
    //        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
    //        env.put(Context.PROVIDER_URL, "file:JNDI");
    //        try {
    //            InitialContext context = new InitialContext(env);
    //            context.rebind(dataSourceName, dataSource);
    //            return context;
    //        } catch(NamingException ne) {
    //            ne.printStackTrace();
    //            return null;
    //        }
    //    }
    //
    //    private static DataSource getDataSource(InitialContext context, String dataSourceName) {
    //        try {
    //            return (DataSource) context.lookup(dataSourceName);
    //        } catch(NamingException ne) {
    //            ne.printStackTrace();
    //            return null;
    //        }
    //    }

    public static MConnection testDsCreateMConnection(){
        //    DataSource dataSource = makeDataSource(
        //            "audc", "audc_psw", "ib", "localhost",5432
        //    );
        DataSource dataSource = makeDataSource(
                "jdbc:postgresql://localhost:5432/ib", "audc", "audc_psw"
        );
        //    InitialContext context = makeContext("jdbc/DataBase", dataSource);
        //    dataSource = getDataSource(context, "jdbc/DataBase");

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

        return MConnection.createMConnection(
            dataSource,
            LIMIT,
            idNames,
            sequences
        );
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

    private static void testDS() {
        MConnection mc = testDsCreateMConnection();
        if (mc.getDataSource() == null) {
            Utils.outln("Can't create DataSource connection mc=" + mc);
            Utils.outln("mc.getDataSource()=" + mc.getDataSource());
            Utils.outln("--------------------------------------------------------------------------------");
            return;
        }

        Utils.outln("mc=" + mc);
        Utils.outln("mc.getDataSource()=" + mc.getDataSource());
        Utils.outln("--------------------------------------------------------------------------------");

        String sql =
            "SELECT " +
                "AUDC_TASK_PARAM.PARAM_VALUE, AUDC_TASK.*, AUDC_PARAM.* " +
            "FROM AUDC_TASK_PARAM " +
            "LEFT JOIN AUDC_TASK  ON AUDC_TASK.ID  = AUDC_TASK_PARAM.TASK_ID " +
            "LEFT JOIN AUDC_PARAM ON AUDC_PARAM.ID = AUDC_TASK_PARAM.PARAM_ID";

        boolean actDs = mc.activateDSConnection();
        Utils.outln("actDs=" + actDs);
        List<DbData> findList5 = testFindRows(mc, sql, true);
        mc.deactivateDSConnection();

        Utils.outln("findList5=" + findList5);
        Utils.outln("--------------------------------------------------------------------------------");
    }

    public static void main(String[] args) {
        testDS();
    }
}
