package org.jmethod.mconnection;

import static java.lang.Class.forName;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The MConnection package is a set of Java utility classes for easing JDBC development.
 *
 * @author Alex L. Tokarev
 */

public class MConnection {
    public static final String ID = "ID";
    public static final String DEFAULT = "{DEFAULT}";

    public static final String LIMIT = "LIMIT";
    public static final String FETCH_FIRST = "FETCH FIRST";
    public static final String ROWS = "ROWS";
    public static final String ROWNUM = "ROWNUM";
    public static final String SELECT_FIRST = "SELECT FIRST";

    private static final String TABLES = "TABLES";
    private static final String DELIM = ",";
    //private static final String ENV_CONTEXT_NAME = "java:comp/env";

    private static final String IS_SEE__PREFIX = "java.sql.SQLSyntaxErrorException see=";
    private static final String IS_REL_NOT_EXIST__PREFIX = "relation not exist e=";

    private static final String SET_URL = "setURL";

    private Connection connection;
    private String connectionError = null;

    // For DataSource MODE
    //private static Context context = null;
    private DataSource dataSource;

    // For Driver MODE
    public String driver;
    public String url;
    public String login;
    public String password;

    // ID name config
    private Map<String, String> idNames = null;
    // SEQUENCES config
    private Map<String, String> sequences = null;
    // LIMIT TYPE config
    public String limitTyp = LIMIT;

    private String CRSF = null;

    // TODO Hashtable
    private Hashtable metaDataTable_Cash = new Hashtable();

    protected static class ActivateResult {
        public boolean activated = false;
        public boolean done = true;
    }

    /**
     * Создание объекта 'MConnection' в режиме 'Драйверный'.
     * При этом создается соединение с БД ('connection'), котрое может существовать все
     * время пока живет приложение.
     * Данный (драйверный) режим используется, как правило, для 'десктопных' приложений.
     *
     * @param driver ("org.postgresql.Driver")
     * @param url ("jdbc:postgresql://127.0.0.1:5432/ib")
     * @param login ("testLogin")
     * @param password ("testPassword")
     * @param limitTyp (LIMIT | FETCH_FIRST | ROWS | ROWNUM | SELECT_FIRST)
     * @param idNames Names of relation id field - Map<String, String>,
     *               key - relation name or {DEFAULT}, value - sequences name. Default: "ID".
     *     Examples:
     *         Map<String, String> idNames = new HashMap<>();
     *         idNames.put(DEFAULT, ID);
     *         idNames.put("AUDC_PCLIB_BUSINESS_OBJECT", "SYSNAME");
     *         idNames.put("AUDC_PCLIB_BUSINESS_OPERATIONS", "SYSNAME");
     *         idNames.put("AUDC_PCLIB_BUSINESS_SERVICE", "SYSNAME");
     *         idNames.put("AUDC_TASK_PARAM", "SYSNAME");
     *
     *
     * @param sequences - Map<String, String>, key - relation name, value - sequences name
     *     Examples:
     *         Map<String, String> sequences = new HashMap<>();
     *         sequences.put("AUDC_ACLIB_ACTOR_ACCOUNT", "audc_aclib_actor_account_id_seq");
     *         sequences.put("AUDC_ACLIB_CLIENT_IP_RANGE", "audc_aclib_client_ip_range_id_seq");
     *         sequences.put("AUDC_ACLIB_ROLE_WORKPLACE", "audc_aclib_role_workplace_id_seq");
     *         sequences.put("AUDC_PARAM", "audc_param_seq");
     *         sequences.put("AUDC_TASK", "audc_task_seq");
     *         sequences.put("AUDC_USER", "audc_user_seq");
     *
     * @return MConnectio object
     */
    public static MConnection createMConnection(
        String driver,
        String url,
        String login,
        String password,
        String limitTyp,
        Map<String, String> idNames,
        Map<String, String> sequences
    ){
        MConnection mc = new MConnection();
        mc.driver = driver;
        mc.url = url;
        mc.login = login;
        mc.password = password;
        mc.limitTyp = limitTyp;
        mc.idNames = idNames;
        mc.sequences = sequences;
        ConnectionReply connectionReply = createConnection(driver, url, login, password);
        mc.connection = connectionReply.connection;
        mc.connectionError = connectionReply.error;
        return mc;
    }

    /**
     * Создание объекта 'MConnection' в режиме 'Datasource'.
     * При этом соединение с БД ('connection') не создается, а берется по мере надобности из пула соединений
     * в 'Datasource' и возвращается в пул после использования.
     * Данный режим (Datasource) используется, как правило, для web-приложений.
     * @param dataSource Datasource
     * @param login ("testLogin")
     * @param password ("testPassword")
     * @param limitTyp (LIMIT | FETCH_FIRST | ROWS | ROWNUM | SELECT_FIRST)
     * @param idNames Names of relation id field - Map<String, String>,
     *               key - relation name or {DEFAULT}, value - sequences name. Default: "ID".
     *     Examples:
     *         Map<String, String> idNames = new HashMap<>();
     *         idNames.put(DEFAULT, ID);
     *         idNames.put("AUDC_PCLIB_BUSINESS_OBJECT", "SYSNAME");
     *         idNames.put("AUDC_PCLIB_BUSINESS_OPERATIONS", "SYSNAME");
     *         idNames.put("AUDC_PCLIB_BUSINESS_SERVICE", "SYSNAME");
     *         idNames.put("AUDC_TASK_PARAM", "SYSNAME");
     * @param sequences - Map<String, String>, key - relation name, value - sequences name
     *     Examples:
     *         Map<String, String> sequences = new HashMap<>();
     *         sequences.put("AUDC_ACLIB_ACTOR_ACCOUNT", "audc_aclib_actor_account_id_seq");
     *         sequences.put("AUDC_ACLIB_CLIENT_IP_RANGE", "audc_aclib_client_ip_range_id_seq");
     *         sequences.put("AUDC_ACLIB_ROLE_WORKPLACE", "audc_aclib_role_workplace_id_seq");
     *         sequences.put("AUDC_PARAM", "audc_param_seq");
     *         sequences.put("AUDC_TASK", "audc_task_seq");
     *         sequences.put("AUDC_USER", "audc_user_seq");
     *
     * @return MConnectio object
     */
    public static MConnection createMConnection(
        DataSource dataSource,
        String login,
        String password,
        String limitTyp,
        Map<String, String> idNames,
        Map<String, String> sequences
    ){
        MConnection mc = new MConnection();
        mc.setDataSource(dataSource);
        mc.login = login;
        mc.password = password;
        mc.limitTyp = limitTyp;
        mc.idNames = idNames;
        mc.sequences = sequences;
        return mc;
    }

    //    public static DataSource getDataSource(String datasourceName){
    //        Context envContext = getContext();
    //        if (envContext == null){
    //            return null;
    //        }
    //        try {
    //            return ((DataSource) envContext.lookup(datasourceName));
    //        } catch(Exception e){
    //            e.printStackTrace();
    //            return null;
    //        }
    //    }
    //
    //    private static Context getContext(){
    //        if (context == null){
    //            try {
    //                context = (Context)((new InitialContext()).lookup(ENV_CONTEXT_NAME));
    //            } catch( Exception e ){
    //                context = null;
    //                e.printStackTrace();
    //            }
    //        }
    //        return context;
    //    }

    public static DataSource createDataSource(String dataSourceClassName, String url) {
        DataSource dataSource = createDataSourceObject(dataSourceClassName, url);
        if (dataSource == null) {
            return null;
        }

        if (!setUrl(dataSource, url)) {
            return null;
        }
        return dataSource;
    }

    private static DataSource createDataSourceObject(String dataSourceClassName, String url) {
        try {
            Class clazz = forName(dataSourceClassName);
            Constructor constructor = clazz.getConstructor();
            Object object = constructor.newInstance();
            return (DataSource) object;
        } catch(ClassNotFoundException cnf){
            cnf.printStackTrace();
            return null;
        } catch(NoSuchMethodException e1) {
            e1.printStackTrace();
            return null;
        } catch(SecurityException e2) {
            e2.printStackTrace();
            return null;
        } catch(InstantiationException e3) {
            e3.printStackTrace();
            return null;
        } catch(IllegalAccessException e4) {
            e4.printStackTrace();
            return null;
        } catch(IllegalArgumentException e5) {
            e5.printStackTrace();
            return null;
        } catch(InvocationTargetException e6) {
            e6.printStackTrace();
            return null;
        }
    }

    private static boolean setUrl(DataSource dataSource, String url) {
        // установка атрибута 'url' для объекта 'dataSource'.

        if (dataSource == null || url == null || url.isEmpty()) {
            return false;
        }

        // поиск метода: 'setURL'
        Method setUrlMethod = null;
        Method[] mets = dataSource.getClass().getMethods();
        for (int i = 0; mets != null && i < mets.length; i++) {
            if (SET_URL.equals(mets[i].getName())) {
                setUrlMethod = mets[i];
                break;
            }
        }
        if (setUrlMethod == null) {
            Utils.outln("Method " + SET_URL + " not found in class:'" + dataSource.getClass() + "'");
            return false;
        }

        // вызов метода: 'setURL'
        try {
            setUrlMethod.invoke(dataSource, url);
            return true;
        } catch(IllegalAccessException e1) {
            e1.printStackTrace();
            return false;
        } catch(IllegalArgumentException e2) {
            e2.printStackTrace();
            return false;
        } catch(InvocationTargetException e3) {
            e3.printStackTrace();
            return false;
        }
    }

    /**
     * Соединение с БД существует (применяется в основном для Драйверного режима).
     *
     * @return признак существующего соединения.
     */
    public boolean isConnectionActivate() {
        return this.connection != null;
    }

    public boolean closeConnection() {
        try {
            this.connection.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // Методы CRUD:
    // CREATE
    public DbData create(DbData dbData, boolean commit) {
        // создает строку в таблицы, значения полей в 'dbData'

        if (dbData == null) {
            return new DbData();
        }
        if (!dbData.isCorrect()) {
            dbData.setDone(false);
            return dbData;
        }

        // Если режим 'DataSource', то берет из пула соединение, если это не сделано вышы (т.е. если соединение == null)
        ActivateResult activateResult = actDSCon(commit, "create", "The row will not be create");
        if (!activateResult.done){
            dbData.setDone(false);
            return dbData;
        }

        try {
            Object id = insertRow(dbData);
            dbData.setDone(id != null);
            dbData.setId(id);

            if (commit){
                if (dbData.getDone()){
                    this.commit();
                } else {
                    this.rollback();
                }
            }
        } finally {
            // Если соединение бралось из DataSource, то оно возвращается
            if (activateResult.activated){
                this.deactivateDSConnection();
            }
        }
        return dbData;
    }

    // READ
    public DbData read(String tableName, Object id, String fieldNames) {
        return read(tableName, id, splitFieldNames(fieldNames));
    }

    // READ
    public DbData read(String tableName, Object id, String[] fieldNames) {
        // читает поля строки таблицы

        if (fieldNames == null || fieldNames.length == 0) {
            return new DbData(tableName, id);
        }

        // Если режим 'DataSource', то берет из пула соединение, если это не сделано вышы (т.е. если соединение == null)
        ActivateResult activateResult = actDSCon(true, "read", "The row will not be read");
        if (!activateResult.done){
            return new DbData(tableName, id);
        }

        try {
            if (fieldNames.length == 1 && "*".equals(fieldNames[0])) {
                fieldNames = getMetaDataTable(tableName);
                if (fieldNames == null || fieldNames.length == 0) {
                    return new DbData(tableName, id);
                }
            }
            return readRow(tableName, id, fieldNames);
        } finally {
            // Если соединение бралось из DataSource, то оно возвращается
            if (activateResult.activated){
                this.deactivateDSConnection();
            }
        }
    }

    // UPDATE
    public DbData update(DbData dbData, boolean commit) {
        // модифицирует поля в строке таблицы, значения полей в 'dbData'

        if (dbData == null) {
            return new DbData();
        }
        if (!dbData.isCorrect() || dbData.getId() == null) {
            dbData.setDone(false);
            return dbData;
        }

        // Если режим 'DataSource', то берет из пула соединение, если это не сделано вышы (т.е. если соединение == null)
        ActivateResult activateResult = actDSCon(commit, "update", "The row will not be update");
        if (!activateResult.done){
            dbData.setDone(false);
            return dbData;
        }

        try {
            dbData.setDone(updateRow(dbData));

            if (commit){
                if (dbData.getDone()){
                    this.commit();
                } else {
                    this.rollback();
                }
            }
        } finally {
            // Если соединение бралось из DataSource, то оно возвращается
            if (activateResult.activated){
                this.deactivateDSConnection();
            }
        }
        return dbData;
    }

    // DELETE
    public DbData delete(String tableName, Object id, boolean commit) {
        // удаляет строку из таблицы <tableName>, id=<id>

        DbData dbData = new DbData(tableName, id);
        if (id == null) {
            return dbData;
        }

        // Если режим 'DataSource', то берет из пула соединение, если это не сделано вышы (т.е. если соединение == null)
        ActivateResult activateResult = actDSCon(commit, "delete", "The row will not be delete");
        if (!activateResult.done){
            dbData.setDone(false);
            return dbData;
        }

        try {
            id = deleteRow(tableName, id);
            dbData.setDone(id != null);
            dbData.setId(id);

            if (commit){
                if (dbData.getDone()){
                    this.commit();
                } else {
                    this.rollback();
                }
            }
        } finally {
            // Если соединение бралось из DataSource, то оно возвращается
            if (activateResult.activated){
                this.deactivateDSConnection();
            }
        }
        return dbData;
    }

    // Методы FIND
    public FindData find(String sql, List<Object> params, boolean resultTableNameFlag){
        FindData fd = findPsRows(sql, params, resultTableNameFlag);
        return fd;
    }
    public FindData find(String sql, boolean resultTableNameFlag){
        return find(sql, new ArrayList<>(), resultTableNameFlag);
    }

    public FindData find1(String sql, List<Object> params, boolean resultTableNameFlag) {
        return find(setLimit(sql), params, resultTableNameFlag);
    }
    public FindData find1(String sql, boolean resultTableNameFlag) {
        return find1(sql, new ArrayList<>(), resultTableNameFlag);
    }

    public String getIdName(String tableName) {
        if (idNames == null) {
            return null;
        }

        String defaultIdName = getDefaultIdName();
        if (tableName == null || tableName.isEmpty()) {
            return defaultIdName;
        }

        String idn = idNames.get(tableName.toUpperCase());
        if (idn == null) {
            return defaultIdName;
        }
        return idn.toUpperCase();
    }

    public String getDefaultIdName() {
        if (idNames == null) {
            return null;
        }
        String idn = idNames.get(DEFAULT);
        if (idn == null) {
            return null;
        }
        return idn.toUpperCase();
    }

    public Connection getConnection() {
        return connection;
    }
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public String getConnectionError() {
        return connectionError;
    }
    //    public void setConnectionError(String connectionError) {
    //        this.connectionError = connectionError;
    //    }

    // DataSource
    public DataSource getDataSource() {
        return dataSource;
    }
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * В режиме 'Datasource' получение соединения с БД ('connection') из пула соединений в 'Datasource',
     * которое после использования должно быть возвращено в пул методом: 'deactivateDSConnection'.
     * Если соединение уже выделено раньше (connection != null), то соединение не выделяется из пула,
     * а предполагается, что будет использоваться уже выделенное соединение. Это происходит автоматически.
     *
     * @param autoCommit - Auto commit flag
     * @return признак, что соединение было выделено.
     */
    public boolean activateDSConnection(boolean autoCommit){
        if ( this.dataSource == null ){
            return false;
        }
        if ( this.connection != null ){
            //    if ( act_pas_warning_flag ){
            //        Utils.outln( "?????????? act_DS_Con: (this.getCon() != null) this.getCon()="+this.getCon() );
            //        try {
            //            int x = 1 / 0;
            //        } catch ( Exception e ){
            //            e.printStackTrace();
            //        } // try
            //    } // if
            return false;
        }

        try {
            this.connection = this.dataSource.getConnection(this.login, this.password);
            //    if ( act_pas_log_flag ){
            //        Utils.outln( ">>>>>>>>>> act_DS_Con: this.getCon()="+this.getCon() );
            //    } // if
            this.connection.setAutoCommit(autoCommit);
            return true;
        } catch(Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * В режиме 'Datasource' получение соединения с БД ('connection') из пула соединений в 'Datasource',
     * которое после использования должно быть возвращено в пул методом: 'deactivateDSConnection'.
     * Если соединение уже выделено раньше (connection != null), то соединение не выделяется из пула,
     * а предполагается, что будет использоваться уже выделенное соединение. Это происходит автоматически.
     * Атрибут 'autoCommit' равен 'false'.
     *
     * @return признак, что соединение было выделено.
     */
    public boolean activateDSConnection(){
        return activateDSConnection( false );
    }

    /**
     * В режиме 'Datasource' возврат (после использования) соединения с БД ('connection') в пул соединений.
     */
    public boolean deactivateDSConnection(){
        String conStr = ""+ connection;
        boolean ok = closeDSConnection(connection);
        //    if ( act_pas_log_flag ){
        //        Utils.outln( "<<<<<<<<<< pas_DS_Con(): closeDataSourceCon() this.getCon()="+conStr );
        //    } // if
        this.connection = null;
        return ok;
    }

    /**
     * Если режим равен 'DataSource', иначе - 'Драйверный'.
     * @return 'DataSource', иначе - 'Драйверный'
     */
    public boolean isDataSourceMode(){
        return this.connection == null && this.dataSource != null;
    }

    /**
     * В режиме 'Datasource' получение соединения с БД ('connection') из пула соединений в 'Datasource',
     * которое после использования должно быть возвращено в пул методом: 'deactivateDSConnection'.
     * Если соединение уже выделено раньше (connection != null), то соединение не выделяется из пула,
     * а предполагается, что будет использоваться уже выделенное соединение.
     *
     * @param commit - commit flag.
     * @param procName - process Name (for log).
     * @param funcName - function Name (for log).
     *
     * @return признак, что соединение было выделено.
     */
    public ActivateResult actDSCon(boolean commit, String procName, String funcName) {
        ActivateResult activateResult = new ActivateResult();

        if (!isDataSourceMode()) {
            // Режим работы с драйвером 'JDBC' (НЕ с 'DataSource')
            return activateResult;
        }

        // Режим работы с 'DataSource'
        //    if ( act_pas_log_flag ){
        //        Utils.outln( "!!!!!!!!!! isDataSourceMode "+procName+"(..." );
        //    } // if
        if (!commit){
            Utils.outln(
                    "???????????????????????????????????????????????????????????????????????????????"
            );
            Utils.outln( "MConnection."+procName+": It Is Nonsense: (isDataSourceMode() && ! commit)");
            Utils.outln( "   "+funcName );
            Utils.outln(
                    "???????????????????????????????????????????????????????????????????????????????"
            );
        }

        // Взять соединение из DataSource
        activateResult.activated = this.activateDSConnection();
        if (activateResult.activated) {
            return activateResult;
        }

        // не могу взять соединение из DataSource: Выход
        Utils.outln(
                "???????????????????????????????????????????????????????????????????????????????"
        );
        Utils.outln( "MConnection."+procName+": Can't get Connection from DataSource");
        Utils.outln( "   "+funcName );
        Utils.outln(
                "???????????????????????????????????????????????????????????????????????????????"
        );
        activateResult.done = false;

        return activateResult;
    }
    //^^DataSource

    public boolean commit() {
        try {
            this.connection.commit();
            //    if ( this.commitTraceFlag ){
            //        Utils.outln(
            //                "CC MConnection.commit() CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
            //        );
            //    } // if
            return true;
        } catch( Exception ex ) {
            //    Utils.outln( "MConnection.commit: ex=" + ex );
            ex.printStackTrace();
            return false;
        } // try
    } // commit

    public boolean rollback() {
        try {
            this.connection.rollback();
            //    if ( this.rollbackTraceFlag ){
            //        Utils.outln(
            //                "RR MConnection.rollback() RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR"
            //        );
            //    } // if
            return true;
        } catch( Exception ex ) {
            //    Utils.outln( "MConnection.rollback: ex=" + ex );
            ex.printStackTrace();
            return false;
        } // try
    } // rollback

    /**
     * Добавить оператор ограничения количества искомых строк, если его нет.
     *
     * @param sql
     * @param quant
     * @return
     */
    public String setLimit(String sql, long quant) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        if (LIMIT.equals(this.limitTyp)) {
            if (!sql.contains(" LIMIT ")) {
                return sql + " LIMIT " + quant + " ";
            }
        } else if (FETCH_FIRST.equals(this.limitTyp)) {
            if (!sql.contains(" FETCH FIRST ")){
                return sql + " FETCH FIRST " + quant + " ROWS ONLY ";
            }
        } else if (ROWS.equals(this.limitTyp)) {
            if (!sql.contains(" ROWS ")){
                return sql + " ROWS " + quant + " ";
            }
        } else if (ROWNUM.equals(this.limitTyp)) {
            if (!sql.contains(" ROWNUM <= ")){
                return sql + " ROWNUM <= " + quant + " ";
            }
        } else if (SELECT_FIRST.equals(this.limitTyp)) {
            if (!sql.contains("SELECT FIRST ")) {
                return sql.replace("SELECT FIRST " + quant + " ", "SELECT ");
            }
        }
        return sql;
    }

    /**
     * Добавить оператор ограничения количества искомых строк, если его нет.
     *
     * @param sql
     * @return
     */
    public String setLimit(String sql) {
        return setLimit(sql, 1L);
    }

    /**
     * Создать ResultSet по тексту sql-запроса. TODO: MResultSet
     *
     * @param sqlString тексту sql-запроса.
     * @return ResultSet
     */
    protected ResultSet createResultSet( String sqlString ){
        warningNotActivatedDSConnection();
        return createResultSet(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                sqlString
        );
    }

    public boolean executeSqlScript(String sqlScript) {
        if (sqlScript == null || sqlScript.isEmpty()) {
            return false;
        }

        // Если режим 'DataSource', то берет из пула соединение, если это не сделано вышы (т.е. если соединение == null)
        ActivateResult activateResult = actDSCon(true, "executeSqlScript",
                "Can't execute sqlScript=\r\n" + sqlScript);
        if (!activateResult.done){
            return false;
        }

        PreparedStatement ps = null;
        try {
            ps = this.getConnection().prepareStatement(sqlScript);
            ps.executeUpdate();
            this.commit();
            return true;
        } catch(SQLException e) {
            this.rollback();
            Utils.outln("?? e=" + e);
            e.printStackTrace();
            return false;
        } finally {
            // Если соединение бралось из DataSource, то оно возвращается
            if (activateResult.activated){
                this.deactivateDSConnection();
            }
            closeRsPs(null, ps);
        }
    }

    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------
    //-------------------------------------------------------------------------

    // CRUD
    private Object insertRow(DbData dbData) {
        // INSERT INTO PRODUCT(NAME,NUM,ODATE) VALUES(?,?,?);
        StringBuilder columns = new StringBuilder();
        StringBuilder quest = new StringBuilder();
        List<String> fieldNames = dbData.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            columns.append(fieldNames.get(i));
            quest.append("?");
            if ( i < fieldNames.size() - 1) {
                columns.append(",");
                quest.append(",");
            }
        }

        PreparedStatement ps = null;
        Object insertedId = null;
        String tableName = dbData.getTableName();
        StringBuilder sql = new StringBuilder();
        try {
            String idn = this.getIdName(tableName);
            int idIndex = fieldNames.indexOf(idn);
            if (idIndex < 0) {
                // В списке полей нет ID
                insertedId = this.genId(tableName, 1);
                if (insertedId == null) {
                    return null;
                }

                if (fieldNames.isEmpty()) {
                    sql.append("INSERT INTO " + tableName + "( " + idn + " ) VALUES( ? )");
                } else {
                    sql.append("INSERT INTO " + tableName + "( " + idn + "," + columns + " ) VALUES( ?," + quest + " )");
                }
                ps = this.connection.prepareStatement(sql.toString());

                // set Data
                int delta = 2;
                ps.setObject( 1, insertedId);
                for ( int i = 0; i < dbData.getValues().size(); i++ ) {
                    Object value = dbData.getValues().get(i);
                    if ( value == null ){
                        ps.setObject( i + delta, value );
                    } else {
                        if ( value.getClass().equals( java.util.Date.class ) ){
                            java.util.Date date = ((java.util.Date) value);
                            ps.setDate(i + delta, new java.sql.Date(date.getTime()));
                        } else {
                            ps.setObject( i + delta, value);
                        }
                    }
                }

                ps.executeUpdate();
                return insertedId;
            } else {
                insertedId = dbData.getValues().get(idIndex);
                if (insertedId == null) {
                    return null;
                }

                sql.append("INSERT INTO " + tableName + "( " + columns + " ) VALUES( " + quest + " )");
                ps = this.connection.prepareStatement(sql.toString());

                // set Data
                int delta = 1;
                for (int i = 0; i < dbData.getValues().size(); i++) {
                    Object value = dbData.getValues().get(i);
                    if ( value == null ){
                        ps.setObject( i + delta, value );
                    } else {
                        if (value.getClass().equals(java.util.Date.class)){
                            java.util.Date date = ((java.util.Date) value);
                            ps.setDate(i + delta, new java.sql.Date(date.getTime()));
                        } else {
                            ps.setObject( i + delta, value);
                        }
                    }
                }

                ps.executeUpdate();
                return insertedId;
            }
        } catch(Exception ex) {
            Utils.outln("MConnection#insertRow: ex = " + ex);
            Utils.outln("  sql=" + sql.toString());
            Utils.outln("  insertedId=" + insertedId);
            Utils.outln("  dbData.getValues()=" + dbData.getValues());
            ex.printStackTrace();
            return null;
        } finally {
            closeRsPs(null, ps);
        }
    }

    private DbData readRow(String tableName, Object id, String[] fieldNames) {
        // читает поля строки таблицы

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //-----------------------------------------------------------------
            // SELECT
            //   ID,
            //   NAME,
            //   NUM,
            //   ODATE
            // FROM PRODUCT
            // WHERE
            //   ID = ?
            //-----------------------------------------------------------------
            // SELECT ID,NAME,NUM,ODATE FROM PRODUCT WHERE ID = ?
            //-----------------------------------------------------------------

            // gen sql
            StringBuilder sql = new StringBuilder().append("SELECT ");
            for (int i = 0; i < fieldNames.length; i++) {
                if (i > 0) {
                    sql.append(",");
                }
                sql.append(fieldNames[i]);
            }
            sql.append(" FROM " + tableName + " WHERE " + this.getIdName(tableName) + " = ?");

            // create PreparedStatement
            ps = this.connection.prepareStatement(
                    sql.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY
            );

            // set value
            if (id.getClass().equals( java.util.Date.class)) {
                Date dateVal = (Date) id;
                ps.setDate(1, new java.sql.Date(dateVal.getTime()));
            } else if (id.getClass().equals(Timestamp.class)) {
                ps.setTimestamp(1, (Timestamp) id);
            } else {
                ps.setObject(1, id);
            }

            // query
            rs = ps.executeQuery();

            // read data
            DbData dbData = new DbData(tableName, id);
            if (!rs.next()){
                return dbData;
            }
            for (int i = 0; i < fieldNames.length; i++) {
                Object val = rs.getObject(i + 1);
                dbData.setObject(fieldNames[i], val);
            }
            dbData.setDone(true);

            return dbData;
        } catch (SQLException e){
            e.printStackTrace();
            return new DbData(tableName, id);
        } finally {
            closeRsPs(rs, ps);
        }
    }

    private boolean updateRow(DbData dbData) {
        // UPDATE PRODUCT SET NAME=?,NUM=?,ODATE=? WHERE ID=?

        // gen sql
        List<String> fieldNames = dbData.getFieldNames();
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE " + dbData.getTableName() + " SET ");
        for (int i = 0; i < fieldNames.size(); i++) {
            if (i > 0) {
                sql.append(",");
            }
            sql.append(fieldNames.get(i) + "=?");
        }
        sql.append(" WHERE " + this.getIdName(dbData.getTableName()) + "=?");

        PreparedStatement ps = null;
        //Object insertedId = null;
        try {
            // create PreparedStatement
            ps = this.connection.prepareStatement(sql.toString());

            // set data
            List<Object> values = dbData.getValues();
            int delta = 1;
            for (int i = 0; i < values.size(); i++) {
                Object value = values.get(i);
                if ( value == null ){
                    ps.setObject( i + delta, value );
                } else if ( value.getClass().equals( java.util.Date.class ) ){
                    java.util.Date date = ((java.util.Date) value);
                    ps.setDate(i + delta, new java.sql.Date(date.getTime()));
                } else {
                    ps.setObject( i + delta, value);
                }
            }
            ps.setObject(values.size() + delta, dbData.getId());

            // query
            ps.executeUpdate();
            return true;
        } catch(Exception ex) {
            Utils.outln("MConnection#updateRowRow: ex = " + ex);
            Utils.outln("  sql=" + sql.toString());
            Utils.outln("  dbData.getValues()=" + dbData.getValues());
            ex.printStackTrace();
            return false;
        } finally {
            closeRsPs(null, ps);
        }
    }

    private Object deleteRow(String tableName, Object id) {
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(
                    "DELETE FROM " + tableName + " WHERE " + this.getIdName(tableName) + " = ?"
            );
            ps.setObject(1, id);
            ps.executeUpdate();
            return id;
        } catch( Exception ex ) {
            Utils.outln("deleteRow: ex = " + ex);
            ex.printStackTrace();
            return null;
        } finally {
            closeRsPs(null, ps);
        }
    }
    //^^CRUD

    // FIND
    private FindData findPsRows(String sql, List<Object> params, boolean resultTableNameFlag){
        FindData fd = new FindData();

        if (sql == null || sql.isEmpty()){
            fd.setExStr(
                "------------------------------------------------------------------------------\r\n"+
                "?? findPsRows: sql == null || sql.isEmpty()\r\n"+
                sql+"\r\n"+
                "params="+params+"\r\n"+
                "------------------------------------------------------------------------------"
            );
            return fd;
        } // if

        // Если режим 'DataSource', то берет из пула соединение, если это не сделано вышы (т.е. если соединение == null)
        ActivateResult activateResult = actDSCon(true, "read", "The row will not be read");
        if (!activateResult.done){
            fd.setExStr(
                "------------------------------------------------------------------------------\r\n"+
                "?? findPsRows: actDSCon\r\n"+
                sql+"\r\n"+
                "params="+params+"\r\n"+
                "------------------------------------------------------------------------------"
            );
            return fd;
        }

        PreparedStatement[] ps = {null};
        ResultSet rs = null;
        try {
            rs = this.createResultSetPs(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY,
                    sql,
                    params,
                    ps
            );
            if ( rs == null ){
                fd.setExStr(
                    "------------------------------------------------------------------------------\r\n"+
                    "?? findPsRows: rs == null\r\n"+
                    sql+"\r\n"+
                    "params="+params+"\r\n"+
                    "------------------------------------------------------------------------------"
                );
                return fd;
            } // if

            // gen result
            List<DbData> dbdList = new ArrayList<>();
            String[] fns = getFieldNamesFromRs(rs, resultTableNameFlag);
            rs.beforeFirst();
            while (rs.next()) {
                DbData dbd = new DbData();
                for ( int i = 0; i < fns.length; i++){
                    // TODO в normRsObject вставить компенсацию возможного искажения времени из-за 'TimeZone'
                    //values[i] = MConnection.normRsObject( rs.getObject( i + 1 ) );
                    Object values = rs.getObject( i + 1);

                    dbd.setObject(fns[i].toUpperCase(), values);
                }
                dbd.setDone(true);
                dbdList.add(dbd);
            }
            fd.setDbDatas(dbdList);

            return fd;
        } catch (Exception ex) {
            ex.printStackTrace();
            fd.setExStr(
                "------------------------------------------------------------------------------\r\n"+
                "?? findPsRows: Exception ex=" + ex +"\r\n"+
                sql+"\r\n"+
                "params="+params+"\r\n"+
                "------------------------------------------------------------------------------"
            );
            return fd;
        } finally {
            closeRsPs(rs, ps[0]);

            // Если соединение бралось из DataSource, то оно возвращается
            if (activateResult.activated){
                this.deactivateDSConnection();
            } // if
        } // try
    }

    // con act !!
    public ResultSet createResultSetPs(
            int resultSetType,
            int resultSetConcurency,
            String sql,
            List<Object> values,
            PreparedStatement[] ps
    ){
        warningNotActivatedDSConnection();

        if (sql == null) {
            return null;
        }

        if (ps == null || ps.length == 0){
            ps = new PreparedStatement[1];
        }
        ps[0] = null;

        try {
            if (resultSetType == -1 || resultSetConcurency == -1) {
                ps[0] = this.connection.prepareStatement(
                        sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            } else {
                ps[0] = this.connection.prepareStatement(sql, resultSetType, resultSetConcurency );
            }

            if (values != null) {
                for (int i = 0; i < values.size(); i++) {
                    int ind = i + 1;
                    Object val = values.get(i);
                    if (val == null) {
                        ps[0].setObject(ind, val);
                    } else {
                        if (val.getClass().equals( java.util.Date.class)) {
                            Date dateVal = (Date) val;
                            ps[0].setDate( ind, new java.sql.Date(dateVal.getTime()));
                        } else if (val.getClass().equals(Timestamp.class)) {
                            ps[0].setTimestamp(ind, (Timestamp) val);
                        } else {
                            ps[0].setObject(ind, val);
                        }
                    }
                }
            }
            return ps[0].executeQuery();
        } catch(SQLException ex) {
            Utils.outln("MConnection#createResultSetPs: ex = " + ex);
            Utils.outln("sql=" + sql);
            ex.printStackTrace();

            // ps[0] закрывается, только если Exception
            closeRsPs(null, ps[0]);
            return null;
        }
    }

    private String[] getFieldNamesFromRs(ResultSet rs, boolean tableNameFlag) {
        try {
            String[] fc = new String[rs.getMetaData().getColumnCount()];
            for ( int i = 0; i < fc.length; i++ ) {
                int ic = i + 1;

                String tn = null;
                if (tableNameFlag) {
                    tn = rs.getMetaData().getTableName(ic);
                }

                String fn = rs.getMetaData().getColumnName(ic);
                if (tn != null && !tn.isEmpty()) {
                    fn = tn + "." + fn;
                }

                fc[i] = fn;
            }
            return fc;
        } catch (SQLException e) {
            return null;
        }
    }
    //^^FIND

    // MetaData
    // con pas
    private String[] getMetaDataTable(String tableName){
        String[] mt = (String[])(metaDataTable_Cash.get(tableName));
        if ( mt == null ){
            //mt = queryMetaDataTable(tableName);
            mt = getMetaDataFields(setLimit("SELECT * FROM " + tableName));
            if ( mt != null ){
                //metaDataTable_Cash.put(mt, tableName);
                metaDataTable_Cash.put(tableName, mt);
            } // if
        } // if

        return mt;
    }

    // con ++
    private String[] getMetaDataFields(String sql){
        if (sql == null){
            return null;
        }

        ResultSet rs = null;
        try {
            rs = createResultSet(sql);
            if ( rs == null ) {
                return null;
            }

            int qCol = rs.getMetaData().getColumnCount();

            String[] fc = new String[qCol];
            for (int i = 0; i < qCol; i++) {
                fc[i] = rs.getMetaData().getColumnName(i + 1);
                if ( fc[i] != null){
                    fc[i] = fc[i].toUpperCase();
                } // if
            }

            return fc;
        } catch (SQLException ex){
            ex.printStackTrace();
            return null;
        } finally {
            this.closeResultSetAndStatement(rs);
        }
    }
    //^^MetaData

    // ResultSet
    private ResultSet createResultSet(Statement[] stmt, String sqlString){
        CRSF = null;
        if ( stmt == null ){
            CRSF = "stmt == null";
            Utils.outln(
                    "??????????????????????????????????????????????????????????????????\r\n"+
                            "MConnection.createResultSet: Ошибка SQL запроса:\r\n" +
                            "stmt == null" + "\r\n\r\n" +
                            sqlString
            );
            return null;
        } // if
        if ( stmt.length == 0 ){
            CRSF = "stmt.length";
            Utils.outln(
                    "??????????????????????????????????????????????????????????????????\r\n"+
                            "MConnection.createResultSet: Ошибка SQL запроса:\r\n" +
                            "stmt.length == 0" + "\r\n\r\n" +
                            sqlString
            );
            return null;
        } // if
        if ( stmt[ 0 ] == null ){
            CRSF = "stmt[ 0 ] == null";
            Utils.outln(
                    "??????????????????????????????????????????????????????????????????\r\n"+
                            "MConnection.createResultSet: Ошибка SQL запроса:\r\n" +
                            "stmt[ 0 ] == null" + "\r\n\r\n" +
                            sqlString
            );
            return null;
        } // if

        ResultSet rs = null;

        // TODO
        //String sqlExt = this.extSql( sqlString );
        String sqlExt = sqlString;
        try {
            rs = stmt[ 0 ].executeQuery( sqlExt );
            return rs;
        } catch( java.sql.SQLSyntaxErrorException see ){
            CRSF = IS_SEE__PREFIX + see;
            Utils.outln( "Ошибка SQL запроса:" );
            Utils.outln( IS_SEE__PREFIX + see );
            Utils.outln( "--sqlString-----------------------------------------------" );
            Utils.outln( "sqlString=" + sqlString );
            Utils.outln( "sqlExt=" + sqlString );
            Utils.outln( "----------------------------------------------------------" );
            see.printStackTrace();
            return null;
        } catch(SQLException e){
            CRSF = IS_REL_NOT_EXIST__PREFIX + e;
            Utils.outln( "Ошибка SQL запроса:" );
            Utils.outln( IS_REL_NOT_EXIST__PREFIX + e );
            Utils.outln( "--sqlString-----------------------------------------------" );
            Utils.outln( "sqlString=" + sqlString );
            Utils.outln( "sqlExt=" + sqlString );
            Utils.outln( "----------------------------------------------------------" );
            //e.printStackTrace();
            return null;
        }
    }

    // con act !!
    private ResultSet createResultSet(int resultSetType, int resultSetConcurency, String sqlString){
        warningNotActivatedDSConnection();

        Statement[] stmt = {
            this.createStatement(resultSetType, resultSetConcurency)
        };
        if (stmt[0] == null){
            return null;
        }
        return this.createResultSet(stmt, sqlString);
    }

    // con act !!
    private Statement createStatement(int resultSetType, int resultSetConcurency){
        //---------------------------
        //  resultSetType:
        //    TYPE_SCROLL_SENSITIVE,
        //    TYPE_FORWARD_ONLY,
        //    TYPE_SCROLL_INSENSITIVE
        //---------------------------
        //  resultSetConcurency:
        //    CONCUR_READ_ONLY
        //    CONCUR_UPDATABLE
        //---------------------------
        warningNotActivatedDSConnection();

        Statement stmt = null;
        try {
            stmt = this.connection.createStatement( resultSetType, resultSetConcurency );
            return stmt;
        } catch(SQLException ex){
            Utils.outln("MConnection#createStatement: ex=" + ex);
            ex.printStackTrace();
            return null;
        }
    }

    public static void closeRsPs(ResultSet rs, PreparedStatement ps){
        try {
            if ( rs != null ){
                rs.close();
            }
            if ( ps != null ){
                ps.close();
            }
        } catch ( Exception e ){
            e.printStackTrace();
        }
    }

    private static void closeResultSetAndStatement(ResultSet rs) {
        if (rs == null || isRsNotValid(rs)) {
            return;
        }

        try {
            Statement stmt = rs.getStatement();
            rs.close();
            if (stmt != null){
                stmt.close();
            }
        } catch (SQLException ex) {
            Utils.outln("MConnection#closeResultSetAndStatement: ex=" + ex);
            ex.printStackTrace();
        }
    }

    private static boolean isRsNotValid(ResultSet rs){
        if (rs == null) {
            return true;
        }
        try {
            rs.beforeFirst();
            return false;
        } catch (SQLException ex){
            return true;
        }
    }
    //^^ResultSet

    // DataSource
    private boolean closeDSConnection(Connection connection){
        //    if ( connection == null ){
        //        //    if ( act_pas_warning_flag ){
        //        //        Utils.outln( "?????????? MConnection.closeDataSourceCon: con == null" );
        //        //        try {
        //        //            int x = 1 / 0;
        //        //        } catch ( Exception e ){
        //        //            e.printStackTrace();
        //        //        } // try
        //        //    } // if
        //        return false;
        //    } // if

        try {
            //String str = connection.toString();
            connection.close();
            return true;
        } catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

    private boolean warningNotActivatedDSConnection(){
        if (this.connection == null){
            Utils.outln(
                    "???????????????????????????????????????????????????????????????????????????????"
            );
            Utils.outln( "MConnection.warning_DS_con_act: ( this.getCon() == null )" );
            Utils.outln(
                    "???????????????????????????????????????????????????????????????????????????????"
            );

            try {
                int x = 1 / 0;
            } catch ( Exception e ){
                e.printStackTrace();
            }

            return false;
        } else {
            return true;
        } // if
    }
    //^^DataSource

    private static String[] splitFieldNames(String fieldNames) {
        if (fieldNames == null || fieldNames.isEmpty()) {
            return null;
        }

        fieldNames = fieldNames.trim();
        if (fieldNames.isEmpty()) {
            return null;
        }

        String[] fns = fieldNames.split(DELIM);
        for (int i = 0; i < fns.length; i++) {
            fns[i] = fns[i].trim();
        }
        return fns;
    }

    // gen Id
    private Object genId(String tableName, int delta){
        if (this.sequences == null){
            // TABLES
            return genIdLoc(tableName, delta);
        } else {
            // sequences (Postgres)
            return selectNextval(sequences.get(tableName));
        }
    }

    private Object selectNextval(String seqName){
        if (seqName == null || seqName.isEmpty()){
            return null;
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // postgresql: Select NEXTVAL ('SEQNAME')
            // h2        : Select NEXTVAL ('SCHEMANAME','SEQNAME')
            ps = this.connection.prepareStatement("SELECT nextval(?)");
            ps.setString(1, seqName);
            rs = ps.executeQuery();
            if (rs.next()){
                return rs.getObject( 1 );
            } else {
                return null;
            }
        } catch ( Exception ex ){
            ex.printStackTrace();
            return null;
        } finally {
            closeRsPs(rs, ps);
        }
    }

    private Long genIdLoc(String tableName, int delta){
        // ....................................................................
        // UPDATE TABLES
        // SET
        //   GENERATOR = GENERATOR + 1
        // WHERE
        //   NAME = ?
        // ....................................................................
        // UPDATE " + TABLES + " SET GENERATOR = GENERATOR + " + Delta + " WHERE Name = ?
        // ....................................................................

        if ( tableName == null || tableName.isEmpty() || delta == 0){
            return null;
        }

        Long gen = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // update
            ps = this.connection.prepareStatement(
                    "UPDATE " + TABLES + " SET GENERATOR = GENERATOR + "+ delta + " WHERE Name = ?"
            );
            ps.setString( 1, tableName.toUpperCase() );
            ps.executeUpdate();
            ps.close();
            //^^update

            // get
            ps = this.connection.prepareStatement(
                    "SELECT Generator FROM "+TABLES+" WHERE ( Name = ? )"
            );
            ps.setObject( 1, TABLES.toUpperCase() );
            rs = ps.executeQuery();
            rs.next();
            gen = rs.getLong( "GENERATOR" );
            //^^get

            return gen;
        } catch ( Exception ex ) {
            ex.printStackTrace();
            return null;
        } finally {
            closeRsPs(rs, ps);
        }
    }
    //^^gen Id

    private static class ConnectionReply {
        protected Connection connection = null;
        protected String error = null;
    }

    private static ConnectionReply createConnection(String driver, String url, String login, String password){
        ConnectionReply connectionReply = new ConnectionReply();
        try {
            Class.forName( driver );

            Locale locale = Locale.getDefault();
            Locale.setDefault( Locale.US );
            connectionReply.connection = DriverManager.getConnection(url, login, password);
            Locale.setDefault( locale );

            connectionReply.connection.setAutoCommit( false );
        } catch(ClassNotFoundException cnfe){
            connectionReply.connection = null;
            connectionReply.error = cnfe.toString();
        } catch(SQLException sqle) {
            connectionReply.connection = null;
            connectionReply.error = sqle.toString();
        }
        return connectionReply;
    }
}
