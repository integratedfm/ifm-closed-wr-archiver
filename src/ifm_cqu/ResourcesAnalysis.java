/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ifm_cqu;

import ifm.db.MSSqlJDBC;
import static ifm_cqu.IfmCQU.AddError;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author ifmuser1
 */
public class ResourcesAnalysis {

    //get list of closed wr and wo remaining in the wr and wo without being moved to hwr and hwo
    //get list of all wrs associated to the closed wr and check if they are closed
    //for each wo create a list of wrs and their status
    //for each wo with all wrs in the 'Clo' status move assosiated resources to the history and delete from active table
    //update using the steps in workrequesthandler.archiveWorkOrder
    final static Logger logger = Logger.getLogger(ResourcesAnalysis.class);

    private Map<Integer, List<WrRec>> woMap = new HashMap<>();

    private List<WrRec> addWOToMap(Integer wo_id) {
        if (wo_id == null) {
            return null;
        }
        List<WrRec> wr_list = woMap.get(wo_id);
        if (wr_list == null) {
            wr_list = new ArrayList<>();
            woMap.put(wo_id, wr_list);
        }
        return wr_list;
    }

    private void addWrRecToList(WrRec rec, Integer wo_id) {
        List<WrRec> woList = addWOToMap(wo_id);

        boolean wrInsrted = false;
        for (WrRec wr : woList) {
            if (wr.wr_id == rec.wr_id) {
                wrInsrted = true;
                break;
            }
        }

        if (woList != null && !wrInsrted) {
            woList.add(rec);
        }
    }

    private static String get_all_table_fields = "SELECT field_name FROM afm.afm_flds where table_name='";

    public static String getAllFieldNames(String table_name) {
        Statement stmt = null;
        final StringBuffer fields = new StringBuffer();

        try {
            Connection con = MSSqlJDBC.getSqlServerDBConnection();
            if (con == null) {
                return null;
            }
            ResultSet rs = null;
            stmt = con.createStatement();
            String sql=get_all_table_fields + table_name+"'";
            rs = stmt.executeQuery(sql);

            while (rs.next()) {
                String fn=rs.getString("field_name").trim()+",";
                fields.append(fn);
            }
            fields.deleteCharAt(fields.length() - 1);//remove last comma

        } catch (SQLException ex) {
            logger.error(ex.getMessage() + " Error: from inside getAllFieldNames Method");
            AddError(ex.getMessage() + " Error: from inside getAllFieldNames Method");
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex1) {
                    logger.error(ex1.getMessage() + " Error: from inside getAllFieldNames Method");
                    AddError(ex1.getMessage() + " Error: from inside getAllFieldNames Method");

                }
            }
        }

        return fields.toString();
    }
    
    public static boolean execDbSql_test(String sql){
        System.out.println(sql);
        return true;
    }
    public static boolean execDbSql(String sql) {
        Statement stmt = null;
        ResultSet rs;
        boolean success = false;

        try {
            Connection con = MSSqlJDBC.getSqlServerDBConnection();
            if (con == null) {
                return success;
            }
            rs = null;
            stmt = con.createStatement();
            rs = stmt.executeQuery(sql);
            success = true;

        } catch (SQLException ex) {

            logger.error(ex.getMessage() + " Error: from inside execDbSql Method");
            AddError(ex.getMessage() + " Error: from inside execDbSql Method");

        } finally {

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex1) {
                    logger.error(ex1.getMessage() + " Error: from inside execDbSql Method");
                    AddError(ex1.getMessage() + " Error: from inside execDbSql Method");
                }
            }
        }
        return success;
    }

    public static void executeDbSqlCommands(List<String> sqls) {
        for (String sql : sqls) {
            execDbSql(sql);
        }
    }

    public static void execDbCommit() {
        Connection con = MSSqlJDBC.getSqlServerDBConnection();
        try {
            con.commit();
        } catch (SQLException ex) {
            logger.error(ex.getMessage() + " Error: in committing");
        }
    }

    public static List<String> selectDbRecords(String table_name, String fld, String cond) {
        Statement stmt = null;
        Connection con;
        List<String> ls = new ArrayList();
        try {
            con = MSSqlJDBC.getSqlServerDBConnection();
            if (con == null) {
                return null;
            }
            ResultSet rs = null;
            stmt = con.createStatement();
            StringBuffer sb = new StringBuffer("SELECT ").append(fld);
            sb.append(" FROM " + table_name + " WHERE " + cond);

            rs = stmt.executeQuery(sb.toString());
            while (rs.next()) {
                int id = rs.getInt(fld);
                ls.add(Integer.toString(id));
            }

        } catch (SQLException ex) {
            logger.error(ex.getMessage() + " Error: from inside selectDbRecords");
            AddError(ex.getMessage() + " Error: from inside selectDbRecords");
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex1) {
                    logger.error(ex1.getMessage() + " Error: from inside selectDbRecords");
                    AddError(ex1.getMessage() + " Error: from inside selectDbRecords");
                }
            }
        }

        return ls;
    }

    public void archiveWorkRequestDocument(final String tableName, final int id) {

        String sql;

        if ("wo".equals(tableName)) {
            sql
                    = "UPDATE afm.afm_docs SET table_name='hwr' where table_name='wr' and pkey_value IN (select wr.wr_id from wr where wr.wo_id = " + id + ")";
        } else {
            sql = "UPDATE afm.afm_docs SET table_name='hwr' where table_name='wr' and pkey_value =" + id;
        }
        execDbSql(sql);

    }

    /**
     *
     * @param cond is like e.g. activity_log_id=345"
     */
    public void archiveHelper(String cond) {
        String fields = getAllFieldNames("activity_log");
        String insert = "INSERT into afm.hactivity_log (" + fields + ") "
                + "SELECT " + fields + " FROM afm.activity_log WHERE " + cond;
        execDbSql(insert);

        String delete = "DELETE FROM afm.activity_log WHERE " + cond;
        execDbSql(delete);

        cond = cond.replaceFirst("activity_log_id", "pkey_value");
        final String update
                = "UPDATE afm.helpdesk_step_log SET table_name= 'hactivity_log' "
                + "WHERE table_name='activity_log' AND " + cond;
        execDbSql(update);
    }

    private static String findProblematicWRs = "select wr_id, wo_id, status from wr "
            + "where wo_id in (select wo_id from wr where wr_id in (select wr_id from wr where status like '%Clo%')) "
            + "order by wo_id, wr_id";

    final void initWOMapWRList() {
        Statement stmt = null;
        Integer nwo_id, nwr_id, nact_log_id;
        WrRec nwr;
        String stat;
        try {
            Connection con = MSSqlJDBC.getSqlServerDBConnection();
            if (con == null) {
                return;
            }
            ResultSet rs = null;
            stmt = con.createStatement();
            rs = stmt.executeQuery(findProblematicWRs);

            while (rs.next()) {
                nwo_id = rs.getInt("wo_id");
                if (nwo_id == null) {
                    continue;
                }
                nwr_id = rs.getInt("wr_id");
                if (nwr_id == null) {
                    continue;
                }

                stat = rs.getString("status");
                //String s, int wo, int wr
                nwr = new WrRec(stat, nwo_id, nwr_id);
                ///(WrRec rec, Integer wo_id)
                addWrRecToList(nwr, nwo_id);
            }

        } catch (SQLException ex) {
            logger.error(ex.getMessage() + " Error: from inside initWOMapWRList Method");
            AddError(ex.getMessage() + " Error: from inside initWOMapWRList Method");
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex1) {
                    logger.error(ex1.getMessage() + " Error: from inside initWOMapWRList Method");
                    AddError(ex1.getMessage() + " Error: from inside initWOMapWRList Method");
                }
            }
        }
    }

    final void archiveTable(final String table_name,
            final int wo_id, final int wr_id) {
        final String fields = getAllFieldNames(table_name);//get comma separated
        String where = null;
        if (wo_id != 0 && table_name.equals("wo")) {
            where = " wo_id = " + wo_id;
        } else if (table_name.equals("wr")) {

            //   calculateDiffHour(wo_id, wr_id);
            // check if work request to be deleted is record with maximal id
            // int max_wr_id = Common.getMaxId(context, "wr", "wr_id", null);
            // boolean max = false;
            if (wo_id != 0) {
                where = "wo_id = " + wo_id;

                final String update
                        = "UPDATE afm.helpdesk_step_log"
                        + "  SET table_name='hwr' "
                        + " WHERE table_name='wr' AND pkey_value IN (SELECT wr_id FROM wr WHERE wo_id = "
                        + wo_id + ")";

                execDbSql(update);
            }

        } else if (wr_id != 0) {
            where = "wr_id = " + wr_id;
        } else if (wo_id != 0) {
            where = "wr_id IN (SELECT wr_id FROM wr WHERE wo_id = " + wo_id + ")";
        }
        // format insert query
        final String insert
                = "INSERT into afm.h" + table_name + " (" + fields + ") SELECT "
                + fields + " FROM afm." + table_name + " WHERE " + where;
        execDbSql(insert);

    }

    private boolean allWrClosed(Integer wo) {
        boolean allClosed = true;
        List<WrRec> wl = woMap.get(wo);
        for (WrRec w : wl) {
            if (!w.stat.equals("Clo")) {
                allClosed = false;
                break;
            }

        }
        return allClosed;
    }

    public void archiveWorkRequest(final int wr_id) {

        // wr
        archiveTable("wr", 0, wr_id);
        // wrtt
        archiveTable("wrtt", 0, wr_id);
        // wrtr
        archiveTable("wrtr", 0, wr_id);
        // wrpt
        archiveTable("wrpt", 0, wr_id);
        // wrtl
        archiveTable("wrtl", 0, wr_id);
        // wrcf
        archiveTable("wrcf", 0, wr_id);
        // wr_other
        archiveTable("wr_other", 0, wr_id);

        //archiveTable("activity_log", 0, wr_id);
        String sql = "SELECT activity_log_id FROM afm.activity_log where wr_id="+wr_id;
        
///(String table_name, String fld, String cond)
        List<String> activity_logs = selectDbRecords("afm.activity_log", "activity_log_id", " wr_id="+wr_id);
        for(String acid:activity_logs){
            archiveHelper(" activity_log_id=" + acid);
        }
                

        final String deleteWr_other = "DELETE FROM wr_other WHERE wr_id  = " + wr_id;
        final String deleteWrcf = "DELETE FROM wrcf WHERE wr_id = " + wr_id;
        final String deleteWrtl = "DELETE FROM wrtl WHERE wr_id = " + wr_id;
        final String deleteWrpt = "DELETE FROM wrpt WHERE wr_id = " + wr_id;
        final String deleteWrtr = "DELETE FROM wrtr WHERE wr_id = " + wr_id;
        final String deleteWrtt = "DELETE FROM wrtt WHERE wr_id = " + wr_id;
        final String deleteWr = "DELETE FROM wr WHERE wr_id = " + wr_id;

        // cascade delete all
        final List<String> commands = new ArrayList();
        commands.add(deleteWr_other);
        commands.add(deleteWrcf);
        commands.add(deleteWrtl);
        commands.add(deleteWrpt);
        commands.add(deleteWrtr);
        commands.add(deleteWrtt);
        commands.add(deleteWr);

        executeDbSqlCommands(commands);

        //archive associated activity_log records
//      archiveWorkRequestDocument("wr", wr_id);
        archiveWorkRequestDocument("wr", wr_id);
        
        // update records in helpdesk_step_log
        final String update =
                "UPDATE afm.helpdesk_step_log  SET table_name='hwr' "
                        + " WHERE table_name='wr' AND pkey_value = " + wr_id;
        
        execDbSql(update);
        // update records in hhelpdesk_step_log
        archiveStepLog();
        
    }
    /**
     * Archive helpdesk_step_log data to hhelpdesk_step_log
     *   
     * KB3034237 - Create an archive table for helpdesk_step_log      
     */
    protected void archiveStepLog() {
        
        String flds= getAllFieldNames("helpdesk_step_log");
        final String insert =
                    "INSERT into afm.hhelpdesk_step_log (" + flds + ") SELECT "
                            + flds + " FROM afm.helpdesk_step_log WHERE table_name IN('hwr','hactivity_log')";
            
        execDbSql(insert);
    }

    public void analyseWrWo() {
        initWOMapWRList();
        Set<Integer> woSet = woMap.keySet();
        for (Integer wo : woSet) {
            boolean allClosed = allWrClosed(wo);
            if (allClosed) {
                //archive all wr, wr_other, ..., wo in the same order as AB
                List<WrRec> wl = woMap.get(wo);
                archiveTable("wo", wo, 0);
                for (WrRec w : wl) {
                    archiveWorkRequest(w.wr_id);
                }
                System.out.println("Finished archiving of wo "+wo);
                execDbCommit();
            }
        }
    }

}

class WrRec {

    String stat;
    int wr_id;
    int wo_id;

    WrRec(String s, int wo, int wr) {
        stat = s;
        wo_id = wo;
        wr_id = wr;
    }

}
