/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ifm_cqu;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 * 1- find wr where wr.status=Closed, get wo, wr_other, activity_log, wrtr and
 * move all to history 2- use archiveTable in archibus to move to history
 *
 * @author ifmuser1
 */
public class IfmCQU {

    final static Logger logger = Logger.getLogger(IfmCQU.class);
    private static Properties props;
    private static List<String> errorList = new ArrayList<>();
    private static DateFormat smdf = new SimpleDateFormat("dd-MM-yy hh:mm:ss");
    private static Date cur_date = new Date();

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException {

        FileInputStream fis;
        fis = new FileInputStream(new File(args[0]));///read the properties file using the first input argument
        props = new Properties();
        props.load(fis);//load all the propties
        fis.close();

        // TODO code application logic here
        ResourcesAnalysis ra = new ResourcesAnalysis();
        ra.analyseWrWo();
    }

    public static String getProp(String pName) {
        return props.getProperty(pName);
    }

    public static void AddError(String err) {
        String tm = getCurrentDateTime();
        String err_msg = "[Error]" + " " + tm + " " + err;
        errorList.add(err_msg);
    }

    public static String getCurrentDateTime() {
        cur_date.setTime(System.currentTimeMillis());
        String cur_tm = smdf.format(cur_date);
        return cur_tm;
    }

}
