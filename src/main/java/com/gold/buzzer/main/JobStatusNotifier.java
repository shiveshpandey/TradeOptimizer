
package com.gold.buzzer.main;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.Properties;
//
//import javax.mail.Message;
//import javax.mail.MessagingException;
//import javax.mail.PasswordAuthentication;
//import javax.mail.Session;
//import javax.mail.Transport;
//import javax.mail.internet.InternetAddress;
//import javax.mail.internet.MimeMessage;
//
//import com.sybase.jdbc3.jdbc.SybDriver;

/**
 * @author shpande
 *
 */
public class JobStatusNotifier {
//
//    private String JDBC_DRIVER = "com.sybase.jdbc3.jdbc.SybDriver";
//    private String USER = "ShPande";
//    private String DB_URL = "jdbc:sybase:Tds:psplsyb06p.fleet.ad:1050";
//    private String PASS = "Gamer@123";
//    private String successJobString = "";
//    private String inProgressJobString = "";
//    private String errorJobString = "";
//    private String to = "shpande@elementcorp.com";
//    private String from = "shpande@elementcorp.com";
//    private String password = "@@Maya12345P";
//    Connection conn = null;
//
//    /**
//     * @param args
//     */
//    public static void main(String[] args) {
//        JobStatusNotifier jobStatusNotifier = new JobStatusNotifier();
//        try {
//            jobStatusNotifier.initializeJDBCConn();
//            jobStatusNotifier.checkJobStatus();
//            jobStatusNotifier.closeJDBCConn();
//        } catch (ClassNotFoundException | SQLException e) {
//            e.printStackTrace();
//        } catch (InstantiationException e) {
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        }
//        jobStatusNotifier.sendEmailToUsers(jobStatusNotifier.successJobString,
//                jobStatusNotifier.inProgressJobString, jobStatusNotifier.errorJobString);
//    }
//
//    public void initializeJDBCConn() throws ClassNotFoundException, SQLException,
//            InstantiationException, IllegalAccessException {
//
//        @SuppressWarnings("unused")
//        SybDriver sybDriver = (SybDriver) Class.forName(JDBC_DRIVER).newInstance();
//        conn = DriverManager.getConnection(DB_URL, USER, PASS);
//    }
//
//    public void checkJobStatus() throws SQLException {
//        if (conn != null) {
//            Statement stmt;
//
//            stmt = conn.createStatement();
//
//            String sql = "select top 100 h.* from Ordering_job_execution_history h order by h.job_start_dt desc";
//            ResultSet resultSet = stmt.executeQuery(sql);
//
//            for (int i = 0; i < resultSet.getFetchSize(); i++) {
//                resultSet.next();
//                formatEmailString(resultSet.getString(1), resultSet.getString(2),
//                        resultSet.getString(3), resultSet.getString(4), resultSet.getString(5));
//            }
//        }
//    }
//
//    public void closeJDBCConn() throws SQLException {
//        if (conn != null) {
//            conn.close();
//        }
//    }
//
//    private void formatEmailString(String jobCode, String startTime, String endTime, String status,
//            String jobNotes) {
//        if (Integer.parseInt(status) == 2)
//            successJobString += jobCode + startTime + endTime + status + jobNotes;
//        if (Integer.parseInt(status) == 1)
//            inProgressJobString += jobCode + startTime + endTime + status + jobNotes;
//        if (Integer.parseInt(status) == 3)
//            errorJobString += jobCode + startTime + endTime + status + jobNotes;
//    }
//
//    private void sendEmailToUsers(String successJobString, String inProgressJobString,
//            String errorJobString) {
//
//        Properties props = new Properties();
//        props.put("mail.smtp.auth", "true");
//        props.put("mail.smtp.starttls.enable", "true");
//        props.put("mail.smtp.host", "smtp.office365.com");
//        props.put("mail.smtp.port", "587");
//
//        Session session = Session.getInstance(props, new javax.mail.Authenticator() {
//            @Override
//            protected PasswordAuthentication getPasswordAuthentication() {
//                return new PasswordAuthentication(from, password);
//            }
//        });
//      
//        try {
//
//            Message message = new MimeMessage(session);
//            message.setFrom(new InternetAddress(from));
//            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
//            message.setSubject("Test");
//            message.setText("HI you have done sending mail with outlook");
//            Transport.send(message);
//
//            System.out.println("Done");
//
//        } catch (MessagingException e) {
//            throw new RuntimeException(e);
//        }
  //  }
}
