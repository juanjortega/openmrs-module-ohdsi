package org.openmrs.module.ohdsi.web;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by maurya on 7/12/15.
 */
public class DownloadInsertStatementsServlet extends HttpServlet {

    private Log log = LogFactory.getLog(this.getClass());

    /**
     * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest,
     * javax.servlet.http.HttpServletResponse)
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {

            String s = new SimpleDateFormat("dMy_Hm").format(new Date());
            response.setHeader("Content-Type", "text/sql;charset=UTF-8");
            response.setHeader("Content-Disposition", "attachment; filename=ohdsiInserts" + s + ".sql");
            //String headerLine = "Concept Id,Name,Description,Synonyms,Answers,Set Members,Class,Datatype,Changed By,Creator\n";
            //response.getWriter().write(headerLine);

            // JDBC driver name and database URL
            String JDBC_DRIVER = "com.mysql.jdbc.Driver";
            String DB_URL = "jdbc:mysql://localhost/openmrs";

            //  Database credentials
            final String USER = "root";
            final String PASS = "root";

            Connection conn = null;
            Statement stmt = null;
            Statement stmt2 = null;
            try{
                //STEP 2: Register JDBC driver
                Class.forName("com.mysql.jdbc.Driver");

                //STEP 3: Open a connection
                System.out.println("Connecting to a selected database...");
                conn = DriverManager.getConnection(DB_URL, USER, PASS);
                System.out.println("Connected database successfully...");

                //STEP 4: Execute a query
                System.out.println("Creating statement...");
                stmt = conn.createStatement();
                stmt2 = conn.createStatement();

                String sql = "SELECT * FROM openmrs.person;";
                ResultSet rs = stmt.executeQuery(sql);

                //STEP 5: Extract data from result set
                int maleGenderConceptID=8507;
                int femaleGenderConceptID=8532;
                String completeString="";
                while(rs.next()) {
                    //Retrieve by column name
                    completeString="\nINSERT INTO person (person_id, gender_concept_id, year_of_birth,month_of_birth,day_of_birth,time_of_birth,race_concept_id,ethnicity_concept_id,location_id,provider_id,care_site_id,person_source_value, gender_source_value,gender_source_concept_id,race_source_value,race_source_concept_id,ethnicity_source_value,ethnicity_source_concept_id)" +
                            "VALUES ("+rs.getInt("person_id")+",";
                    //gender conditioning
                    if (rs.getString("gender").equalsIgnoreCase("M"))
                        completeString=completeString+maleGenderConceptID;
                    else if (rs.getString("gender").equalsIgnoreCase("F"))
                        completeString=completeString+femaleGenderConceptID;
                    else
                        completeString=completeString+0;
                    //birth date conditioning
                    if(rs.getDate("birthdate")!=null){
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(rs.getDate("birthdate"));
                        int year = cal.get(Calendar.YEAR);
                        int month = cal.get(Calendar.MONTH)+1;
                        int day = cal.get(Calendar.DAY_OF_MONTH);
                        completeString=completeString+","+year+","+month+","+day;
                    }
                    else
                        continue;
                    //time
                    completeString=completeString+",null";
                    //race
                    completeString=completeString+",2060-2";
                    //ethnicity
                    completeString=completeString+",0";

                    //location_id,provider_id,care_site_id,
                    completeString=completeString+",null,null,null";
                    //person source value
                    String patientidentifiersql = "SELECT * FROM openmrs.patient_identifier where patient_id="+rs.getInt("person_id")+";";
                    ResultSet patientidentifierresutset = stmt2.executeQuery(patientidentifiersql);
                    String identifier="";
                    while(patientidentifierresutset.next()) {
                        identifier = patientidentifierresutset.getString("identifier");
                    }
                    completeString=completeString+",'"+rs.getInt("person_id")+identifier+"'";
                    //gendersource
                    if(rs.getString("gender")!=null)
                    completeString=completeString+",'"+rs.getString("gender")+"'";
                    else
                        completeString=completeString+",null";
                    //gender_source_concept_id,race_source_value,race_source_concept_id,ethnicity_source_value,ethnicity_source_concept_id
                    completeString=completeString+",null,null,null,null,null";
                    completeString=completeString+");";

                    response.getWriter().write(completeString);
                    //System.out.println(completeString);
                }
                rs.close();
                //obs table population

                sql = "SELECT DISTINCT person_id from openmrs.obs;";
                rs = stmt.executeQuery(sql);
                int counter=0;
                while(rs.next()) {
                    counter++;
                    completeString="\nINSERT INTO observation_period (observation_period_id,person_id,observation_period_start_date,observation_period_end_date,period_type_concept_id)" +
                            "VALUES ("+counter+","+rs.getInt("person_id");
                    String obsStartDatesql = "select min(obs_datetime) from obs where person_id="+rs.getInt("person_id")+";";
                    ResultSet obsStartDateresutset = stmt2.executeQuery(obsStartDatesql);
                    Date obsStartDate=null;
                    while(obsStartDateresutset.next()) {
                        obsStartDate = obsStartDateresutset.getDate("min(obs_datetime)");
                    }
                    completeString=completeString+",'"+obsStartDate+"'";

                    String obsEndDatesql = "select max(obs_datetime) from obs where person_id="+rs.getInt("person_id")+";";
                    ResultSet obsEndDateresutset = stmt2.executeQuery(obsEndDatesql);
                    Date obsEndDate=null;
                    while(obsEndDateresutset.next()) {
                        obsEndDate = obsEndDateresutset.getDate("max(obs_datetime)");
                    }
                    completeString=completeString+",'"+obsEndDate+"'";
                    completeString=completeString+",0";
                    completeString=completeString+");";

                    response.getWriter().write(completeString);
                }
            }catch(SQLException se){
                //Handle errors for JDBC
                se.printStackTrace();
            }catch(Exception e){
                //Handle errors for Class.forName
                e.printStackTrace();
            }finally{
                //finally block used to close resources
                try{
                    if(stmt!=null)
                        conn.close();
                }catch(SQLException se){
                }// do nothing
                try{
                    if(conn!=null)
                        conn.close();
                }catch(SQLException se){
                    se.printStackTrace();
                }//end finally try
            }//end try
        }
        catch (Exception e) {
            log.error("Error while downloading concepts.", e);
        }
    }
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
}
