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
import java.util.*;
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
                    completeString=completeString+",38003600";
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
                //Provider Table Population - currently only one provider exists and we would
                sql = "SELECT DISTINCT * from openmrs.provider;";
                rs = stmt.executeQuery(sql);
                while(rs.next()) {
                    completeString="\nINSERT INTO provider(provider_id, provider_name, npi, dea, specialty_concept_id, care_site_id,year_of_birth, gender_concept_id, provider_source_value, specialty_source_value,specialty_source_concept_id, gender_source_value, gender_source_concept_id)" +
                            "VALUES ("+rs.getInt("provider_id")+",";
                    if(rs.getString("name")!=null){
                        completeString=completeString+"'"+rs.getString("name")+"'";
                    }
                    else{
                        completeString=completeString+"'"+rs.getString("identifier")+"'";
                    }
                    completeString=completeString+",null,null,null,null,null,null,null,null,null,null,null);";

                    response.getWriter().write(completeString);
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
                rs.close();

                //Visit_occurrence Table Population
                sql = "SELECT * from openmrs.encounter;";
                rs = stmt.executeQuery(sql);
                //int counter=0;
                while(rs.next()) {
                    counter++;
                    completeString="\nINSERT INTO visit_occurrence(visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_time, visit_end_date, visit_end_time, visit_type_concept_id, provider_id, care_site_id, visit_source_value, visit_source_concept_id)" +
                            "VALUES ("+rs.getInt("encounter_id")+","+rs.getInt("patient_id")+",9202,'"+rs.getDate("encounter_datetime")+"',null,'"+rs.getDate("encounter_datetime")+"',null,44818518,";


                    String encounterProvidersql = "select * from encounter_provider where encounter_id="+rs.getInt("encounter_id")+";";
                    ResultSet encounterProviderset = stmt2.executeQuery(encounterProvidersql);
                    String encounterProviderId="";
                    while(encounterProviderset.next()) {
                        encounterProviderId = encounterProviderset.getString("encounter_provider_id");
                    }
                    completeString=completeString+encounterProviderId+",null,9202,null";
                    completeString=completeString+");";

                    response.getWriter().write(completeString);
                }
                rs.close();
                //condition_occurrence table population
                int[] valueCodedDoNotIncludeArray={1252,1262,1266,1269,5303,6097};
                //List<Integer> valueCodedDoNotIncludeArrayList = new ArrayList<Integer>();
               // valueCodedDoNotIncludeArrayList.addAll(valueCodedDoNotIncludeArray.);
                sql = "SELECT DISTINCT * from openmrs.obs;";
                rs = stmt.executeQuery(sql);
                counter=0;
                while(rs.next()) {
                    //also considering 0 as null
                    if(rs.getInt("value_coded")==0)
                        continue;
                    String valueCodedConceptsql = "select * from concept where concept_id="+rs.getInt("value_coded")+";";
                    ResultSet valueCodedConceptresutset = stmt2.executeQuery(valueCodedConceptsql);
                    int valueCodedClassCheck=0;
                    boolean isUnwantedConcept = false;
                    while(valueCodedConceptresutset.next()) {
                        valueCodedClassCheck=valueCodedConceptresutset.getInt("class_id");

                    }

                    //check for diagnosis
                    if(valueCodedClassCheck!=4 && valueCodedClassCheck!=12 && valueCodedClassCheck!=13)
                        continue;
                    int valueCodedConcept=rs.getInt("concept_id");
                    for(int i: valueCodedDoNotIncludeArray){
                        if( valueCodedConcept==i) {
                            isUnwantedConcept = true;
                        }
                    }
                    //remove unwanted concepts
                    if(isUnwantedConcept)
                        continue;
                    counter++;
                    completeString="\nINSERT INTO condition_occurrence (condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_end_date,condition_type_concept_id,stop_reason,provider_id,visit_occurrence_id,condition_source_value,condition_source_concept_id)" +
                            "VALUES ("+counter+","+rs.getInt("person_id");
                    completeString=completeString+",'"+rs.getInt("concept_id")+rs.getInt("value_coded")+"'";

                    completeString=completeString + ",'" + rs.getDate("obs_datetime")+"'";
                    //conditiontype not null - taking it as 0 for now
                    completeString=completeString+",null,0,null";
                    String encounterProvidersql = "select * from encounter_provider where encounter_id="+rs.getInt("encounter_id")+";";
                    ResultSet encounterProviderset = stmt2.executeQuery(encounterProvidersql);
                    String encounterProviderId="";
                    while(encounterProviderset.next()) {
                        encounterProviderId = encounterProviderset.getString("encounter_provider_id");
                    }
                    //hard coding provider ID as 1 as we only have one provider and there is no specific way to relate an observation with a Provider
                    completeString=completeString+","+encounterProviderId+","+rs.getInt("encounter_id");

                    completeString=completeString+","+rs.getInt("value_coded")+","+rs.getInt("value_coded");

                    completeString=completeString+");";
                    response.getWriter().write(completeString);
                }
                rs.close();
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
