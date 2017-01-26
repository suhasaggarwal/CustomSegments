package com.rules.model;



import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import util.DTOPopulator;
import util.DBConnector;



//Reports are generated corresponding to Audience Section, for user interests,demographics,device properties, geography based reports


public class RulesDAOImpl {

	

	private static RulesDAOImpl INSTANCE;

	private static final Logger logger = Logger.getLogger(RulesDAOImpl.class);

	public static RulesDAOImpl getInstance() {
		
		if(INSTANCE == null)
			return new RulesDAOImpl();
		else
		return INSTANCE;
	}

	public List<Rules> FetchRulesData() {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Rules> rule = null;
		
		
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();

			if (connection != null) {
				
			//	QueryString = "Select * FROM CustomSegments WHERE date like "
			//			+ "'%"+dateScan +"%'";
				
				
				
				QueryString = "Select * FROM CustomSegments WHERE Status = "+"'false'";
				
				
				
				System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

			//	int count = 0;

			//	while (rs.next()) {
			//	    ++count;
				    // Get data from the current row and use it
			//	}
				
			//	System.out.println(count);
				
				
				rule=DTOPopulator.populateDTO(rs);
                 	
				
				
				//	jo.put("data",obj1);
				
				//Resultset to json converter
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
             
		finally{
			
			DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		return rule;
	
	
	
	}


	public void updateStatusForRule(String rulename) {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Rules> rule = null;
		
		
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();

			if (connection != null) {
				
			//	QueryString = "Select * FROM CustomSegments WHERE date like "
			//			+ "'%"+dateScan +"%'";
				
				
				
				QueryString = "Update CustomSegment Set Status="+"'true'"+"where RuleName="+rulename;
				
				
				
				System.out.println(QueryString+"\n");
				
				preparedStatement = connection.prepareStatement(QueryString);
				
				preparedStatement.setString(1,"true");

			//	int count = 0;

			//	while (rs.next()) {
			//	    ++count;
				    // Get data from the current row and use it
			//	}
				
			//	System.out.println(count);
				preparedStatement.execute();
				
				
                 	
				
				
				//	jo.put("data",obj1);
				
				//Resultset to json converter
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
             
		finally{
			
			DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
	
	
	
	
	}




















}
