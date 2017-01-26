package util;


import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.rules.model.Rules;


//Does computations for CPC,CPM,CPP and embeds in json response

/**
 * Utility for converting ResultSets into DTO
 */
public class DTOPopulator {
    /**
     * Populate a result set into DTO
     
     */
    public static List<Rules> populateDTO(ResultSet resultSet)
            throws Exception {
       
    	   List<Rules> rules = new ArrayList<Rules>();
           Rules ruleDTO = null;
    	  
           while (resultSet.next()) {
        	
        	String id = null;
           	String interestsegment = null;
           	String gender = null;
           	String agegroup = null;
           	String incomelevel = null;  
        	String name = null;       	   
        	int total_columns = resultSet.getMetaData().getColumnCount();
            
        	
        	Rules obj = new Rules();
            for (int i = 0; i < total_columns; i++) {
               name = resultSet.getMetaData().getColumnLabel(i + 1);
              
               if( name.equals("RuleId"))
                obj.setID(resultSet.getObject(i + 1).toString());
               
               
               if( name.equals("RuleName"))
                  obj.setRuleName(resultSet.getObject(i + 1).toString());
                  
               
              if(name.equals("AudienceSegment"))
            	 obj.setInterestSegment(resultSet.getObject(i + 1).toString());
               
               
               if( name.equals("Gender"))
            	  obj.setGender(resultSet.getObject(i + 1).toString());
            
               if( name.equals("AgeGroup"))   
            	   obj.setAgeGroup(resultSet.getObject(i + 1).toString());
            
            
               if( name.equals("IncomeLevel"))
                  obj.setIncomeLevel(resultSet.getObject(i + 1).toString());
                            
            }
            rules.add(obj);
        
        }
        return rules;
    }
    
}