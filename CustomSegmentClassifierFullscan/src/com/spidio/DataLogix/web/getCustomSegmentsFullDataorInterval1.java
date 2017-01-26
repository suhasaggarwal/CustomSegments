package com.spidio.DataLogix.web;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.xml.sax.InputSource;

import com.kohlschutter.boilerpipe.document.TextDocument;
import com.kohlschutter.boilerpipe.extractors.ArticleExtractor;
import com.kohlschutter.boilerpipe.sax.BoilerpipeSAXInput;
import com.kohlschutter.boilerpipe.sax.HTMLFetcher;
import com.rules.model.Rules;
import com.rules.model.RulesDAOImpl;
import com.spidio.UserSegmenter.IndexCategoriesData;
import com.spidio.UserSegmenter.IndexCategoriesData1;
import com.spidio.UserSegmenter.IndexCategoriesData2;
import com.spidio.UserSegmenter.ProcessRefurl;
import com.spidio.UserSegmenter.TitleExtractor;
import com.spidio.UserSegmenter.TitleExtractorRegex;
import com.sree.textbytes.jtopia.Configuration;
import com.sree.textbytes.jtopia.TermDocument;
import com.sree.textbytes.jtopia.TermsExtractor;

public class getCustomSegmentsFullDataorInterval1 {
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		/**
		 * This method returns ESclient instance.
		 *
		 * @return the es client ESClient instance
		 */

		Client client = new TransportClient()
				.addTransportAddress(new InetSocketTransportAddress(
						"localhost", 9300));

		// Client client = new TransportClient()
		// .addTransportAddress(new InetSocketTransportAddress(
		// "localhost", 9300));

		
		String category = null;
		String subcategory = null;
		
		String refurl = null;
		String title = null;
		String audienceSegment = null;
		
		String id = null;
		List<Rules> rules  = new ArrayList<Rules>(); 

		String gender = null;
		String agegroup = null;
		String incomelevel = null;
		
		
		
		
		Map<String, Integer> CustomerSegmentMap = null;
		
		
		String genderValue = null;

		String incomeValue = null;

		String ageValue = null;
		
		String custom_segment = null;

		Integer flag1 = 0;

		Integer flag2 = 0;

		Integer flag3 = 0;

		String fingerprintId = null;

		Map<String, String> categorysubcategoryMap = new HashMap<String, String>();

		
		List<Map<String,Integer>> rulemaps = new ArrayList<Map<String,Integer>>();
		
	/*		
		try {
			System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	*/
		

		// SearchHit[] results = IndexCategoriesData2.searchDocumentContent(
		// client, "userprofilestore",
		// "core2","fingerprint_id","575962882.967132425.3646610088.120");

	
		// Scroll until no hits are returned

        
		SearchResponse response1 = client.prepareSearch("userprofilestore")
				.setTypes("core2").setSearchType(SearchType.QUERY_THEN_FETCH)
				.setScroll(new TimeValue(600000))
				.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),FilterBuilders.rangeFilter("request_time").from("now-82d").to("now"))).setSize(10).execute()
				.actionGet();

		// Scroll until no hits are returned

		
		rules = RulesDAOImpl.getInstance().FetchRulesData();		
		
		for(int i=0; i < rules.size(); i++){
        	
	          rulemaps.add(new HashMap<String, Integer>());	
	      }
		
		
		
		while (true) {
		
			
		for (SearchHit hit : response1.getHits().getHits()) {
		// Scroll until no hits are returned

		
			// Scroll until no hits are returned
				try {

					// System.out.println("------------------------------");

					Map<String, Object> result = hit.getSource();
					fingerprintId = (String) result.get("fingerprint_id");

				  //  fingerprintId = "3708452540.3793673478.132992963";
					
					
				 	//fingerprintId = "202534697.3230925007.1677986676";
					
					
				//	fingerprintId = "367811237.1964553723.1081626625";
					
					System.out.println(fingerprintId);
					
					String [] fingerprintSegments = fingerprintId.split("\\.");
					//	System.out.println(fingerprintSegments[0]+":"+fingerprintSegments[1]+":"+fingerprintSegments[2]+":"+fingerprintSegments[3]);
					    if(fingerprintSegments.length > 1)
						fingerprintId = fingerprintSegments[0].trim()+"."+fingerprintSegments[1].trim()+"."+fingerprintSegments[2].trim();
						fingerprintId = fingerprintId.trim();

					// Store/Site specific Categorisation

					SearchHit[] results1 = IndexCategoriesData2
							.searchDocumentContent(client, "userprofilestore",
									"core2", "fingerprint_id", fingerprintId);

					for (SearchHit hit0 : results1) {

						
							
							Map<String, Object> result0 = hit0.getSource();
							subcategory = (String) result0.get("subcategory");

							
							id = (String) hit0.getId();

						//	String gender1 = (String)result0.get("gender");
						//	System.out.println("Test index"+gender);
							
							gender = (String) result0.get("gender");
							
							agegroup = (String) result0.get("agegroup");
							
							incomelevel = (String) result0.get("incomelevel");
						   
							//if(custom_segment == null)
						//	custom_segment=(String) result0.get("custom_segment");

							audienceSegment = (String) result0.get("audience_segment");

						    
					        
					        for(int i=0; i < rules.size(); i++){
					        
					        if(subcategory != null)	{
					        if(rules.get(i).getInterestSegment().contains(subcategory))	
					        	rulemaps.get(i).put("Subcategory",1);
					       
					        }
					        
					        
					        if(gender != null){
					        if(rules.get(i).getGender().contains(gender))	
					        	rulemaps.get(i).put("Gender",1);
					        }
					        
					        
					        if(agegroup != null){
					        if(agegroup.contains(rules.get(i).getAgeGroup()))		
					        	rulemaps.get(i).put("AgeGroup",1);
					        }
					        
					        
					        if(incomelevel != null){
					        if(rules.get(i).getIncomeLevel().contains(incomelevel))		
					    	    rulemaps.get(i).put("IncomeLevel",1);
					        } 
					        
					        }
										
					}		
						
				// Break condition: No hits are returned
					
					int sum = 0;
					
					
					for(int i=0; i < rules.size(); i++){
				        	
						    //    CustomerSegmentMap = new HashMap<String, Integer>();	
						 Iterator it = rulemaps.get(i).entrySet().iterator();
						    while (it.hasNext()) {
						        Map.Entry pair = (Map.Entry)it.next();
						        sum = sum + Integer.parseInt(pair.getValue().toString());
						        it.remove(); // avoids a ConcurrentModificationException
						    }
						        
						    
						    System.out.println("Sum:"+sum);
						    
						        if(sum >= 2){
						        if(i == 0)	
						        custom_segment = rules.get(i).getRuleName();
						        else
						        	custom_segment = custom_segment+","+rules.get(i).getRuleName();
						        }
				               
						        System.out.println(custom_segment);
						        
						 rulemaps.get(i).clear();       
				//Insert query to update status of rule as classification complete status as expired or remaining
				         sum =0;
				      
				}
			// System.out.println("Number of scrolls:"+count3);
                     if(custom_segment != null){
					IndexCategoriesData.updateDocument(client,
							"userprofilestore", "core2", id,
							 "custom_segment",custom_segment);	
                     }
		
				    custom_segment = null;
				
				
				} catch (Exception e) {

					e.printStackTrace();
					// continue;

				}

	      	
				
		
		         response1 = client.prepareSearchScroll(response1.getScrollId())
						.setScroll(new TimeValue(600000)).execute().actionGet();
				// Break condition: No hits are returned
		
		}
		
		
		if (response1.getHits().getHits().length == 0) {
			break;
		}

	}
		
		for(int i=0; i < rules.size(); i++){
        	
		RulesDAOImpl.getInstance().updateStatusForRule(rules.get(i).getRuleName());	
		}
		

		
	 }
  }
	
	