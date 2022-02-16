package org.bgu.ise.ddb;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;


import org.bgu.ise.ddb.items.ItemsController;
import org.bgu.ise.ddb.registration.RegistarationController;

import redis.clients.jedis.Jedis;


public class Utils {

	
	public static final String DB_address = "132.72.65.45";
	
	public static final String USERS = "Users";
	public static final String USERS_REG_STAMP = "Users_reg_stamp";
	public static final String Hash = "#";
	public static final String USER_PRE = USERS + Hash;
	
	public static final String HISTORY = "History";
	public static final String HISTORY_PRE = HISTORY + Hash;
	
	public static final String HISTORY_BY_USER = HISTORY + "_by_users";
	public static final String HISTORY_BY_ITEM = HISTORY + "_by_items";
	
	
	public static final String MEDIAITEM = "MediaItems";
	public static final String MEDIAITEM_PRE = MEDIAITEM+Hash;
	
	public static final String ORACLE_URL = "oracle.jdbc.driver.OracleDriver";
	public static final String connectionURL = "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE";
	
	public static final String ORACLE_USER = "idolou";
	public static final String ORACLE_PASS = "abcd";
	
	public static String getKey(String title, String username)
	{
		return HISTORY + Hash + title + Hash  + username;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
//	================== Delete Items Table ==================
	 public static boolean deleteAllHistory()
	    {
	    	try(Jedis jedis = new Jedis(DB_address))
			{
	    		List<String> keys = new ArrayList<>();
	    		Set<String> titles = jedis.smembers(MEDIAITEM);
	        	
	        	for(String title : titles)
	    		{
	        		// delete index by item
	        		keys.add(Utils.HISTORY_BY_ITEM + Hash + title);
	        	
	    			Set<String> usernames = jedis.smembers(HISTORY_BY_ITEM + Hash + title);
	    			for(String username : usernames)
	    			{
	    				String key = Utils.getKey(title, username);
	    				// delete index by user
	    				keys.add(Utils.HISTORY_BY_USER + Hash + username);
	    				// delete table row
	    				keys.add(key);
	    			}
	    		}
	        	
	        	if(!keys.isEmpty()) 
	        	{
	        		String[] keysToDelete = new String[keys.size()];
	            	keys.toArray(keysToDelete);
	            	jedis.del(keysToDelete);
	        	}
			}
			catch (Exception e) 
	    	{ 
				e.printStackTrace(); 
				return false;
			}

	    	return true;
	    }
	
//	================== Delete Items Table ==================
	 public static boolean deleteItemsTable()
	    {
	    	if(!Utils.deleteAllHistory()) return false;
	    	
	    	try(Jedis jedis = new Jedis(DB_address))
			{
	    		List<String> keys = new ArrayList<>();
	    		// get all the common with mediaItem key
	    		Set<String> items = jedis.smembers(Utils.MEDIAITEM);
	    			
	   			for(String title : items)
	    		{
	   				if(title.equals("Se7en"))
	   				{
	   					keys.add(Utils.MEDIAITEM + Utils.Hash + title);
	   				}
	    		}
	    			
	   			if(!items.isEmpty()) keys.add(Utils.MEDIAITEM);
	   			
	   			if(!keys.isEmpty()) 
	        	{
	        		String[] keysToDelete = new String[keys.size()];
	            	keys.toArray(keysToDelete);
	            	jedis.del(keysToDelete);
	        	}
			}
	    	catch (Exception e) 
	    	{ 
				e.printStackTrace(); 
				return false;
			}
	    	
	    	return true;
	    }
	
	
	
}