/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;
import java.util.Comparator;

import javax.servlet.http.HttpServletResponse;
import java.util.Set;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bgu.ise.ddb.Utils;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{
	
	
	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")    String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
		System.out.println(username+" "+title);
		//:TODO your implementation
		
		String key = Utils.HISTORY_PRE+ title + "#" + username;
		long tmestmp = System.currentTimeMillis();
		
		HttpStatus status = HttpStatus.CONFLICT;
		
		try(Jedis jedis = new Jedis(Utils.DB_address))
		{
			//will check if both username and title exist
			 if(jedis.sismember("Users", username) && jedis.sismember("MediaItems", title)) {  //check if both username and movie title exist in the DB
				 Transaction trans = jedis.multi();   //open transaction
				 trans.hset(key, "username", username);  //set username
				 trans.hset(key, "title", title);       //set title
				 trans.hset(key, "time_stamp", Long.toString(tmestmp));   //set log timestamp
				 
				 
				 trans.sadd(Utils.HISTORY_BY_USER+ Utils.Hash + username, title); // will to to table History by users
				 trans.sadd(Utils.HISTORY_BY_ITEM+ Utils.Hash + title, username);   // will to to table History by items
				 

				 
				 trans.exec();
				 status = HttpStatus.OK;
				
			 }
		}
		
		catch (Exception exc) { exc.printStackTrace(); }
		
		response.setStatus(status.value());
	}
	
	
	
	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		//:TODO your implementation
		HistoryPair[] result = null;
		ArrayList<HistoryPair> hist = new ArrayList<>();
		try(Jedis jedis = new Jedis(Utils.DB_address))
		{ 
			
			if(!jedis.sismember("Users", username)) {
				//check if user exist
				return result;
			}
			
			// get the list of movies the user watched
			Set<String> movie_hist = jedis.smembers(Utils.HISTORY_BY_USER + Utils.Hash + username);
			
			

			for(String movie : movie_hist) {
				
				System.out.println(movie);
				
				// get the timestamp of each log
				String log = jedis.hget(Utils.HISTORY + Utils.Hash + movie + Utils.Hash + username, "time_stamp");
				long tmestemp = Long.parseLong(log);
				Date timeStamp = new Date(tmestemp);
				HistoryPair histp = new HistoryPair(movie, timeStamp);
				hist.add(histp);
			}
			
			// will get it in descending order
			hist.sort(Comparator.comparing(HistoryPair::getViewtime).reversed());

			
			// convert the list to an array
			result = hist.toArray(new HistoryPair[hist.size()]);
		
		}
		catch (Exception exc) { exc.printStackTrace(); }
		int i = 0;
//		for (HistoryPair histp: result) {
//			System.out.println("ByUser "+result[0]);
//			i++;
//		}
		return result;

	}
	
	
	

	

	
	
	
	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		
		HistoryPair[] result = null;
		ArrayList<HistoryPair> hist = new ArrayList<>();
		try(Jedis jedis = new Jedis(Utils.DB_address))
		{
			
			// get the list of users watched the movie
			Set<String> user_hist = jedis.smembers(Utils.HISTORY_BY_ITEM + Utils.Hash + title);
		
			
			for(String user : user_hist) {
				

				// get the timestamp of each log
				String log = jedis.hget(Utils.HISTORY + Utils.Hash + title + Utils.Hash + user, "time_stamp");
				long tmestemp = Long.parseLong(log);
				Date timeStamp = new Date(tmestemp);
				HistoryPair histp = new HistoryPair(user, timeStamp);
				hist.add(histp);
			}
			
			// will get it in descending order
			hist.sort(Comparator.comparing(HistoryPair::getViewtime).reversed());
			// convert the list to an array
			result = hist.toArray(new HistoryPair[hist.size()]);

			
		}
		
		catch (Exception exc) { exc.printStackTrace(); }
		int i = 0;
//		for (HistoryPair histp: result) {
//			System.out.println("ByItem "+result[0]);
//			i++;
//		}
		return result;	
		

	}
	
	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		//:TODO your implementation
		
		User users[]= null;
		try(Jedis jedis = new Jedis(Utils.DB_address))
		{
			if(!jedis.sismember("Mediaitems", title)) {
				return users;    //if the movie title does not exist!
			}
			
			Set<String> user_hist = jedis.smembers(Utils.HISTORY_BY_ITEM + Utils.Hash + title);
			users = new User[user_hist.size()];
			
			int i =0;
			for(String user : user_hist) {
				
				
				String f_name = jedis.hget(Utils.USER_PRE+user, "username");
				String l_name = jedis.hget(Utils.USER_PRE+user, "lastname");

				users[i] = new User(user, f_name, l_name);
				System.out.println(users[i]);
				i++;
			}
			
			
			
			return users;
			
		}
		
		catch (Exception exc) { exc.printStackTrace(); }
		
		
		return users;
	}
	
	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){
		
		double intersection = 0.0;
		int union = 0;
		try(Jedis jedis = new Jedis(Utils.DB_address))
		
		{
			if(!jedis.sismember(Utils.MEDIAITEM, title1) || !jedis.sismember(Utils.MEDIAITEM, title2)) {
				return 0.0;}    // if one of the movies title does not exist!
			
			Set<String> usernames_title1 = jedis.smembers(Utils.HISTORY_BY_ITEM + Utils.Hash + title1);  // Retrieve the list of user watched movie 1
			Set<String> usernames_title2 = jedis.smembers(Utils.HISTORY_BY_ITEM + Utils.Hash + title2); // Retrieve the list of user watched movie 2
			
			union = usernames_title2.size();
			
			if(union == 0 && usernames_title1.size() == 0) return 0.0;
			
			for(String username_title1 : usernames_title1)
			{
				if(usernames_title2.contains(username_title1))
				{
					intersection++;
				}
				else
				{
					union++;
				}
			}
		}
		catch (Exception e) { e.printStackTrace(); }
		
		return intersection / union;

	}
	

}
