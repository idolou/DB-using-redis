/**
 * 
 */
package org.bgu.ise.ddb.registration;


import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Set;
import javax.servlet.http.HttpServletResponse;
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
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import org.bgu.ise.ddb.items.ItemsController;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{	
	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		//:TODO your implementation
		HttpStatus status = HttpStatus.OK;
		try(Jedis jedis = new Jedis(Utils.DB_address))
		{
			if(isExistUser(username)) {
				status = HttpStatus.CONFLICT;
				
			}
			else {
			Transaction trans = jedis.multi();
			trans.hset("Users#"+username, "password", password);
			trans.hset("Users#"+username, "lastName", lastName);
			trans.hset("Users#"+username, "firstName", firstName);

			
			trans.sadd("Users", username);


			trans.zadd("Users_reg_stamp", System.currentTimeMillis(), username);
			trans.exec();		
			
			status = HttpStatus.OK;
		
			}
		}

		catch (Exception e) { e.printStackTrace(); }


		response.setStatus(status.value());
		
	}
	
	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean result = false;
		//:TODO your implementation
		try(Jedis jedis = new Jedis(Utils.DB_address))
		{
			result = jedis.sismember("Users", username);
		}
		catch (Exception exp) {
			exp.printStackTrace(); }
				
		return result;
		
	}
	
	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		//:TODO your implementation
		try(Jedis jedis = new Jedis(Utils.DB_address))
		{
			if(isExistUser(username)) {
				result = new String(jedis.hget(Utils.USER_PRE + username, "password")).equals(password);
			}
			
		}
		catch (Exception exp) {
			exp.printStackTrace(); }
				
		return result;
		
	}
	
	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		int result = 0;
		//:TODO your implementation
		
	       
	    	
	    	try(Jedis jedis = new Jedis(Utils.DB_address))
			{
	    		GregorianCalendar dateBeforeNdays = new GregorianCalendar();
	    		Calendar calendar = Calendar.getInstance();
	            dateBeforeNdays.setTime(calendar.getTime());  ///get current time
	            dateBeforeNdays.add(Calendar.DATE, -days);      ///get it minus n days
	            
	           
	            
	           
	    		Set<String> users_reg = jedis.zrangeByScore("Users_reg_stamp", dateBeforeNdays.getTimeInMillis() , Double.POSITIVE_INFINITY);
	    		result = users_reg.size();   ///will give all the users that they're time stamp reg is between current time to minus n days
	    		
			}
			catch (Exception exp) {
				exp.printStackTrace(); }
					

		
		
		return result;
		
	}
	
	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public  User[] getAllUsers(){
		//:TODO your implementation
		User u = new User("alex", "alex", "alex");
		try(Jedis jedis = new Jedis(Utils.DB_address))
		{
			Set<String> users = jedis.smembers("Users");   //create list of all users exist
			User[] users_arr = new User[users.size()];  //init new array in the size of list
			
			int i = 0;
			for(String username : users)
			{
				String password = jedis.hget("Users#" + username, "password");
				String firstName = jedis.hget("Users#" + username, "firstName");
				String lastName = jedis.hget("Users#" + username, "lastName");

				users_arr[i] = new User(username, password, firstName, lastName);
				System.out.println(users_arr[i]);
				i++;
			}
			System.out.println(jedis.info());
			return users_arr;
		}
		catch(Exception exc)
		{exc.printStackTrace();}
		
		//database
		
		return new User[] {u};
	}

}
