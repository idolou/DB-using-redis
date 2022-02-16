/**

 * 
 */
package org.bgu.ise.ddb.items;

import java.io.IOException;

import org.bgu.ise.ddb.Utils;


import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;

import java.sql.SQLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;




import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	
	
	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response){
		System.out.println("was here");
		//:TODO your implementation
		String q = "SELECT TITLE,PROD_YEAR FROM MediaItems";
		//========= clean table =========
		Utils.deleteItemsTable();
		HttpStatus status = HttpStatus.CONFLICT;
		try(Jedis jedis = new Jedis(Utils.DB_address))
		{
			Class.forName(Utils.ORACLE_URL);
			//======= connection to oracle =======
			
			

			try(Connection connection = DriverManager.getConnection(Utils.connectionURL,Utils.ORACLE_USER,Utils.ORACLE_PASS);
				// ==== query execute ====
				PreparedStatement ps = connection.prepareStatement(q);
				ResultSet qResult = ps.executeQuery())
			{
				// Begin Transaction
				Transaction transaction = jedis.multi();
				// while we have more in the set qResult 
				while(qResult.next())
				{
					String title = qResult.getString(1);
					int prod_year = qResult.getInt(2);

					
					transaction.hset(Utils.MEDIAITEM + Utils.Hash + title, "prod_year", "" + prod_year); //key + value 
					transaction.sadd(Utils.MEDIAITEM, title); //add title with mediaItem key
				}
				transaction.exec();
				status = HttpStatus.OK;
			}
			catch(Exception e) { e.printStackTrace(); }	
		}
		catch (Exception e) { e.printStackTrace(); }
		
		
		response.setStatus(status.value());
	}
	
	

	
	/**
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);
		//the function get csv file and add the values to our web
		//:TODO your implementation
		Utils.deleteItemsTable(); // clean table
		try(Jedis jedis = new Jedis(Utils.DB_address);
			Scanner csv_scanner = new Scanner(new URL(urladdress).openStream()))
		{
			int titleIndex = 0;
			int prodYearIndex = 1;
			Transaction t = jedis.multi();
            System.out.printf("|%70s|%5s|\n", "Title                                   ", "Year");
            System.out.printf("|%70s|%5s|\n", "______________________________________________________________________", "_____");

			while(csv_scanner.hasNext())
			{
				try
				{
					String[] data = csv_scanner.nextLine().split(","); //in the csv file we have title and year in 2 columns
					if(data == null || data.length != 2) continue; // Checks if there are no more than 2 columns
							
					String title = data[titleIndex];
					int prod_year = Integer.parseInt(data[prodYearIndex]);

					t.sadd(Utils.MEDIAITEM, title);
					t.hset(Utils.MEDIAITEM + Utils.Hash + title, "prod_year", "" + prod_year);
		            System.out.printf("|%70s|%5s|\n", title, prod_year);
					//System.out.println("Title: "+title+"  Year : "+prod_year);

				}
				catch(Exception e) { e.printStackTrace(); }
			}
			t.exec();
		}
		catch(Exception e) { e.printStackTrace(); }
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	

	

	
	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){
		//:TODO your implementation
//		MediaItems m = new MediaItems("Game of Thrones", 2011);
//		System.out.println(m);
//		return new MediaItems[]{m};
		
		try(Jedis jedis = new Jedis(Utils.DB_address))
		{
			ArrayList<MediaItems> list = new ArrayList<>();
			Iterator<String> items_iterator = jedis.smembers(Utils.MEDIAITEM).iterator();
			
			while(list.size() < topN && items_iterator.hasNext())
			{
				try
				{
					String title = items_iterator.next();
					int prod_year = Integer.parseInt(jedis.hget(Utils.MEDIAITEM + Utils.Hash + title, "prod_year"));
					//System.out.println("I = [" + title + "," + prod_year + "]");
					list.add(new MediaItems(title, prod_year));
				}
				catch(Exception e) { e.printStackTrace(); }
			}
			
			MediaItems[] result = new MediaItems[list.size()];
			for(int i = 0; i < result.length; i++)
			{
				result[i] = list.get(i);
			}
			
			return result;
		}
		catch (Exception e) { e.printStackTrace(); }
		
		return new MediaItems[]{};
	}

		

}