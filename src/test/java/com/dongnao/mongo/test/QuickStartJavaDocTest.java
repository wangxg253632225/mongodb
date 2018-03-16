package com.dongnao.mongo.test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;


import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;


public class QuickStartJavaDocTest {

	private static final Logger logger = LoggerFactory.getLogger(QuickStartJavaDocTest.class);
	
	private MongoDatabase db;
	private MongoCollection<Document> doc;
	private MongoClient client ;
	
	@Before
	public void init(){
//		client = new MongoClient("192.168.1.104",27022);
		client = new MongoClient("10.73.10.144",27022);
		db = client.getDatabase("lison");
		doc = db.getCollection("users");
	}
	
	@Test
	public void insertDemo() {
		Document doc1 = new Document();
		doc1.append("username", "lison");
		doc1.append("country", "USA");
		doc1.append("age", 20);
		doc1.append("length", 1.77f);
		doc1.append("salary", new BigDecimal("6523.2"));
		Map<String,Object> address1 = new HashMap<String,Object>();
		address1.put("aCode", "0000");
		address1.put("add", "xxx000");
		doc1.append("address", address1);
		Map<String,Object> favorites1 = new HashMap<String, Object>();
		favorites1.put("movies", Arrays.asList("aa","bb"));
		favorites1.put("cites", Arrays.asList("东莞","东京"));
		doc1.append("favorites", favorites1);
		
		Document doc2 = new Document();
		doc2.append("username", "chen");
		doc2.append("country", "China");
		doc2.append("age", 30);
		doc2.append("length", 1.77f);
		doc2.append("salary", new BigDecimal("5555.5"));
		Map<String,Object> address2 = new HashMap<String,Object>();
		address2.put("aCode", "411000");
		address2.put("add", "我的地址2");
		doc2.append("address", address2);
		Map<String,Object> favorites2 = new HashMap<String, Object>();
		favorites2.put("movies", Arrays.asList("东游记","西乡塘"));
		favorites2.put("cites", Arrays.asList("珠海","广州"));
		doc2.append("favorites", favorites2);
		
		doc.insertMany(Arrays.asList(doc1,doc2));
		
	}
	
//	@Test
	public void testDelete(){
		
		//delete from users where username="lison"
//		doc.deleteMany(Filters.eq("username"));
		DeleteResult deleteMany1 = doc.deleteMany(eq("username","lison"));
		logger.info(String.valueOf(deleteMany1.getDeletedCount()));
		
		//delete from users where age > 8 and age <25 
		DeleteResult deleteMany2 = doc.deleteMany(and(gt("age",8),lt("age",25)));
		logger.info(String.valueOf(deleteMany2.getDeletedCount()));
		
	}
	
	@Test
	public void testUpdate(){
		// update users set age=6 where username='lison'
		UpdateResult udpateResult = doc.updateMany(eq("username","lison"), 
				new Document("$set",new Document("age",6)));
		logger.info(String.valueOf(udpateResult.getModifiedCount()));
		
		//update users set favorites.movies add "小电影2"，"小电影3" where favorites.cites has '东莞'
		UpdateResult updateResult2 = doc.updateMany(eq("favorites.cites","东莞"), 
				addEachToSet("favorites.movies", Arrays.asList("小电影2","小电影3")));
		logger.info(String.valueOf(updateResult2.getModifiedCount()));
		
	}
	
	@Test
	public void testFind(){
		//select * from users where favorites.movies has '东莞'、'东京'
		final List<Document> ret = new ArrayList<Document>();
		Block<Document> blockPrint = new Block<Document>(){
			public void apply(Document t) {
//				logger.info(t.toJson());
				ret.add(t);
			}
		};
		
		FindIterable<Document> find = doc.find(all("favorites.movies", Arrays.asList("东莞","东京")));
		find.forEach(blockPrint);
		logger.info(String.valueOf(ret.size()));
		ret.removeAll(ret);
		
		//select * from users where username like '%s%' and (country=English or country=USA )
		
		String regexStr = ".*s.*";
		Bson regex = regex("username", regexStr);
		Bson or = or(eq("country","English"),eq("country","USA"));
		
		FindIterable<Document> find2 = doc.find(and(regex,or));
		find2.forEach(blockPrint);
		logger.info(String.valueOf(ret.size()));
		
		
		
		
	}
	
}
