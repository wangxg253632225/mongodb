package com.dongnao.mongo.test;

import static com.mongodb.client.model.Filters.all;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Updates.addEachToSet;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dongnao.mongodb.entity.Address;
import com.dongnao.mongodb.entity.Favorites;
import com.dongnao.mongodb.entity.User;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import  com.mongodb.client.result.DeleteResult.*;
import  com.mongodb.client.result.UpdateResult.*;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;


public class QuickStartJavaPojoTest {

	private static final Logger logger = LoggerFactory.getLogger(QuickStartJavaPojoTest.class);
	
	private MongoDatabase db;
	private MongoCollection<User> doc;
	private MongoClient client ;
	
	@Before
	public void init(){
		//生成List编解码器
		List<CodecRegistry> codecRegistry = new ArrayList<CodecRegistry>();
		//将默认的编解码器加入到List解码器中
		codecRegistry.add(MongoClient.getDefaultCodecRegistry());
		//生成一个pojo的编码器
		CodecRegistry pojoRegistry = CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build());
		//通过编解码器List生成编解码器注册中心
		codecRegistry.add(pojoRegistry);
		
		//生成注册中心
		CodecRegistry registry = CodecRegistries.fromRegistries(codecRegistry);
		
		//把编解码器注册中心加入到MongoClientOption中
		ServerAddress serverAddr = new ServerAddress("192.168.1.104",27022);
		MongoClientOptions mongoOptions = MongoClientOptions.builder().codecRegistry(registry).build();
		
		client = new MongoClient(serverAddr, mongoOptions);
		
//		client = new MongoClient("192.168.1.104",27022);
//		doc = db.getCollection("users");
		db = client.getDatabase("lison");
		doc = db.getCollection("users", User.class);
	}
	
	@Test
	public void insertDemo() {
		
		User user1 = new User();
		user1.setUsername("lison");
		user1.setCountry("USA");
		user1.setAge(20);
		user1.setLenght(1.77f);
		user1.setSalary(new BigDecimal("11112.2"));
		Address address = new Address();
		address.setaCode("0000");
		address.setAdd("xxx0000");
		user1.setAddress(address);
		Favorites favorites = new Favorites();
		favorites.setCites(Arrays.asList("东莞","东京"));
		favorites.setMovies(Arrays.asList("东游记","西乡塘"));
		user1.setFavorites(favorites);
		
		User user2 = new User();
		user2.setUsername("chen");
		user2.setCountry("china");
		user2.setAge(30);
		user2.setLenght(1.87f);
		user2.setSalary(new BigDecimal("2342.2"));
		Address address2 = new Address();
		address2.setaCode("2222");
		address2.setAdd("xxx2222");
		user2.setAddress(address2);
		Favorites favorites2 = new Favorites();
		favorites2.setCites(Arrays.asList("南宁","南京"));
		favorites2.setMovies(Arrays.asList("肉蒲团","一剪梅"));
		user2.setFavorites(favorites2);
		
		doc.insertMany(Arrays.asList(user1,user2));
		
	}
	
	@Test
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
		final List<User> ret = new ArrayList<User>();
		Block<User> blockPrint = new Block<User>(){
			public void apply(User t) {
				logger.info(t.toString());
				ret.add(t);
			}
		};
		
		FindIterable<User> find = doc.find(all("favorites.movies", Arrays.asList("东莞","东京")));
		find.forEach(blockPrint);
		logger.info(String.valueOf(ret.size()));
		ret.removeAll(ret);
		
		//select * from users where username like '%s%' and (country=English or country=USA )
		
		String regexStr = ".*s.*";
		Bson regex = regex("username", regexStr);
		Bson or = or(eq("country","English"),eq("country","USA"));
		
		FindIterable<User> find2 = doc.find(and(regex,or));
		find2.forEach(blockPrint);
		logger.info(String.valueOf(ret.size()));

		
		
		
		
		
	}
	
}
