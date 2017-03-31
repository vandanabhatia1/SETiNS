import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import org.bson.BasicBSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;


public class finalresult {
	
	
	public static void store(String dbName, String colName) throws MongoException, IOException {

		
		
		
		String filePath="/home/hduser/workspace/DeDup/forcomp/"+colName;
		
			File f=new File(filePath);
			
			FileWriter fw=new FileWriter(f);
			
		
			Mongo m = new Mongo();
			
					
			DB db= m.getDB(dbName);
			
			
			DBCollection col1= db.getCollection(colName);
			
				DBCursor cursor = col1.find();
				while (cursor.hasNext()) {
					
				    BasicDBObject studentObj = (BasicDBObject) cursor.next();
				    
				   // System.out.println(studentObj);
				    
			        String _id= studentObj.getString("_id");
			        String _value = studentObj.getString("value");
			        
				String s= _id
						+ " " + _value;
					    
				fw.write(s+"\r\n");
				
					
					
					
					
				}
				
				
			
			
			fw.close();
			
			m.close();
		}
	
	public static void storeMain(String dbName, String colName) throws MongoException, IOException {

		
	
	String filePath="/home/hduser/workspace/DeDup/forcomp/pointerfile";
	
		File f=new File(filePath);
		
		FileWriter fw=new FileWriter(f);
		
	
		Mongo m = new Mongo();
		
				
		DB db= m.getDB(dbName);
		
		
		DBCollection col1= db.getCollection(colName);
		
		
		
		
			
			//BasicDBList studentsList = (BasicDBList) o.ge;
		    //for (int i = 0; i < studentsList.size(); i++) {
		    
			
			
		
			DBCursor cursor = col1.find();
			while (cursor.hasNext()) {
				
			    BasicDBObject studentObj = (BasicDBObject) cursor.next();
			    
			   // System.out.println(studentObj);
			    
		        String _id= studentObj.getString("_id");
		        String _name = studentObj.getString("name");
		        String _class = studentObj.getString("class");
		        String _city = studentObj.getString("city");
		        String _branch = studentObj.getString("branch");
		        String _rollno = studentObj.getString("rollno");
		        
			String s= _id
					+ " " + _name
				    + " " + _class
					+ " " + _city
					+ " " + _branch
					+ " " + _rollno;
			
			fw.write(s+"\r\n");
			
				
				
				
				
			}
			
			
		
		
		fw.close();
		
		m.close();
	}
	
	
	public static void main(String [] args) throws MongoException, IOException {
	
		
		String dbName="studb10";
		String colNameMain="stu";
		
		String [] fields= {"_id","name","class","city","branch","rollno"};
		
		storeMain(dbName,colNameMain);
		
		for(int i=1;i<fields.length;i++) {
			
			store(dbName,fields[i]);
			
		}
		
		
		
		
	}
	
	
}
		
		
	

	
	
	
	





