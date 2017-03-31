import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Date;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;


public class FileToMongoDB {

	
	public static boolean storeMain(String dbName,String filePath,String colName,String [] fields) {
		
		boolean result=false;
		
		try {
			
			//Configuration conf = new Configuration();
			//FileSystem fs = FileSystem.get(conf);
			//System.out.println(fs.getUri());
			//Path file = new Path( "/home/hduser/workspace/DeDup/field1/part-00000.txt", "/home/hduser/workspace/DeDup/field2/part-00000.txt");
			
			Mongo m = new Mongo();
			DB db= m.getDB(dbName);
			DBCollection col1= db.getCollection(colName);
			
			
			
			//if (fs.exists(file)) {
			//	System.out.println("File exists.");
				// "/home/hduser/workspace/DeDup/field2/part-00000"
				File f=new File(filePath);
				
				BufferedReader br= new BufferedReader(new FileReader(f));
				
				String s=null;
				int i=0;
				BasicDBObject document=null;
				while((s=br.readLine())!=null) {
					
					i++;
					
					String [] p=s.split("\\s+");
					
					
					document = new BasicDBObject();
					
					
					for(String fld: fields) {
									
						if(fld.equals("_id")) {
							document.put(fld,p[0]);
						} else {
							document.put(fld,"x");
						}
					
					}
					
					col1.insert(document);
									
					
				}
						
				
			
				br.close();
				result=true;
				
				m.close();
				
			
			//}
			
			} catch(Exception e) {
				System.out.println(e);
			}
		
		return result;
		
	}
	
	
	
	public static boolean  store(String mtableName, String dbName,String filePath,String colName) {
		
		boolean result=false;
		
		try {
			
			//Configuration conf = new Configuration();
			//FileSystem fs = FileSystem.get(conf);
			//System.out.println(fs.getUri());
			//Path file = new Path( "/home/hduser/workspace/DeDup/field1/part-00000.txt", "/home/hduser/workspace/DeDup/field2/part-00000.txt");
			
			Mongo m = new Mongo();
			DB db= m.getDB(dbName);
			
			DBCollection col1= db.getCollection(colName);
			col1.remove(new BasicDBObject());
			
			DBCollection colm= db.getCollection(mtableName);  ///?
			
			col1.remove(new BasicDBObject());
			
			//if (fs.exists(file)) {
				
				// "/home/hduser/workspace/DeDup/field2/part-00000"
				File f=new File(filePath);
				
				BufferedReader br= new BufferedReader(new FileReader(f));
				
				String s=null;
				int i=0;
				while((s=br.readLine())!=null) {
					
					i++;
					
					String [] p=s.split("\\s+");
					
					
					BasicDBObject document = new BasicDBObject();
					document.put("_id",i);
					document.put("value",p[0]);
					col1.insert(document);
					
					System.out.println("i:"  + i + " p length: " + p.length);	
					
					int recId=0;
					
					BasicDBObject newDocument = new BasicDBObject();
					newDocument.append("$set", new BasicDBObject().append(colName, String.valueOf(i)));
					
					for(int t=1;t<p.length;t++) {
						
						recId=Integer.parseInt(p[t]);
						System.out.println("\trecId: " + recId);
						
						
					 
						BasicDBObject searchQuery = new BasicDBObject().append("_id", String.valueOf(recId));
					 
						WriteResult wr=colm.update(searchQuery, newDocument);
						
						System.out.println("Success: " + wr.getN());
						
					}
					
					
				}
						
				
			
				br.close();
				result=true;
				

				m.close();
			
			//}
			
			} catch(Exception e) {
				System.out.println(e);
			}
		
		return result;
		
	}
	
	public static void main(String [] args) {
		
		String dbName="studb10";
				
		String mtable="/home/hduser/workspace/DeDup/input/copy";
		
		String [] fields= {"_id","name","class","city","branch","rollno"};
		
		String mtableName="stu";
		
		boolean r2=storeMain(dbName,mtable,mtableName,fields);
		
		System.out.println("r2: " + r2);
		
		
		String name="/home/hduser/workspace/DeDup/name/part-00000";
		String classes="/home/hduser/workspace/DeDup/class/part-00000";
		String city="/home/hduser/workspace/DeDup/city/part-00000";
		String Branch="/home/hduser/workspace/DeDup/branch/part-00000";
		String Rollno="/home/hduser/workspace/DeDup/rollno/part-00000";
		
		boolean r=store(mtableName,dbName,name,fields[1]);
		r=store(mtableName,dbName,classes,fields[2]);
		r=store(mtableName,dbName,city,fields[3]);
		r=store(mtableName,dbName,Branch,fields[4]);
		r=store(mtableName,dbName,Rollno,fields[5]);
		
		
		System.out.println("r: " + r);
		
		
		
		
		
		
		
		
		
		
		
		
		
		
	}

}
