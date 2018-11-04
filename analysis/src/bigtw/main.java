package bigtw;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;


public class main {
	static final String DB_URL = "jdbc:mysql://211.23.17.100/itravel?useUnicode=yes&characterEncoding=UTF-8";
	
	static final String USER = "root";
	static final String PASS = "imacwebteammysql9457songyy";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf();
		conf.setAppName("JavaWordCount");
		conf.setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Calendar c = Calendar.getInstance();
		SimpleDateFormat dateFormatter = new SimpleDateFormat("y/M/d");
		String yearMonthDay = dateFormatter.format(c.getTime());
		c.add(Calendar.DAY_OF_MONTH, -1);
		dateFormatter = new SimpleDateFormat("y");
		String year =dateFormatter.format(c.getTime());
		dateFormatter = new SimpleDateFormat("y-M");
		String yearMonth = dateFormatter.format(c.getTime());

				
		ArrayList<String> pointList =new ArrayList<>();
		ArrayList<String> yearNewsList = new ArrayList<>();
		ArrayList<String> monthNewsList = new ArrayList<>();
		ArrayList<String> dayNewsList = new ArrayList<>();
		ArrayList<String> previouslyDataList = new ArrayList<>();
		HashMap<String, Integer> previouslyDataMap = new HashMap<>();
		try{
			Class.forName("com.mysql.jdbc.Driver");  
			Connection con=DriverManager.getConnection(  
					DB_URL, USER, PASS);
			Statement stmt=con.createStatement();  
			ResultSet rs=stmt.executeQuery("select * from all_point");
			
			while(rs.next()) {
				pointList.add(rs.getString(2));
			}
			
			rs = stmt.executeQuery("select * from news where time like '"+year+"%'");
			while(rs.next()) {
				yearNewsList.add(rs.getString("content"));
			}

			rs = stmt.executeQuery("select * from news where time like '"+yearMonth+"%'");
			while(rs.next()) {
				monthNewsList.add(rs.getString("content"));
			}
			
			//抓取離目前最近的歷年資料
			rs = stmt.executeQuery("select time from all_rank group by time");
			rs.last();
			String previousDataDay = rs.getString("time");
			rs = stmt.executeQuery("select * from all_rank where time = '"+previousDataDay+"'");
			while(rs.next()) {
				previouslyDataList.add(rs.getString("name"));
				previouslyDataMap.put(rs.getString("name"), rs.getInt("value"));
			}
			con.close();
		}catch(Exception e) {
			System.out.println(e);
		}

//		//將每月所有資料進行tuple統整
		JavaPairRDD<String, Integer> monthFlatMapPair = sc.parallelize(monthNewsList).flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

			@Override
			public Iterator<Tuple2<String, Integer>> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Tuple2<String, Integer>> result = new ArrayList();
				for(String i : pointList) {
					if(arg0.contains(i)) {
						Tuple2<String, Integer> tuple =new Tuple2<String, Integer>(i, 1);
						result.add(tuple);
					}	
				}
				return result.iterator();
			}
		});
		monthFlatMapPair.collect();
		
		JavaPairRDD<String, Integer> monthReduceByKey = monthFlatMapPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		});	
		
		List<Tuple2<String, Integer>> monthTuple = monthReduceByKey.collect();
//		
//		//將每年所有資料進行tuple統整
		JavaPairRDD<String, Integer> yearFlatMapPair = sc.parallelize(yearNewsList).flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

			@Override
			public Iterator<Tuple2<String, Integer>> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Tuple2<String, Integer>> result = new ArrayList();
				for(String i : pointList) {
					if(arg0.contains(i)) {
						Tuple2<String, Integer> tuple =new Tuple2<String, Integer>(i, 1);
						result.add(tuple);
					}	
				}
				return result.iterator();
			}
		});
		JavaPairRDD<String, Integer> yearReduceByKey = yearFlatMapPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		});
		
		List<Tuple2<String, Integer>> yearTuple = yearReduceByKey.collect();
		
//		//將資料匯入資料庫
		try{
			Class.forName("com.mysql.jdbc.Driver");  
			Connection con=DriverManager.getConnection(  
					DB_URL, USER, PASS);
			Statement stmt=con.createStatement();
			//每月資料
			for(Tuple2<String, Integer> i : monthTuple) {

				stmt.executeUpdate("Insert into months_rank (time, name, value) Values ('"+yearMonthDay+"', '"+i._1+"', "+i._2+")");				
			}
//			//每年資料
			for(Tuple2<String, Integer> i : yearTuple) {
				stmt.executeUpdate("Insert into years_rank (time, name, value) Values ('"+yearMonthDay+"', '"+i._1+"', "+i._2+")");		
			}
			//歷年資料
			for (Tuple2<String, Integer> i : monthTuple){
				if(previouslyDataMap.containsKey(i._1)) {
					stmt.executeUpdate("Insert into all_rank (time, name, value) Values ('"+yearMonthDay+"', '"+i._1+"', '"+(i._2 + previouslyDataMap.get(i._1))+"')");		
					previouslyDataMap.remove(i._1);
				}else {
					stmt.executeUpdate("Insert into all_rank (time, name, value) Values ('"+yearMonthDay+"', '"+i._1+"', '"+i._2+"')");		
				}
			}
			for(int i=0;i<previouslyDataList.size();i++) {
				if(previouslyDataMap.size()==0) {
					break;
				}else if(previouslyDataMap.containsKey(previouslyDataList.get(i))) {
					stmt.executeUpdate("Insert into all_rank (time, name, value) Values ('"+yearMonthDay+"', '"+previouslyDataList.get(i)+"', '"+previouslyDataMap.get(previouslyDataList.get(i))+"')");		
					previouslyDataMap.remove(previouslyDataList.get(i));
					continue;
				}
			}
		con.close();
		}catch(Exception e) {
			System.out.println(e);
		}
	}
}
