package CassHelloWorld;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class CassHelloWorld {

	public static void main(String[] args) {
		CassandraConnection conn = new CassandraConnection();
		conn.connect("192.168.0.101", "cassdba", "flynnLives", "ClockworkAngels");
		
		String strSELECT ="SELECT cluster_name,data_center,"
                   + "listen_address,release_version,dateof(now()) "
				   + "FROM system.local WHERE key='local'";
		ResultSet rows = conn.query(strSELECT);
		
		System.out.println("Hello from:");
		for (Row row : rows) {
		    System.out.print(
                  row.getString("cluster_name") + " " +
                  row.getString("data_center") + " " +
		    	  row.getString("release_version") + "\n" +
                  row.getTimestamp("system.dateof(system.now())")
                  );
		}		
		conn.close();
	}
}
