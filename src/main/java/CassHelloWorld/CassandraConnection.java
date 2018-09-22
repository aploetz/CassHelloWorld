package CassHelloWorld;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ConsistencyLevel;
	
public class CassandraConnection {
	private Cluster cluster;
	private Session session;

    public CassandraConnection() {	
    }
	
    public CassandraConnection(String node, String user, String pwd, String dc) {
      connect(node,user,pwd,dc);
    }
    
    public void connect(String node, String user, String pwd, String dc) {
      QueryOptions qo = new QueryOptions();
      qo.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
      
      cluster = Cluster.builder()
        .addContactPoint(node)
        .withCredentials(user,pwd)
        .withQueryOptions(qo)
        .withLoadBalancingPolicy(
          new TokenAwarePolicy(
       	    DCAwareRoundRobinPolicy.builder()
       		.withLocalDc(dc)
       		.build()
        ))
        .build();
      session = cluster.connect();
    }

    public ResultSet query(String strQuery) {
      return session.execute(strQuery);
    }
    	
    public void close() {
      cluster.close();
    }
    
    public ResultSet query(BoundStatement bStatement) {
	  return session.execute(bStatement);
	}
		
	public void insert(BoundStatement bStatement) {
	  session.execute(bStatement);
	}
		
	public Session getSession() {
	  return session;
	}
}
