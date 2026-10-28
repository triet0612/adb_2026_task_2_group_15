import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class NoLiveLocks {
    public static int num_taks=30;
	public static int num_queries_per_task = 3;
	public static int num_of_threads = 4;
	
	static Connection con;
	Random rand = new Random(123456);
	int num_tuple = 50000;
	static double max_value = 50.0f;
	AtomicInteger tasks_finished;
	static int timeout = 0;
	
	public NoLiveLocks(){
		tasks_finished = new AtomicInteger(0);
	}
	
	void executeSQL(String sql, Connection con){
		java.sql.Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.execute(sql);
			System.out.println("Executing "+sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	void cerateDB(){	
		try {
			Connection con = ConnectionFactory.getDefaultParameterConnection();
			executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
			executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);
			
			PreparedStatement ps;
			con.setAutoCommit(false);
			ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			for(int i=0;i<num_tuple;i++){
				ps.setInt(1, i);
				ps.setDouble(2,rand.nextDouble()*max_value);
				ps.setInt(3, 0);
				ps.addBatch();
				ps.clearParameters();
			}
			
			int[] results = ps.executeBatch();
			con.commit();
			con.close();
			// System.out.println(Arrays.toString(results));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		//Create DB
		NoLiveLocks b = new NoLiveLocks();
		b.cerateDB();

		//Create task and their queries
		ArrayList<PredeclareBlocker> my_tasks = new ArrayList<PredeclareBlocker>();
		double start, stop;

		for(int task=0;task<num_taks;task++){
			my_tasks.add(b.new PredeclareBlocker(task));
		}
		
		//Execute tasks
		ThreadPoolExecutor executor = 
				  (ThreadPoolExecutor) Executors.newFixedThreadPool(num_of_threads);//XXX - Do not change the number of threads used

		start = System.currentTimeMillis();

		for (PredeclareBlocker task : my_tasks) {
			executor.execute(task);
		}
		executor.shutdown();
		
		//wait for the tasks to finish
		while(!executor.isTerminated()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		stop = System.currentTimeMillis();
		System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms finished: "+b.tasks_finished+" of "+num_taks);
	}

	public class PredeclareBlocker implements Runnable {
		final int task;
		final double [] start_range;
		final double [] stop_range;
		
		public PredeclareBlocker(int id){
			this.task = id;
			this.start_range = new double[num_queries_per_task];
			this.stop_range  = new double[num_queries_per_task];
			for(int q=0;q<num_queries_per_task;q++) {
				stop_range[q]  = rand.nextDouble()*max_value;
				start_range[q] = rand.nextDouble()*stop_range[q];
			}
		}
		@Override 
		public void run() {
			double start = System.currentTimeMillis();
			try {
				Connection con = ConnectionFactory.getDefaultParameterConnection();
				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				PreparedStatement ps = con.prepareStatement("LOCK TABLE blocking_data IN EXCLUSIVE MODE;");
				ps.execute();
				ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = "+task+" WHERE data_value >= ? AND data_value <= ?",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
				for (int q = 0; q<num_queries_per_task; q++) {
					ps.setDouble(1, (float) (rand.nextDouble()*max_value));
					ps.setDouble(2, start_range[q]);
					ps.setDouble(3, stop_range[q]);
					ps.executeUpdate();
				}
				con.commit();
				int num_finished = tasks_finished.incrementAndGet();
				double end = System.currentTimeMillis();	
				System.out.println("Done task "+task+". I am finisher number "+num_finished + ". Finished in "+ (end-start));
				con.close();
			} catch (SQLException e) {
				System.err.println("Failed " + task + " due to "+ e.getMessage());
			}
        }
	}

	class ConnectionFactory {
		public static Connection getDefaultParameterConnection() throws SQLException {
			String url = "jdbc:postgresql://localhost:5432/";
			Properties props = new Properties();
			// Add connection properties as needed:
			props.setProperty("user", "pg_username");
			props.setProperty("password", "pg_password");
			return DriverManager.getConnection(url, props);
		}
	}
}