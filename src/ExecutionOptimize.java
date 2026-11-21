import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutionOptimize {
	public static int num_queries_per_task = 3;
	public static int num_of_threads = 4;
	public static final int running_time = 30_000;
	
	ConnectionPool pool;
	Random rand = new Random(123456);
	AtomicInteger num_tuple;
	double max_value = 50.0f;
	static AtomicInteger tasks_finished;
	static AtomicInteger tasks_failed;
	
	public ExecutionOptimize(){
		tasks_finished = new AtomicInteger(0);
		tasks_failed = new AtomicInteger(0);
        num_tuple = new AtomicInteger(50000);
		pool = new ConnectionPool(num_of_threads);
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
			Connection con = pool.acquire();
			executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
			executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);

			PreparedStatement ps;
			con.setAutoCommit(false);
			ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			for(int i=0;i<num_tuple.get();i++){
				ps.setInt(1, i);
				ps.setDouble(2,rand.nextDouble()*max_value);
				ps.setInt(3, 0);
				ps.addBatch();
				ps.clearParameters();
			}
			ps.executeBatch();
			con.commit();
			pool.release(con);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		//Create DB
		ExecutionOptimize b = new ExecutionOptimize();
		b.cerateDB();

		BlockingQueue<Blocker> my_tasks = new LinkedBlockingQueue<Blocker>(2_000_000);
        Thread task_producer = new Thread(() -> {
			int task_id = 1;
			while (!Thread.currentThread().isInterrupted()) {
				if (my_tasks.offer(b.new Blocker(task_id))) {
					task_id++;
				}
			}
			my_tasks.clear();
			System.out.println("Produced: " + task_id);
            System.out.println("Producer stop");
        });
		try {
			task_producer.start();
			ArrayList<Thread> myThreads = new ArrayList<>();
			for (int i = 0; i < num_of_threads; i++) {
				Thread t = new Thread(() -> {
					while (!Thread.currentThread().isInterrupted()) {
						Blocker task = my_tasks.poll();
						if (task != null) {
							task.run();
						}
					}
				});
				t.start();
				myThreads.add(t);
			}
			Thread.sleep(running_time);
			task_producer.interrupt();
			task_producer.join();
			for (Thread t : myThreads) {
				t.interrupt();
				t.join();
			}
			System.out.println("Consumer stop");
		} catch (Exception e) {
			e.printStackTrace();
		}
		double throughput = tasks_finished.intValue() / (running_time / 1000.0);
		System.out.println("Total task: " + tasks_finished + " finished, " + tasks_failed + " failed");
		System.out.println("Throughput: " + throughput+ " (task/sec)");
		System.out.println("Program close");
	}
	
	public class Blocker implements Runnable  {
		final int task;
        final double type;

		public Blocker(int id){
            this.task = id;
            this.type = rand.nextDouble();
        }
		
		@Override
		public void run() {
			try {
				Connection conn = pool.acquire();
				if (this.type < 0.6) {
					getSumOfRandomTuples(conn);
				}
				else if (this.type >= 0.6 && this.type < 0.7) {
					insertNewTuple(conn);
				}
				else if (this.type >= 0.7 && this.type < 0.8) {
					getSumOfAllTuples(conn);
				}
				else if (this.type >= 0.8) {
					updateRandomTuples(conn);
				}
				pool.release(conn);
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
		}

		private void getSumOfRandomTuples(Connection conn) throws SQLException {
			try {
                double sum = 0;
				int max =  num_tuple.get();
				Set<Integer> ids = new HashSet<Integer>();
				while (ids.size() < num_queries_per_task) {
					ids.add(ThreadLocalRandom.current().nextInt(0, max));
				}
				PreparedStatement ps = conn.prepareStatement("SELECT data_value FROM blocking_data WHERE data_id = ?;");
				for (int id : ids) {
					ps.setInt(1, id);
					ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        sum += rs.getDouble("data_value");
                    }
				}
				conn.commit();
				final double res = sum;
				System.out.println("Task " + task + ", Case 1, result: " + res + " Finisher: "+ tasks_finished.incrementAndGet());
			} catch (SQLException e) {
				conn.rollback();
                String msg = e.getMessage();
				System.err.println("Task " + task + ", Case 1, " + msg);
				tasks_failed.incrementAndGet();
			}
		}

		private void insertNewTuple(Connection conn) throws SQLException {
            try {
                double new_value = rand.nextDouble()*max_value;
                PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO blocking_data VALUES (?, ?, ?);");
                ps.setInt(1, num_tuple.getAndIncrement());
                ps.setDouble(2, (float) new_value);
                ps.setInt(3, 0);
                ps.execute();
                conn.commit();
				System.out.println("Task " + task + ", Case 2, result: inserted (" + num_tuple.get() + " " + new_value + " " + 0 +")" + " Finisher: "+ tasks_finished.incrementAndGet());
            } catch (SQLException e) {
                conn.rollback();
                if (!"40001".equals(e.getSQLState())) {
                    System.err.println("Task " + task + ", Case 2, " + e.getMessage());
                    tasks_failed.incrementAndGet();
                }
            }
		}

		private void getSumOfAllTuples(Connection conn) throws SQLException {
            try {
                PreparedStatement ps = conn.prepareStatement("SELECT SUM(data_value) as sum_value FROM blocking_data;");
                ResultSet rs = ps.executeQuery();
                rs.next();
                conn.commit();
                System.out.println("Task " + task + ", Case 3, result: " + rs.getDouble("sum_value") + " Finisher: "+ tasks_finished.incrementAndGet());
            } catch (SQLException e) {
                conn.rollback();
                System.err.println("Task " + task + ", Case 3, " + e.getMessage());
                tasks_failed.incrementAndGet();
            }
		}

		private void updateRandomTuples(Connection conn) throws SQLException {
            while (true) {
                try {
					int max =  num_tuple.get();
					Set<Integer> ids = new HashSet<Integer>();
					while (ids.size() < num_queries_per_task) {
						ids.add(ThreadLocalRandom.current().nextInt(0, max));
					}
					PreparedStatement ps = conn.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?;");
                    for (int id : ids) {
                        ps.setDouble(1, (float) (rand.nextDouble()*max_value));
                        ps.setInt(2, task);
                        ps.setInt(3, id);
						ps.executeUpdate();
                    }
                    conn.commit();
					System.out.println("Task " + task + ", Case 4, result: Updated " + num_queries_per_task + " rows" + " Finisher: "+ tasks_finished.incrementAndGet());
                    break;
                } catch (SQLException e) {
                    conn.rollback();
					tasks_failed.incrementAndGet();
                    System.err.println("Task " + task + ", Case 4, " + e.getMessage());
					continue;
                }
            }
		}
	}
	class ConnectionPool implements AutoCloseable {
		private BlockingQueue<Connection> available;

		public ConnectionPool(int n) {
			String url = "jdbc:postgresql://localhost:5432/";
			Properties props = new Properties();
			props.setProperty("user", "postgres");
			props.setProperty("password", "password");
			available = new ArrayBlockingQueue<Connection>(n);
			try {
				for (int i = 0; i < n; i++) {
					Connection con = DriverManager.getConnection(url, props);
					con.setAutoCommit(false);
					con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					available.add(con);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		public synchronized Connection acquire() throws Exception {
			return available.take();
		}
		public void release(Connection con) {
			available.offer(con);
		}

		@Override
		public synchronized void close() {
			Connection connection = available.poll();
			while (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				connection = available.poll();
			}
		}
	}
}
