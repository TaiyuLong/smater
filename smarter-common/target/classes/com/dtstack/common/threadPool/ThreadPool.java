package com.dtstack.common.threadPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;


public class ThreadPool {
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private int defaultPoolSize = 5000;
	
	private int defaultThreadNum = 1;
	
	private String defaultPoolName = "pool";
	
	private ScheduledThreadPoolExecutor scheduler;
	
	private Object synedObject = new Object();
	
	private Map<String,BlockedExecutor> executors = new ConcurrentHashMap<>();
	
	public ThreadPool() {
		this.scheduler = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("scheduler"));
	}
	
	public ThreadPool(int defaultPoolSize, int defaultThreadNum, String defaultPoolName) {
		this.defaultPoolName = defaultPoolName;
		this.defaultPoolSize = defaultPoolSize;
		this.defaultThreadNum = defaultThreadNum;
		this.scheduler = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("scheduler-" + defaultPoolName));
	}
	
	public ScheduledFuture<?> scheduleWithDelay(Runnable command, long delaytime) {
		return scheduler.scheduleWithFixedDelay(command, delaytime, delaytime, TimeUnit.MILLISECONDS);
	}
	
	public BlockedExecutor getExecutor() {
		
		String key = defaultPoolName + defaultPoolSize + defaultThreadNum;
		
		if(!executors.containsKey(key)) {
			synchronized (synedObject) {
				if(!executors.containsKey(key)) {
					executors.put(key, new BlockedExecutor(defaultPoolName,defaultPoolSize,defaultThreadNum));
				}
				
			}
		}
		
		return executors.get(key); 
	}
	
	public BlockedExecutor getExecutor(String poolName, int poolSize, int threadNum) {
		
		String key = poolName + poolSize + threadNum;
		
		if(!executors.containsKey(key)) {
			synchronized (synedObject) {
				if(!executors.containsKey(key)) {
					executors.put(key, new BlockedExecutor(poolName,poolSize,threadNum));
				}
				
			}
		}
		
		return executors.get(key); 
	}
	
	public class BlockedExecutor extends ThreadPoolExecutor {
		
		private final Semaphore semp;

		public BlockedExecutor(String poolName, int corePoolSize, int threadNum) {
			super(corePoolSize, corePoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(poolName));
			
			semp = new Semaphore(corePoolSize); 
		}
		
		
		/**
		 * 线程池满阻塞执行。
		 * @param command
		 */
		public void exec(final RunnableHandler command){
			
			try {
				
				semp.acquire();
				
				super.execute(new Runnable() {
					
					@Override
					public void run() {
						
						try {
							
							command.process();
							
						} catch(Exception e) {
							
						} finally {
							semp.release();
						}
						
					}
				});
				
				
			} catch (InterruptedException e) {
				
				logger.warn("msg={}",e.getMessage());
			}
			
		}
	}
	
	public interface RunnableHandler {
		
		public void process();
	}
	
	public void shutdown() {
		for(Map.Entry<String, BlockedExecutor> e : executors.entrySet()) {
			e.getValue().shutdown();
		}
		
		scheduler.shutdown();
	}
	
	public static void main(String[] args) {
		ThreadPool pool = new ThreadPool(1, 1, "haode");
		pool.getExecutor().execute(new Runnable() {
			
			@Override
			public void run() {
				
				int i = 0;
				while(i++ < 100) {
					System.out.println("dddd");
					try {
						Thread.sleep(10000l);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		});
	}
	
}
