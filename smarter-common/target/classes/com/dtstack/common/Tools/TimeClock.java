package com.dtstack.common.Tools;

import com.dtstack.common.threadPool.ThreadPool;

import java.util.concurrent.atomic.AtomicLong;

public class TimeClock {
	
	private static TimeClock clock;
	
	static {
		clock = new TimeClock();
		clock.start();
	}
	
	ThreadPool threadPool = new ThreadPool(1, 1, "timeclock");
	
	private AtomicLong current = new AtomicLong(0);
	
	public void start() {
		threadPool.scheduleWithDelay(new Runnable() {
			
			@Override
			public void run() {
				current.set(System.currentTimeMillis());
			}
		}, 1);
	}
	
	public void stop() {
		threadPool.shutdown();
	}
	
	public static long now() {
		return clock.current();
	}
	public long current() {
		
		if(current.get() <= 0) {
			return System.currentTimeMillis();
		}
		
		return current.get();
	}
	
	public static void main(String[] args) throws InterruptedException {
		TimeClock clock = new TimeClock();
		clock.start();
		for(int i = 0; i < 100; i++) {
			System.out.println("system:"+System.currentTimeMillis()+",timeclock:"+clock.current());
			Thread.sleep(i*1000);
		}
	}
}
