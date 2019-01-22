package com.dtstack.common.Tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.common.threadPool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TimeWaste {
	
	private ThreadPool threadPool = new ThreadPool(1, 1, "TimeWaste");
	
	enum StatType {
		TIME_INTERVAL,COUNT_THREDHOLD;
	}
	
	public StatType statType;
	
	public StatType getStatType() {
		return statType;
	}

	public void setStatType(StatType statType) {
		this.statType = statType;
	}

	public String getShowTemplate() {
		return showTemplate;
	}

	public void setShowTemplate(String showTemplate) {
		this.showTemplate = showTemplate;
	}

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private String showTemplate = "";
	
	private static TimeWaste timeWaste;
	
	private volatile int switchIndex = 0;
	
	private List<List<Map<String,Long>>> tupleList = new ArrayList<List<Map<String,Long>>>();
	
	private ThreadLocal<Map<String,Long>> timestampContainer = new ThreadLocal<Map<String,Long>>();
	
	private ThreadLocal<Map<String,Long>> gapContainer = new ThreadLocal<Map<String,Long>>();
	
	private static AtomicBoolean hasInit = new AtomicBoolean(false);
	
	private Long countThredhold  = 0l;
	
	private Long statCount = 0l;
	
	public TimeWaste(StatType statType, Long statFactor, String showTemplate) {
		
		this.showTemplate = showTemplate;
		
		if(statType.equals(StatType.TIME_INTERVAL)) {
			threadPool.scheduleWithDelay(new Runnable() {
				
				@Override
				public void run() {
					 count();
				}
			}, statFactor);
			
		}else if(statType.equals(StatType.COUNT_THREDHOLD)){
			countThredhold = statFactor;
		}else {
			//报错
		}
	}
	
	public static TimeWaste init(StatType statType, Long statFactor, String showTemplate) {
		
		if(!hasInit.get()) {
			synchronized (TimeWaste.class) {
				if(!hasInit.get()) {
					
					timeWaste = new TimeWaste(statType, statFactor, showTemplate);
					timeWaste.setStatType(statType);
					timeWaste.setShowTemplate(showTemplate);
					
					hasInit.set(true);
				}
			}
		}
		
		return timeWaste;
	}
	
	public TimeWaste tab(String tab) {
		Map<String,Long> tempMap = timestampContainer.get();
		if(tempMap == null) {
			tempMap = new HashMap<String,Long>();
			timestampContainer.set(tempMap);
		}
		
		tempMap.put(tab, System.currentTimeMillis());
		
		return this;
	}
	
	
	public TimeWaste compare(String fromTab, String toTab, String storeKey) {
		Map<String,Long> tempMap = timestampContainer.get();
		
		Long fromTabTimestamp = tempMap.get(fromTab);
		Long toTabTimestamp = tempMap.get(toTab);
		
		
		Long gap = toTabTimestamp - fromTabTimestamp;
		
		Map<String,Long> gapMap = gapContainer.get();
		
		gapMap.put(storeKey, gap);
		
		return this;
	}
	
	public void flush() {
		Map<String,Long> gapMap = gapContainer.get();
		List<Map<String,Long>> l = tupleList.get(switchIndex);
		l.add(gapMap);
		
		gapContainer.remove();
		timestampContainer.remove();
		
		if(statType.equals(StatType.COUNT_THREDHOLD)) {
			statCount++;
			if(statCount >= countThredhold) {
				count();
			}
		}
	}
	
	public void count() {
		int idx = 0;
		if(switchIndex == 0) {
			switchIndex = 1;
		}else if(switchIndex == 1) {
			switchIndex = 0;
			idx = 1;
		}else {
			//报错。
		}
		
		List<Map<String,Long>> l = tupleList.get(idx);
		
		Map<String,AtomicLong> sumMap = new HashMap<String, AtomicLong>();
		for(Map<String,Long> m : l) {
			for(Map.Entry<String, Long> entry : m.entrySet()) {
				if(sumMap.containsKey(entry.getKey())) {
					sumMap.get(entry.getKey()).addAndGet(entry.getValue());
				}else {
					sumMap.put(entry.getKey(), new AtomicLong(entry.getValue()));
				}
			}
		}
		
		Map<String,Long>  avgMap = new HashMap<String, Long>();
		for(Map.Entry<String, AtomicLong> entry : sumMap.entrySet()) {
			avgMap.put(entry.getKey(), entry.getValue().get()/l.size());
		}
	}
	
	public void show(Map<String,Long> input) {
		String template = showTemplate;
		for(Map.Entry<String, Long> m : input.entrySet()) {
			template.replaceAll("\\{"+ m.getKey() +"\\}", m.getValue().toString());
		}
		
		logger.info(template);
	}
	
	public static void main(String[] args) {
		
	}
	
	
}
