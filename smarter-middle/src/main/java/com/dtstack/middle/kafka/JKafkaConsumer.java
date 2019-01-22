package com.dtstack.middle.kafka;

import com.dtstack.common.Tools.TimeClock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JKafkaConsumer {

	private static Logger logger = LoggerFactory.getLogger(JKafkaConsumer.class);

	private volatile Properties props;

	private static List<JKafkaConsumer> jconsumerList = new CopyOnWriteArrayList<>();

	private Map<String, Client> containers = new ConcurrentHashMap<>();

	private ExecutorService executor = Executors.newCachedThreadPool();

	private long offsetCountInterval = Long.MAX_VALUE - 256;
	private long offsetTimeIntervalMs = Long.MAX_VALUE - 256;

	public JKafkaConsumer(Properties props) {
		this.props = props;
	}

	public static JKafkaConsumer init(Properties p) throws IllegalStateException {

		Properties props = new Properties();
		props.put("max.poll.interval.ms", "86400000");
		props.put("connections.max.idle.ms", "30000");
		props.put("request.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		if (p != null) {
			props.putAll(p);
		}

		JKafkaConsumer jKafkaConsumer = new JKafkaConsumer(props);

		jconsumerList.add(jKafkaConsumer);

		return jKafkaConsumer;
	}

	public static JKafkaConsumer init(String bootstrapServer) {

		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServer);

		return init(props);
	}

	public JKafkaConsumer add(String topics, String group, Caller caller, long timeout, long offsetCountInterval,
			long offsetTimeIntervalMs, int threadCount) {
		Properties clientProps = new Properties();
		clientProps.putAll(props);
		clientProps.put("group.id", group);

		if (threadCount < 1) {
			threadCount = 1;
		}
		for (int i = 0; i < threadCount; i++) {
			containers.put(topics + "_" + i, new Client(clientProps, Arrays.asList(topics.split(",")), caller, timeout,
					offsetCountInterval, offsetTimeIntervalMs));
		}

		return this;
	}

	public JKafkaConsumer add(String topics, String group, Caller caller) {
		return add(topics, group, caller, Long.MAX_VALUE, offsetCountInterval, offsetTimeIntervalMs, 1);
	}

	public JKafkaConsumer add(String topics, String group, Caller caller, int threadCount) {
		return add(topics, group, caller, Long.MAX_VALUE, offsetCountInterval, offsetTimeIntervalMs, threadCount);
	}

	public void execute() {
		for (Map.Entry<String, Client> c : containers.entrySet()) {
			executor.execute(c.getValue());
		}
	}

	public interface Caller {
		public void processMessage(String message);

		public void catchException(String message, Throwable e);
	}

	public class Client implements Runnable {

		private long offsetCountInterval;

		private long offsetTimeIntervalMs;

		private long count = 0;

		private long lasttime = 0;

		private Caller caller;

		private volatile boolean running = true;

		private long pollTimeout;

		private KafkaConsumer<String, String> consumer;

		public Client(Properties clientProps, List<String> topics, Caller caller, long pollTimeout,
				long offsetCountInterval, long offsetTimeIntervalMs) {

			this.pollTimeout = pollTimeout;
			this.offsetCountInterval = offsetCountInterval;
			this.offsetTimeIntervalMs = offsetTimeIntervalMs;
			this.caller = caller;

			consumer = new KafkaConsumer<>(clientProps);
			consumer.subscribe(topics);
		}

		@Override
		public void run() {

			try {

				while (running) {

					try {
						
						ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
						
						for (ConsumerRecord<String, String> r : records) {

							if (r.value() == null || "".equals(r.value())) {
								continue;
							}

							try {
								caller.processMessage(r.value());

								if (++count > offsetCountInterval
										|| TimeClock.now() - lasttime > offsetTimeIntervalMs) {
									consumer.commitSync();
									count = 0;
									lasttime = TimeClock.now();
								}

							} catch (Throwable e) {
								caller.catchException(r.value(), e);
							}
						}
						
					} catch (WakeupException e) {
						logger.warn("kafka consumer WakeupException", e);
					}
				}

			} catch (Throwable e) {
				caller.catchException("", e);
			} finally {
				consumer.close();
			}
		}

		public void close() {
			try {
				running = false;
				consumer.wakeup();
			} catch (Exception e) {
				logger.error("close kafka consumer error", e);
			}
		}
	}

	public void close() {
		for (Map.Entry<String, Client> c : containers.entrySet()) {
			containers.remove(c.getKey());
			c.getValue().close();
		}
	}

	public static void closeAll() {
		for (JKafkaConsumer c : jconsumerList) {
			c.close();
		}
	}

	public static void main(String[] args) throws Exception {

		JKafkaConsumer consumer = JKafkaConsumer.init("localhost:9092");
		consumer.add("liangchentopic", "liangchen_group", new Caller() {

			@Override
			public void processMessage(String message) {
				int i = Integer.valueOf(message);
				try {
					Thread.sleep(10000000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			@Override
			public void catchException(String message, Throwable e) {
				e.printStackTrace();
			}
		}, 1).execute();

		Thread.sleep(1000000000);

		consumer.close();
	}

}
