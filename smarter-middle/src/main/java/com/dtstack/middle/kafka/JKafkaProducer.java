package com.dtstack.middle.kafka;

import com.alibaba.fastjson.JSON;
import com.dtstack.common.threadPool.ThreadPool;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class JKafkaProducer {

	private static final Logger logger = LoggerFactory.getLogger(JKafkaProducer.class);

	private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(100);

	private KafkaProducer<String, String> producer;

	public JKafkaProducer(Properties props) {
		producer = new KafkaProducer<>(props);
	}

	public static JKafkaProducer init(Properties p) {

		Properties props = new Properties();

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("request.timeout.ms", "86400000");
		props.put("retries", "1000000");
		props.put("max.in.flight.requests.per.connection", "1");

		if (p != null) {
			props.putAll(p);
		}

		return new JKafkaProducer(props);
	}

	public static JKafkaProducer init(String bootstrapServers) {

		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);

		return init(props);
	}

	/**
	 * 发送消息，失败重试。
	 * 
	 * @param topic
	 * @param key
	 * @param value
	 */
	public void sendWithRetry(String topic, String key, String value) {
		while (!queue.isEmpty()) {
			sendWithBlock(topic, key, queue.poll());
		}

		sendWithBlock(topic, key, value);
	}

	/**
	 * 发送消息，失败阻塞（放到有界阻塞队列）。
	 * 
	 * @param topic
	 * @param key
	 * @param value
	 */
	public void sendWithBlock(String topic, String key, final String value) {

		if (value == null) {
			return;
		}

		producer.send(new ProducerRecord<String, String>(topic, key, value), new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				try {

					if (exception != null) {
						queue.put(value);
						logger.error("send data failed, wait to retry, value={},error={}", value,
								exception.getMessage());
						Thread.sleep(1000l);
					}
				} catch (InterruptedException e) {
					logger.error("kafka send callback error", e);
				}

			}
		});

	}

	public void close() {
		producer.close();
	}

	public void flush() {
		producer.flush();
	}

	public static void main(String[] args) throws InterruptedException {
		System.out.println("args=" + JSON.toJSONString(args));
		// perf(args[0], args[1], new Integer(args[2]), new Integer(args[3]), new
		// Integer(args[4]));
		test();
	}

	public static void test() throws InterruptedException {
		int i = 0;
		JKafkaProducer p = JKafkaProducer.init("localhost:9092");

		while (i++ < 1000000000) {
			p.sendWithRetry("dt_all_log", UUID.randomUUID().toString(),
					UUID.randomUUID() + "_" + System.currentTimeMillis() + "_" + i);
			Thread.sleep(1000l);
		}

		p.close();
	}

	public static void perf(final String topic, String propsInfo, int recordNum, int recordSize, int concurrent) {
		ThreadPool threadPool = new ThreadPool(concurrent, concurrent, "kafka-producer");
		Properties props = new Properties();
		String[] propsFields = propsInfo.split("&");
		for (String field : propsFields) {
			String[] propsKV = field.split("=");
			props.put(propsKV[0], propsKV[1]);
		}

		final JKafkaProducer p = JKafkaProducer.init(props);

		final StringBuffer strBuf = new StringBuffer();
		for (int i = 0; i < recordSize; i++) {
			strBuf.append(i + "");
			if (strBuf.length() > recordSize) {
				break;
			}
		}
		final long num = recordNum / concurrent;
		final CountDownLatch cc = new CountDownLatch(concurrent);
		for (int i = 0; i < concurrent; i++) {
			threadPool.getExecutor().execute(new Runnable() {

				@Override
				public void run() {
					try {
						for (int j = 0; j < num; j++) {
							p.sendWithRetry(topic, j + "", strBuf.toString());
						}
						cc.countDown();
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			});
		}

		long start = System.currentTimeMillis();
		try {
			cc.await();
		} catch (InterruptedException e) {
		}
		long end = System.currentTimeMillis();

		long totalBytes = strBuf.toString().length() * recordNum;
		long avgBytes = totalBytes * 1000 / (end - start);
		long avgNum = recordNum * 1000 / (end - start);
		System.out.println("waste time=" + (end - start));
		System.out.println("totalBytes=" + totalBytes);
		System.out.println("avgBytes=" + avgBytes);
		System.out.println("avgNum=" + avgNum);

		p.close();
		threadPool.shutdown();
	}

}
