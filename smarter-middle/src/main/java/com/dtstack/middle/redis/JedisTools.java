package com.dtstack.middle.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.net.URI;

public class JedisTools {

	private JedisPool jedisPool;

	public JedisTools(String addr) {
		this(new JedisPoolConfig(), addr, Protocol.DEFAULT_TIMEOUT);
	}
	
	public JedisTools(final GenericObjectPoolConfig poolConfig, String addr, int timeout) {
		RedisUri uri = parse(addr);
		jedisPool = new JedisPool(poolConfig, uri.getHost(), uri.getPort(), timeout, uri.getPassword(), uri.getDatabase());
	}

	public JedisTools(String host, int port) {
		jedisPool = new JedisPool(host, port);
	}
	
	public JedisTools(int maxIdle, int maxTotal, int maxWaitMillis, URI uri, int timeout) {
		JedisPoolConfig configs = new JedisPoolConfig();
		configs.setMaxIdle(maxIdle);
		configs.setMaxTotal(maxTotal);
		configs.setMaxWaitMillis(maxWaitMillis);

		RedisUri u = parse(uri.toString());
		jedisPool = new JedisPool(configs, u.getHost(), u.getPort(), timeout, u.getPassword(), u.getDatabase());
	}
	
	public static RedisUri parse(String addr) {
		
		if(addr == null) {
			throw new NullPointerException("addr is null");
		}
		
		RedisUri uri = new RedisUri();
		
		if(!addr.startsWith(uri.getProtocol())) {
			throw new IllegalArgumentException("redis protocol error");
		}
		
		//提取密码
		addr = addr.substring(uri.getProtocol().length());
		int pwdIndex = addr.lastIndexOf("@");
		uri.setPassword(addr.substring(0, pwdIndex));
		
		//提取域名
		addr = addr.substring(pwdIndex + 1);
		int hostIndex = addr.indexOf(":");
		uri.setHost(addr.substring(0, hostIndex));
		
		addr = addr.substring(hostIndex + 1);
		int portIndex = addr.indexOf("/");
		uri.setPort(Integer.valueOf(addr.substring(0, portIndex)));
		
		uri.setDatabase(Integer.valueOf(addr.substring(portIndex + 1)));
		
		return uri;
	}
	
	public static class RedisUri {
		
		private String protocol = "redis://:";
		private String password;
		private String host;
		private int port;
		private int database;
		
		public String getProtocol() {
			return protocol;
		}
		public void setProtocol(String protocol) {
			this.protocol = protocol;
		}
		public String getPassword() {
			return password;
		}
		public void setPassword(String password) {
			this.password = password;
		}
		public String getHost() {
			return host;
		}
		public void setHost(String host) {
			this.host = host;
		}
		public int getPort() {
			return port;
		}
		public void setPort(int port) {
			this.port = port;
		}
		public int getDatabase() {
			return database;
		}
		public void setDatabase(int database) {
			this.database = database;
		}
		@Override
		public String toString() {
			return "RedisUri [protocol=" + protocol + ", password=" + password + ", host=" + host + ", port=" + port
					+ ", database=" + database + "]";
		}
		
	}

	

	public String get(String key) {

		Jedis client = jedisPool.getResource();
		String res = client.get(key);
		client.close();

		return res;
	}

	public String lpop(String key) {

		Jedis client = jedisPool.getResource();
		String res = client.lpop(key);
		client.close();

		return res;
	}

	public String set(String k, String v) {
		Jedis client = jedisPool.getResource();
		String res = client.set(k, v);
		client.close();

		return res;
	}

	public Long publish(String channel, String message) {
		Jedis client = jedisPool.getResource();
		Long res = client.publish(channel, message);
		client.close();

		return res;
	}

	public void subscribe(JedisPubSub jedisPubSub, String... channels) {

		Jedis client = jedisPool.getResource();
		try {
			client.subscribe(jedisPubSub, channels);
		} finally {
			if(client != null && client.isConnected()) {
				client.close();
			}
		}
	}

	
	/**
	 * 
	 * @param lockKey
	 * @param lockValue 用于控制可重入，相同则继续延长过期时间。
	 * @param expiredSeconds
	 * @return
	 */
	public boolean acquireLock(String lockKey, String lockValue, int expiredSeconds) {
		Jedis jedis = jedisPool.getResource();
        String value = jedis.get(lockKey);
        boolean flag = false;
        if (value == null) {
            boolean success = jedis.setnx(lockKey, "1") == 1;
            if (success) {
                jedis.expire(lockKey, expiredSeconds);
                flag = true;
            }
        } else if (lockValue.equals(value)) {
            jedis.expire(lockKey, expiredSeconds);
            flag = true;
        }
        jedis.close();
        return flag;
    }
	
	public boolean tryAcquireLock(String lockKey, String lockValue, int expiredSeconds, int waitSeconds) throws InterruptedException {
		long start = System.currentTimeMillis();
		int i = 0;
		int thredhold = Integer.MAX_VALUE/2; 
		while(i++ < thredhold) {
			if(acquireLock(lockKey, lockValue, expiredSeconds)) {
				return true;
			} else {
				if(System.currentTimeMillis() - start > waitSeconds * 1000) {
					return false;
				}
			}
			Thread.sleep(10l);
		}
		
		return false;
	}
	
	public void releaseLock(String lockKey) {
		Jedis jedis = jedisPool.getResource();
		jedis.del(lockKey.getBytes());
		jedis.close();
	}
	
	public static void main(String[] args) {
		String addr = "redis://:ddd@123@localhost:6379/0";
		System.out.println(new JedisTools(1,2,2,URI.create(addr),1000).get("tt"));
	}

}
