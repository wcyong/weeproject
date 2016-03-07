package org.wee.common.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * redis操作类封装
 * @author wcyong
 *
 */
@Service
public class RedisService {

	@Autowired
	private ShardedJedisPool shardedJedisPool;
	
	private <T> T execute(Function<T, ShardedJedis> fun) {
		ShardedJedis shardedJedis = null;
		try {
			//从连接池中获取到jedis分片对象
			shardedJedis = shardedJedisPool.getResource();
			return fun.callback(shardedJedis);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(shardedJedis != null) {
				// 关闭，检测连接是否有效，有效则放回到连接池中，无效则重置状态
				shardedJedis.close();
			}
		}
		return null;
	}
	
	/**
	 * 执行set操作
	 * @param key
	 * @param value
	 * @return
	 */
	public String set(final String key, final String value) {
		return this.execute(new Function<String, ShardedJedis>() {

			@Override
			public String callback(ShardedJedis shardedJedis) {
				return shardedJedis.set(key, value);
			}
		});
	}
	
	/**
	 * 设置值并且设置生存时间
	 * @param key
	 * @param value
	 * @param seconds
	 * @return
	 */
	public String set(final String key, final String value, final Integer seconds) {
		return this.execute(new Function<String, ShardedJedis>() {

			@Override
			public String callback(ShardedJedis shardedJedis) {
				String result = shardedJedis.set(key, value);
				shardedJedis.expire(key, seconds);
				return result;
			}
		});
	}
	
	/**
	 * get操作
	 * @param key
	 * @return
	 */
	public String get(final String key) {
		return this.execute(new Function<String, ShardedJedis>() {

			@Override
			public String callback(ShardedJedis shardedJedis) {
				return shardedJedis.get(key);
			}
		});
	}
	
	/**
	 * 删除操作
	 * @param key
	 * @return
	 */
	public Long del(final String key) {
		return this.execute(new Function<Long, ShardedJedis>() {

			@Override
			public Long callback(ShardedJedis shardedJedis) {
				return shardedJedis.del(key);
			}
		});
	}
	
	/**
	 * 设置生存时间
	 * @param key
	 * @param seconds
	 * @return
	 */
	public Long expire(final String key, final Integer seconds) {
		return this.execute(new Function<Long, ShardedJedis>() {

			@Override
			public Long callback(ShardedJedis shardedJedis) {
				return shardedJedis.expire(key, seconds);
			}
		});
	}
	
}
