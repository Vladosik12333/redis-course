package com.redislabs.university.RU102J.dao;

import redis.clients.jedis.*;

import java.time.Instant;
import java.time.ZonedDateTime;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        long timestamp = Instant.now().toEpochMilli();

        try (Jedis jedis = jedisPool.getResource()) {
            Transaction transaction = jedis.multi();
            String value = new StringBuilder().append(timestamp).append("-").append(name).toString();

            transaction.zadd(getKey(name), timestamp, value);

            long passedTimestamp = timestamp - windowSizeMS;

            transaction.zremrangeByScore(getKey(name), 0, passedTimestamp);

            Response<Long> count = transaction.zcard(getKey(name));

            transaction.exec();

            if (count.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
    }

    private String getKey(String name) {
        return RedisSchema.getRateSlidingLimiterKey(name, windowSizeMS, maxHits);
    }
}
