package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {
    private final Jedis jedis;
    private final String KEY_LAST_SEARCHES = "LAST_SEARCHES";
    private final int MAX_LAST_SEARCHES_NUMBER = 10;

    public RedisRepository() {
        this.jedis = new Jedis("127.0.0.1", 6379);
    }

    public void addToLastTenSearches(String search) {
        this.jedis.lpush(KEY_LAST_SEARCHES, search);
    }

    public List<String> getLastTenSearches() {
        return this.jedis.lrange(KEY_LAST_SEARCHES, 0, MAX_LAST_SEARCHES_NUMBER - 1);
    }
}
