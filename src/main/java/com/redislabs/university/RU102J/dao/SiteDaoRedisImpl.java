package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class SiteDaoRedisImpl implements SiteDao {
    private final JedisPool jedisPool;

    public SiteDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // When we insert a site, we set all of its values into a single hash.
    // We then store the site's id in a set for easy access.
    @Override
    public void insert(Site site) {
        try (Jedis jedis = jedisPool.getResource()) {
            String hashKey = RedisSchema.getSiteHashKey(site.getId());
            String siteIdKey = RedisSchema.getSiteIDsKey();
            jedis.hmset(hashKey, site.toMap());
            jedis.sadd(siteIdKey, hashKey);
        }
    }

    @Override
    public Site findById(long id) {
        try(Jedis jedis = jedisPool.getResource()) {
            String key = RedisSchema.getSiteHashKey(id);
            Map<String, String> fields = jedis.hgetAll(key);
            if (fields == null || fields.isEmpty()) {
                return null;
            } else {
                return new Site(fields);
            }
        }
    }

    // Challenge #1
    @Override
    public Set<Site> findAll() {
        // START Challenge #1
          
        Set<Site> responce = new HashSet<>();
        try(Jedis jedis = jedisPool.getResource()) {
            
            Set<String> keySet = jedis.smembers(RedisSchema.getSiteIDsKey());
            Iterator<String> iterator = keySet.iterator();
       
	        while (iterator.hasNext()) {
	           
	        Map<String, String> fields = jedis.hgetAll(iterator.next());
	            if (fields == null || fields.isEmpty()) {
	                continue;
	            } else {
	                responce.add( new Site(fields));
	            }
	            
	        }
           
        }
        
        return responce;
        // END Challenge #1
    }
}
