package com.lyl.demoChatRoom.util.ConnectionRouter;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 基于Redis的无状态一致性哈希路由器
 */
public class StatelessConsistentHashRouter implements IConnectionRouter {
    
    private static final Logger log = LoggerFactory.getLogger(StatelessConsistentHashRouter.class);
    
    // Redis键名常量
    private static final String HASH_RING_NODES = "hash_ring:nodes";
    private static final String IM_SERVERS = "im_servers";
    private static final String CONNECTION_MAPPING = "connection_mapping";
    private static final String HASH_RING_CONFIG = "hash_ring:config";
    private static final String SERVER_CHANGE_CHANNEL = "im_server_changes";
    
    private final RedisTemplate<String, Object> redisTemplate;
    private final HashFunction hashFunction;
    private final int virtualNodes;
    
    // 本地缓存
    private volatile NavigableMap<Long, String> localRing = new ConcurrentSkipListMap<>();
    private volatile long cacheVersion = -1;
    private final ScheduledExecutorService scheduler;
    
    public StatelessConsistentHashRouter(RedisTemplate<String, Object> redisTemplate) {
        this(redisTemplate, 150);
    }
    
    public StatelessConsistentHashRouter(RedisTemplate<String, Object> redisTemplate, int virtualNodes) {
        this.redisTemplate = redisTemplate;
        this.hashFunction = Hashing.murmur3_128();
        this.virtualNodes = virtualNodes;
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        // 初始化配置
        initializeConfig();
        
        // 启动定时刷新任务
        startCacheRefreshTask();
        
        // 初始加载
        loadRingFromRedis();
    }
    
    /**
     * 初始化Redis中的配置
     */
    private void initializeConfig() {
        try {
            // 检查是否已有配置
            if (!redisTemplate.hasKey(HASH_RING_CONFIG)) {
                Map<String, Object> config = new HashMap<>();
                config.put("virtual_nodes", virtualNodes);
                config.put("hash_function", "murmur3");
                config.put("version", 0);
                redisTemplate.opsForHash().putAll(HASH_RING_CONFIG, config);
            }
        } catch (Exception e) {
            log.error("初始化配置失败", e);
        }
    }
    
    @Override
    public String getServerForUser(String userId) {
        return getServer("user:" + userId);
    }
    
    @Override
    public String getServerForService(String serviceId) {
        return getServer("service:" + serviceId);
    }
    
    private String getServer(String key) {
        // 确保本地缓存是最新的
        refreshCacheIfNeeded();
        
        if (localRing.isEmpty()) {
            return null;
        }
        
        long hash = hashFunction.hashString(key, StandardCharsets.UTF_8).asLong();
        
        // 查找顺时针方向第一个节点
        Map.Entry<Long, String> entry = localRing.ceilingEntry(hash);
        if (entry == null) {
            entry = localRing.firstEntry();
        }
        
        String server = entry.getValue();
        
        // 缓存连接映射
        cacheConnectionMapping(key, server);
        
        return server;
    }
    
    @Override
    public List<String> addServer(String server) {
        try {
            log.info("开始添加服务器: {}", server);
            
            // 获取添加前的连接分布（用于计算迁移）
            Map<String, String> beforeMapping = getCurrentConnectionMapping();
            
            // 1. 添加到服务器列表
            redisTemplate.opsForSet().add(IM_SERVERS, server);
            
            // 2. 添加虚拟节点到哈希环
            for (int i = 0; i < virtualNodes; i++) {
                String virtualNode = server + "#" + i;
                long hash = hashFunction.hashString(virtualNode, StandardCharsets.UTF_8).asLong();
                redisTemplate.opsForZSet().add(HASH_RING_NODES, virtualNode, hash);
            }
            
            // 3. 更新版本号
            redisTemplate.opsForHash().increment(HASH_RING_CONFIG, "version", 1);
            
            // 4. 立即刷新本地缓存
            loadRingFromRedis();
            
            // 5. 计算迁移的连接
            List<String> migratedConnections = calculateMigratedConnections(beforeMapping);
            
            // 6. 发布服务器变更事件
            publishServerChangeEvent("ADD", server);
            
            log.info("服务器添加完成: {}, 迁移连接数: {}", server, migratedConnections.size());
            return migratedConnections;
            
        } catch (Exception e) {
            log.error("添加服务器失败: " + server, e);
            throw new RuntimeException("添加服务器失败", e);
        }
    }
    
    @Override
    public List<String> removeServer(String server) {
        try {
            log.info("开始移除服务器: {}", server);
            
            // 1. 获取该服务器上的连接（需要迁移的连接）
            List<String> connectionsToMigrate = getConnectionsToMigrate(server);
            
            // 2. 从服务器列表移除
            redisTemplate.opsForSet().remove(IM_SERVERS, server);
            
            // 3. 移除虚拟节点
            for (int i = 0; i < virtualNodes; i++) {
                String virtualNode = server + "#" + i;
                redisTemplate.opsForZSet().remove(HASH_RING_NODES, virtualNode);
            }
            
            // 4. 更新版本号
            redisTemplate.opsForHash().increment(HASH_RING_CONFIG, "version", 1);
            
            // 5. 清理该服务器的连接映射缓存
            cleanupConnectionMappings(server);
            
            // 6. 立即刷新本地缓存
            loadRingFromRedis();
            
            // 7. 重新分配迁移的连接
            for (String connection : connectionsToMigrate) {
                String newServer = getServer(connection);
                if (newServer != null) {
                    cacheConnectionMapping(connection, newServer);
                }
            }
            
            // 8. 发布服务器变更事件
            publishServerChangeEvent("REMOVE", server);
            
            log.info("服务器移除完成: {}, 迁移连接数: {}", server, connectionsToMigrate.size());
            return connectionsToMigrate;
            
        } catch (Exception e) {
            log.error("移除服务器失败: " + server, e);
            throw new RuntimeException("移除服务器失败", e);
        }
    }
    
    @Override
    public Set<String> getAllServers() {
        try {
            Set<Object> servers = redisTemplate.opsForSet().members(IM_SERVERS);
            return servers != null ? 
                servers.stream().map(Object::toString).collect(Collectors.toSet()) : 
                new HashSet<>();
        } catch (Exception e) {
            log.error("获取所有服务器失败", e);
            return new HashSet<>();
        }
    }
    
    @Override
    public Set<String> getConnectionsOnServer(String server) {
        try {
            // 从Redis中查询该服务器上的所有连接
            Map<Object, Object> allMappings = redisTemplate.opsForHash().entries(CONNECTION_MAPPING);
            Set<String> connections = new HashSet<>();
            
            for (Map.Entry<Object, Object> entry : allMappings.entrySet()) {
                if (server.equals(entry.getValue().toString())) {
                    connections.add(entry.getKey().toString());
                }
            }
            
            return connections;
        } catch (Exception e) {
            log.error("获取服务器连接失败: " + server, e);
            return new HashSet<>();
        }
    }
    
    @Override
    public Map<String, Set<String>> getAllConnections() {
        try {
            Map<Object, Object> allMappings = redisTemplate.opsForHash().entries(CONNECTION_MAPPING);
            Map<String, Set<String>> result = new HashMap<>();
            
            // 初始化所有服务器的连接集合
            Set<String> servers = getAllServers();
            for (String server : servers) {
                result.put(server, new HashSet<>());
            }
            
            // 分组连接
            for (Map.Entry<Object, Object> entry : allMappings.entrySet()) {
                String connection = entry.getKey().toString();
                String server = entry.getValue().toString();
                result.computeIfAbsent(server, k -> new HashSet<>()).add(connection);
            }
            
            return result;
        } catch (Exception e) {
            log.error("获取所有连接失败", e);
            return new HashMap<>();
        }
    }
    
    /**
     * 从Redis加载哈希环
     */
    private void loadRingFromRedis() {
        try {
            // 获取当前版本号
            Object versionObj = redisTemplate.opsForHash().get(HASH_RING_CONFIG, "version");
            Long currentVersion = versionObj != null ? Long.parseLong(versionObj.toString()) : 0L;
            
            if (currentVersion.equals(cacheVersion)) {
                // 版本未变更，无需重新加载
                return;
            }
            
            // 从Redis加载哈希环
            Set<ZSetOperations.TypedTuple<Object>> ringData = redisTemplate.opsForZSet()
                .rangeWithScores(HASH_RING_NODES, 0, -1);
            
            NavigableMap<Long, String> newRing = new ConcurrentSkipListMap<>();
            if (ringData != null) {
                for (ZSetOperations.TypedTuple<Object> tuple : ringData) {
                    Long hashValue = Objects.requireNonNull(tuple.getScore()).longValue();
                    String virtualNode = Objects.requireNonNull(tuple.getValue()).toString();
                    String server = virtualNode.split("#")[0]; // 去掉虚拟节点后缀
                    newRing.put(hashValue, server);
                }
            }
            
            // 原子更新本地缓存
            this.localRing = newRing;
            this.cacheVersion = currentVersion;
            
            log.debug("哈希环已更新，版本: {}, 节点数: {}", currentVersion, newRing.size());
            
        } catch (Exception e) {
            log.error("加载哈希环失败", e);
        }
    }
    
    /**
     * 检查并刷新缓存
     */
    private void refreshCacheIfNeeded() {
        try {
            Object versionObj = redisTemplate.opsForHash().get(HASH_RING_CONFIG, "version");
            Long currentVersion = versionObj != null ? Long.parseLong(versionObj.toString()) : 0L;
            
            if (!currentVersion.equals(cacheVersion)) {
                loadRingFromRedis();
            }
        } catch (Exception e) {
            log.error("检查缓存版本失败", e);
        }
    }
    
    /**
     * 缓存连接映射
     */
    private void cacheConnectionMapping(String connection, String server) {
        try {
            redisTemplate.opsForHash().put(CONNECTION_MAPPING, connection, server);
        } catch (Exception e) {
            log.error("缓存连接映射失败: {} -> {}", connection, server, e);
        }
    }
    
    /**
     * 获取当前所有连接的映射关系
     */
    private Map<String, String> getCurrentConnectionMapping() {
        try {
            Map<Object, Object> rawMapping = redisTemplate.opsForHash().entries(CONNECTION_MAPPING);
            Map<String, String> mapping = new HashMap<>();
            for (Map.Entry<Object, Object> entry : rawMapping.entrySet()) {
                mapping.put(entry.getKey().toString(), entry.getValue().toString());
            }
            return mapping;
        } catch (Exception e) {
            log.error("获取当前连接映射失败", e);
            return new HashMap<>();
        }
    }
    
    /**
     * 计算迁移的连接
     */
    private List<String> calculateMigratedConnections(Map<String, String> beforeMapping) {
        List<String> migratedConnections = new ArrayList<>();
        
        try {
            // 重新计算所有连接的分配
            for (String connection : beforeMapping.keySet()) {
                String oldServer = beforeMapping.get(connection);
                String newServer = findServerForConnection(connection);
                
                if (newServer != null && !newServer.equals(oldServer)) {
                    migratedConnections.add(connection);
                    // 更新映射
                    cacheConnectionMapping(connection, newServer);
                }
            }
        } catch (Exception e) {
            log.error("计算迁移连接失败", e);
        }
        
        return migratedConnections;
    }
    
    /**
     * 清理指定服务器的连接映射
     */
    private void cleanupConnectionMappings(String server) {
        try {
            Map<Object, Object> allMappings = redisTemplate.opsForHash().entries(CONNECTION_MAPPING);
            List<String> toRemove = new ArrayList<>();
            
            for (Map.Entry<Object, Object> entry : allMappings.entrySet()) {
                if (server.equals(entry.getValue().toString())) {
                    toRemove.add(entry.getKey().toString());
                }
            }
            
            if (!toRemove.isEmpty()) {
                String[] keys = toRemove.toArray(new String[0]);
                redisTemplate.opsForHash().delete(CONNECTION_MAPPING, (Object[]) keys);
            }
            
        } catch (Exception e) {
            log.error("清理连接映射失败: {}", server, e);
        }
    }
    
    /**
     * 获取需要迁移的连接
     */
    private List<String> getConnectionsToMigrate(String server) {
        try {
            Set<String> connections = getConnectionsOnServer(server);
            return new ArrayList<>(connections);
        } catch (Exception e) {
            log.error("获取迁移连接失败: {}", server, e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 为指定连接查找服务器（不更新缓存）
     */
    private String findServerForConnection(String connection) {
        if (localRing.isEmpty()) {
            return null;
        }
        
        long hash = hashFunction.hashString(connection, StandardCharsets.UTF_8).asLong();
        Map.Entry<Long, String> entry = localRing.ceilingEntry(hash);
        if (entry == null) {
            entry = localRing.firstEntry();
        }
        
        return entry != null ? entry.getValue() : null;
    }
    
    /**
     * 启动缓存刷新任务
     */
    private void startCacheRefreshTask() {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                refreshCacheIfNeeded();
            } catch (Exception e) {
                log.error("定时刷新缓存失败", e);
            }
        }, 5, 10, TimeUnit.SECONDS); // 每10秒检查一次
    }
    
    /**
     * 发布服务器变更事件
     */
    private void publishServerChangeEvent(String action, String server) {
        try {
            Map<String, Object> event = new HashMap<>();
            event.put("action", action);
            event.put("server", server);
            event.put("timestamp", System.currentTimeMillis());
            
            redisTemplate.convertAndSend(SERVER_CHANGE_CHANNEL, event);
        } catch (Exception e) {
            log.error("发布服务器变更事件失败", e);
        }
    }
    
    /**
     * 关闭资源
     */
    public void shutdown() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    
    /**
     * 调试方法：打印哈希环状态
     */
    public void printRingStatus() {
        refreshCacheIfNeeded();
        System.out.println("=== 哈希环状态 ===");
        for (Map.Entry<Long, String> entry : localRing.entrySet()) {
            System.out.printf("Hash: %d -> Server: %s%n", entry.getKey(), entry.getValue());
        }
        System.out.println();
    }
    
    /**
     * 调试方法：打印负载均衡统计
     */
    public void printLoadBalanceStats() {
        Map<String, Set<String>> allConnections = getAllConnections();
        System.out.println("=== 负载均衡统计 ===");
        for (Map.Entry<String, Set<String>> entry : allConnections.entrySet()) {
            System.out.printf("Server: %s, Connections: %d%n", 
                entry.getKey(), entry.getValue().size());
        }
        System.out.println();
    }
}
