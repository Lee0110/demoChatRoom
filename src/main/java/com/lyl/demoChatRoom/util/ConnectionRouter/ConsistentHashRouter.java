package com.lyl.demoChatRoom.util.ConnectionRouter;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsistentHashRouter implements IConnectionRouter {
    
    private final HashFunction hashFunction;
    private final int virtualNodes; // 虚拟节点数量
    private final NavigableMap<Long, String> ring; // 哈希环
    private final Map<String, Set<String>> serverConnections; // 服务器->连接映射
    private final Map<String, String> connectionToServer; // 连接->服务器映射
    
    public ConsistentHashRouter(int virtualNodes) {
        this.hashFunction = Hashing.murmur3_128(); // 使用murmur3哈希，性能优秀
        this.virtualNodes = virtualNodes;
        this.ring = new ConcurrentSkipListMap<>();
        this.serverConnections = new ConcurrentHashMap<>();
        this.connectionToServer = new ConcurrentHashMap<>();
    }
    
    public ConsistentHashRouter() {
        this(150); // 默认150个虚拟节点
    }
    
    @Override
    public String addUser(String userId) {
        // todo
        return getServer("user:" + userId);
    }
    
    @Override
    public String addService(String serviceId) {
        // todo
        return getServer("service:" + serviceId);
    }

    @Override
    public String getServerForUser(String userId) {
        // todo
        return "";
    }

    @Override
    public String getServerForService(String serviceId) {
        // todo
        return "";
    }

    private String getServer(String key) {
        if (ring.isEmpty()) {
            return null;
        }
        
        long hash = hashFunction.hashString(key, StandardCharsets.UTF_8).asLong();
        
        // 查找顺时针方向第一个节点
        Map.Entry<Long, String> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            // 如果没找到，选择第一个节点（环形结构）
            entry = ring.firstEntry();
        }
        
        String server = entry.getValue();
        
        // 记录连接分配
        serverConnections.computeIfAbsent(server, k -> ConcurrentHashMap.newKeySet()).add(key);
        connectionToServer.put(key, server);
        
        return server;
    }
    
    @Override
    public List<String> addServer(String server) {
        List<String> migratedConnections = new ArrayList<>();
        
        // 添加虚拟节点到环上
        for (int i = 0; i < virtualNodes; i++) {
            String virtualNode = server + "#" + i;
            long hash = hashFunction.hashString(virtualNode, StandardCharsets.UTF_8).asLong();
            ring.put(hash, server);
        }
        
        // 初始化服务器连接集合
        serverConnections.putIfAbsent(server, ConcurrentHashMap.newKeySet());
        
        // 重新分配现有连接，找出需要迁移的连接
        Map<String, String> newMapping = new HashMap<>();
        for (String connection : connectionToServer.keySet()) {
            String newServer = findServerForConnection(connection);
            newMapping.put(connection, newServer);
        }
        
        // 找出迁移的连接
        for (Map.Entry<String, String> entry : newMapping.entrySet()) {
            String connection = entry.getKey();
            String newServer = entry.getValue();
            String oldServer = connectionToServer.get(connection);
            
            if (!newServer.equals(oldServer)) {
                migratedConnections.add(connection);
                // 更新映射
                serverConnections.get(oldServer).remove(connection);
                serverConnections.get(newServer).add(connection);
                connectionToServer.put(connection, newServer);
            }
        }
        
        return migratedConnections;
    }
    
    @Override
    public List<String> removeServer(String server) {

        // 移除虚拟节点
        ring.entrySet().removeIf(entry -> entry.getValue().equals(server));
        
        // 获取需要迁移的连接
        Set<String> connectionsToMigrate = serverConnections.getOrDefault(server, Collections.emptySet());
        List<String> migratedConnections = new ArrayList<>(connectionsToMigrate);
        
        // 重新分配这些连接
        for (String connection : connectionsToMigrate) {
            String newServer = findServerForConnection(connection);
            if (newServer != null) {
                serverConnections.computeIfAbsent(newServer, k -> ConcurrentHashMap.newKeySet()).add(connection);
                connectionToServer.put(connection, newServer);
            } else {
                connectionToServer.remove(connection);
            }
        }
        
        // 移除服务器
        serverConnections.remove(server);
        
        return migratedConnections;
    }
    
    private String findServerForConnection(String connection) {
        if (ring.isEmpty()) {
            return null;
        }
        
        long hash = hashFunction.hashString(connection, StandardCharsets.UTF_8).asLong();
        Map.Entry<Long, String> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }
    
    @Override
    public Set<String> getAllServers() {
        return new HashSet<>(serverConnections.keySet());
    }
    
    @Override
    public Set<String> getConnectionsOnServer(String server) {
        return new HashSet<>(serverConnections.getOrDefault(server, Collections.emptySet()));
    }
    
    @Override
    public Map<String, Set<String>> getAllConnections() {
        Map<String, Set<String>> result = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : serverConnections.entrySet()) {
            result.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return result;
    }
    
    /**
     * 获取哈希环状态（调试用）
     */
    public void printRingStatus() {
        System.out.println("=== 哈希环状态 ===");
        for (Map.Entry<Long, String> entry : ring.entrySet()) {
            System.out.printf("Hash: %d -> Server: %s%n", entry.getKey(), entry.getValue());
        }
        System.out.println();
    }
    
    /**
     * 获取负载均衡统计
     */
    public void printLoadBalanceStats() {
        System.out.println("=== 负载均衡统计 ===");
        for (Map.Entry<String, Set<String>> entry : serverConnections.entrySet()) {
            System.out.printf("Server: %s, Connections: %d%n", 
                entry.getKey(), entry.getValue().size());
        }
        System.out.println();
    }
}
