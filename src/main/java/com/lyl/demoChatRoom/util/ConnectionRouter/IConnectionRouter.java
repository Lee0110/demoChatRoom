package com.lyl.demoChatRoom.util.ConnectionRouter;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 连接路由接口
 */
public interface IConnectionRouter {

    /**
     * 获取用户应该连接的服务器
     * @param userId 用户ID
     * @return 服务器节点，如果没有可用服务器返回null
     */
    String addUser(String userId);

    /**
     * 获取客服应该连接的服务器
     * @param serviceId 客服ID
     * @return 服务器节点，如果没有可用服务器返回null
     */
    String addService(String serviceId);

    String getServerForUser(String userId);

    String getServerForService(String serviceId);

    /**
     * 添加服务器节点
     * @param server 服务器标识
     * @return 需要迁移的连接列表
     */
    List<String> addServer(String server);

    /**
     * 移除服务器节点
     * @param server 服务器标识
     * @return 需要迁移的连接列表
     */
    List<String> removeServer(String server);

    /**
     * 获取所有服务器节点
     * @return 服务器列表
     */
    Set<String> getAllServers();

    /**
     * 获取指定服务器上的所有连接
     * @param server 服务器标识
     * @return 连接列表
     */
    Set<String> getConnectionsOnServer(String server);

    /**
     * 获取所有连接的分布情况
     * @return 服务器->连接列表的映射
     */
    Map<String, Set<String>> getAllConnections();
}
