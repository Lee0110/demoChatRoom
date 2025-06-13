package com.lyl.demoChatRoom.util.ConnectionRouter;

import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class IMRouterConsole {
    
    private final IConnectionRouter router;
    private final Scanner scanner;
    
    public IMRouterConsole() {
        this.router = new ConsistentHashRouter(150);
        this.scanner = new Scanner(System.in);
    }
    
    public void start() {
        System.out.println("IM服务器路由控制台");
        System.out.println("==================");
        
        while (true) {
            printMenu();
            int choice = getIntInput("请选择操作: ");
            
            try {
                switch (choice) {
                    case 1:
                        addServer();
                        break;
                    case 2:
                        removeServer();
                        break;
                    case 3:
                        getUserServer();
                        break;
                    case 4:
                        getServiceServer();
                        break;
                    case 5:
                        showAllServers();
                        break;
                    case 6:
                        showServerConnections();
                        break;
                    case 7:
                        showAllConnections();
                        break;
                    case 8:
                        showLoadStats();
                        break;
                    case 9:
                        batchAddUsers();
                        break;
                    case 0:
                        System.out.println("再见！");
                        return;
                    default:
                        System.out.println("无效的选择，请重试。");
                }
            } catch (Exception e) {
                System.out.println("操作失败: " + e.getMessage());
            }
            
            System.out.println();
        }
    }
    
    private void printMenu() {
        System.out.println("1. 添加服务器");
        System.out.println("2. 移除服务器");
        System.out.println("3. 查询用户分配的服务器");
        System.out.println("4. 查询客服分配的服务器");
        System.out.println("5. 显示所有服务器");
        System.out.println("6. 显示指定服务器的连接");
        System.out.println("7. 显示所有连接分布");
        System.out.println("8. 显示负载统计");
        System.out.println("9. 批量添加测试用户");
        System.out.println("0. 退出");
        System.out.println("------------------");
    }
    
    private void addServer() {
        String server = getStringInput("请输入服务器名称: ");
        System.out.println("正在添加服务器: " + server);
        
        List<String> migratedConnections = router.addServer(server);
        
        System.out.println("服务器添加成功！");
        if (!migratedConnections.isEmpty()) {
            System.out.println("需要迁移的连接数: " + migratedConnections.size());
            System.out.println("迁移的连接: " + migratedConnections);
        } else {
            System.out.println("无需迁移连接。");
        }
    }
    
    private void removeServer() {
        String server = getStringInput("请输入要移除的服务器名称: ");
        
        if (!router.getAllServers().contains(server)) {
            System.out.println("服务器不存在: " + server);
            return;
        }
        
        System.out.println("正在移除服务器: " + server);
        List<String> migratedConnections = router.removeServer(server);
        
        System.out.println("服务器移除成功！");
        if (!migratedConnections.isEmpty()) {
            System.out.println("迁移的连接数: " + migratedConnections.size());
            System.out.println("迁移的连接: " + migratedConnections);
        }
    }
    
    private void getUserServer() {
        String userId = getStringInput("请输入用户ID: ");
        String server = router.getServerForUser(userId);
        
        if (server != null) {
            System.out.println("用户 " + userId + " 分配到服务器: " + server);
        } else {
            System.out.println("没有可用的服务器");
        }
    }
    
    private void getServiceServer() {
        String serviceId = getStringInput("请输入客服ID: ");
        String server = router.getServerForService(serviceId);
        
        if (server != null) {
            System.out.println("客服 " + serviceId + " 分配到服务器: " + server);
        } else {
            System.out.println("没有可用的服务器");
        }
    }
    
    private void showAllServers() {
        Set<String> servers = router.getAllServers();
        if (servers.isEmpty()) {
            System.out.println("当前没有服务器");
        } else {
            System.out.println("当前服务器列表:");
            servers.forEach(server -> System.out.println("- " + server));
        }
    }
    
    private void showServerConnections() {
        String server = getStringInput("请输入服务器名称: ");
        Set<String> connections = router.getConnectionsOnServer(server);
        
        System.out.println("服务器 " + server + " 上的连接数: " + connections.size());
        if (!connections.isEmpty()) {
            System.out.println("连接列表:");
            connections.forEach(conn -> System.out.println("- " + conn));
        }
    }
    
    private void showAllConnections() {
        Map<String, Set<String>> allConnections = router.getAllConnections();
        
        if (allConnections.isEmpty()) {
            System.out.println("当前没有连接");
            return;
        }
        
        System.out.println("所有连接分布:");
        for (Map.Entry<String, Set<String>> entry : allConnections.entrySet()) {
            System.out.printf("服务器 %s (%d个连接):%n", 
                entry.getKey(), entry.getValue().size());
            entry.getValue().forEach(conn -> System.out.println("  - " + conn));
        }
    }
    
    private void showLoadStats() {
        Map<String, Set<String>> allConnections = router.getAllConnections();
        
        if (allConnections.isEmpty()) {
            System.out.println("当前没有连接");
            return;
        }
        
        System.out.println("负载统计:");
        int totalConnections = 0;
        for (Map.Entry<String, Set<String>> entry : allConnections.entrySet()) {
            int count = entry.getValue().size();
            totalConnections += count;
            System.out.printf("服务器 %s: %d个连接%n", entry.getKey(), count);
        }
        
        System.out.printf("总连接数: %d%n", totalConnections);
        System.out.printf("平均每台服务器: %.2f个连接%n", 
            (double) totalConnections / allConnections.size());
    }
    
    private void batchAddUsers() {
        int userCount = getIntInput("请输入要添加的用户数量: ");
        int serviceCount = getIntInput("请输入要添加的客服数量: ");
        
        System.out.println("正在批量添加连接...");
        
        // 添加用户
        for (int i = 1; i <= userCount; i++) {
            router.getServerForUser("user" + i);
        }
        
        // 添加客服
        for (int i = 1; i <= serviceCount; i++) {
            router.getServerForService("service" + i);
        }
        
        System.out.printf("成功添加 %d 个用户和 %d 个客服连接%n", userCount, serviceCount);
    }
    
    private String getStringInput(String prompt) {
        System.out.print(prompt);
        return scanner.nextLine().trim();
    }
    
    private int getIntInput(String prompt) {
        while (true) {
            try {
                System.out.print(prompt);
                return Integer.parseInt(scanner.nextLine().trim());
            } catch (NumberFormatException e) {
                System.out.println("请输入有效的数字！");
            }
        }
    }
    
    public static void main(String[] args) {
        new IMRouterConsole().start();
    }
}
