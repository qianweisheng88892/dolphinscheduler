/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.registry.consul;

import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.registry.api.ConnectionListener;
import org.apache.dolphinscheduler.registry.api.Event;
import org.apache.dolphinscheduler.registry.api.Registry;
import org.apache.dolphinscheduler.registry.api.RegistryException;
import org.apache.dolphinscheduler.registry.api.SubscribeListener;

import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.*;
import io.vertx.core.Vertx;

import lombok.NonNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.orbitz.consul.Consul;
//import com.orbitz.consul.SessionClient;
//import com.orbitz.consul.cache.KVCache;
//import com.orbitz.consul.model.kv.Value;
//import com.orbitz.consul.model.session.ImmutableSession;
//import com.orbitz.consul.option.ImmutablePutOptions;
//import com.orbitz.consul.option.PutOptions;

public final class ConsulRegistry implements Registry {

    private static final Logger log = LoggerFactory.getLogger(ConsulRegistry.class);
    private final ConsulClient consulClient;
    private final Vertx vertx;
    private final Map<String, Watch> kvCacheMap = new ConcurrentHashMap<>();
    Map<String, String> lockMap = new HashMap<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private String sessionId;
    private final String namespace;
    private final ConsulConnectionStateListener consulConnectionStateListener;
    public static final String FOLDER_SEPARATOR = "/";

    public String apNameSpace(String p) {
        return namespace + p;
    }

    public String rmNameSpace(String p) {
        return p.substring(namespace.length());
    }

    public ConsulRegistry(ConsulRegistryProperties prop) {
        String url = prop.getUrl();
        Duration sessionTimeout = prop.getSessionTimeout();
        Duration sessionRefreshTime = prop.getSessionTimeout();
        String namespace = prop.getNamespace();
        String userName = prop.getUserName();
        String password = prop.getPassword();
        String aclToken = prop.getAclToken();

        ConsulClientOptions op = new ConsulClientOptions();
        op.setHost(url);
        if (StringUtils.isNoneBlank(aclToken)) {
            op.setAclToken(aclToken);
        }
        VertxOptions vertxOptions = new VertxOptions();
        this.vertx = Vertx.vertx(vertxOptions);
        this.consulClient = ConsulClient.create(vertx, op);

        this.consulConnectionStateListener = new ConsulConnectionStateListener(consulClient);
        this.namespace = namespace;
//        consulConnectionStateListener.start();
        createConsulSession(sessionTimeout.getSeconds());
        startSessionRenewal(sessionRefreshTime);
    }

    /**
     * 给sessionId续期
     */
    private void startSessionRenewal(Duration sessionRefreshTime) {
        executorService.submit(() -> {
            while (true) {
                try {
                    consulClient.renewSession(sessionId);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    Thread.sleep(sessionRefreshTime.toMillis());
                }
            }
        });
    }



    @Override
    public void start() {
        // The start has been set in the constructor
    }

    public <T> T dealwith(Future<T> future,String errorMessage){
        AtomicReference<T> res = new AtomicReference<>();
        future.onComplete(re -> {
            if(re.succeeded()){
                res.set(re.result());
            }else {
              throw new RegistryException(errorMessage,re.cause());
            }
        }).result();
        return res.get();
    }


    private void createConsulSession(long sessionTimeout) {
        SessionOptions sessionOptions = new SessionOptions()
                .setName("consul_registry_session")
                .setLockDelay(15000)
                .setTtl(sessionTimeout)
                .setBehavior(SessionBehavior.DELETE);
        try {
            Future<String> sessionWithOptions = consulClient.createSessionWithOptions(sessionOptions);
            sessionId = dealwith(sessionWithOptions,"Failed to create Consul session");
        }catch (RegistryException e){
            throw e;
        }catch (Exception e) {
            throw new RegistryException("Failed to create Consul session", e);
        }
    }


    @Override
    public boolean isConnected() {
        try {
            Future<JsonObject> future = consulClient.agentInfo();
            dealwith(future,"Failed to check connection status");
            return true;
        }catch (RegistryException e){
            throw e;
        } catch (Exception e) {
            throw new RegistryException("Failed to check connection status", e);
        }
    }

    @Override
    public void connectUntilTimeout(@NonNull Duration timeout) throws RegistryException {
        long start = System.currentTimeMillis();
        while (!isConnected()) {
            if (System.currentTimeMillis() - start > timeout.toMillis()) {
                throw new RegistryException("Cannot connect to registry within the given timeout");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RegistryException("Thread interrupted while waiting for connection", e);
            }
        }
    }

    @Override
    public void subscribe(String path, SubscribeListener listener) {
        path = apNameSpace(path);

        Watch<KeyValueList> watch = Watch.keyPrefix(path, vertx);
        watch.setHandler(new ConsulSubscribeDataListener(new MidSubscriveListener(listener)));
        watch.start();
        kvCacheMap.put(path, watch);
        watch.start();
    }

    public class MidSubscriveListener implements SubscribeListener {

        SubscribeListener listener;

        public MidSubscriveListener(SubscribeListener listener) {
            this.listener = listener;
        }

        @Override
        public void notify(Event event) {
            Event e = new Event(rmNameSpace(event.key()), rmNameSpace(event.path()), event.data(), event.type());
            listener.notify(e);
        }
    }

    @Override
    public void unsubscribe(String path) {
        path = apNameSpace(path);

        if (kvCacheMap.containsKey(path)) {
            kvCacheMap.get(path).stop();
            kvCacheMap.remove(path);
        }
    }

    @Override
    public void addConnectionStateListener(ConnectionListener listener) {
        consulConnectionStateListener.addConnectionListener(listener);
    }

    @Override
    public String get(String key) throws RegistryException {
        key = apNameSpace(key);
        try {
            Future<KeyValue> future = consulClient.getValue(key);
            KeyValue res = dealwith(future, "Failed to get value for key" + key);
            if(res.getValue() == null){
                throw new RegistryException("key: " + key + " not exist");
            }
            return res.getValue();
        } catch (RegistryException e) {
            throw e;
        } catch (Exception e) {
            throw new RegistryException("Error getting value for key: " + key, e);
        }
    }

    @Override
    public void put(String key, String value, boolean deleteOnDisconnect) {
        try {
            key = apNameSpace(key);
            KeyValueOptions op = new KeyValueOptions();
            if (deleteOnDisconnect) {
                op.setAcquireSession(sessionId);
            }
            Future<Boolean> future = consulClient.putValueWithOptions(key, value, op);
            dealwith(future, "Failed to put key" + key);
        } catch (RegistryException e) {
            throw e;
        } catch (Exception e) {
            throw new RegistryException("Error putting key: " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        key = apNameSpace(key);

        try {
            Future<Void> future = consulClient.deleteValue(key);
            dealwith(future, "Failed to delete key" + key);
        } catch (Exception e) {
            throw new RegistryException("Error deleting key: " + key, e);
        }
    }

    @Override
    public Collection<String> children(String key) {
        try {
            key = apNameSpace(key);

            String prefix = key.endsWith(FOLDER_SEPARATOR) ? key : key + FOLDER_SEPARATOR;
            Future<List<String>> future = consulClient.getKeys(prefix);
            List<String> keys = dealwith(future, "Failed to get children for key:" + key);
            return keys.stream().map(e -> getSubNodeKeyName(prefix, e)).distinct().collect(Collectors.toList());
        } catch (Exception e) {
            throw new RegistryException("Error getting children for key: " + key, e);
        }
    }

    /**
     * If "/" exists in the child object, get the string prefixed with "/"
     */
    private String getSubNodeKeyName(final String prefix, final String fullPath) {
        String pathWithoutPrefix = fullPath.substring(prefix.length());
        return pathWithoutPrefix.contains(FOLDER_SEPARATOR)
                ? pathWithoutPrefix.substring(0, pathWithoutPrefix.indexOf(FOLDER_SEPARATOR))
                : pathWithoutPrefix;
    }

    @Override
    public boolean exists(String key) {
        try {
            key = apNameSpace(key);
            Future<KeyValue> future = consulClient.getValue(key);
            KeyValue res = dealwith(future, "Failed check existence for key: " + key);
            return res.getValue()!=null;
        } catch (Exception e) {
            throw new RegistryException("Error checking existence for key: " + key, e);
        }
    }

    @Override
    public boolean acquireLock(String key) {
        key = apNameSpace(key);
        while (true) {
            if (lock(key)) {
                return true;
            }
        }
    }

    public boolean lock(String key) {
        synchronized (this) {
            if (lockMap.containsKey(key)) {
                return lockMap.get(key).equals(LockUtils.getLockOwner());
            }
            Future<Boolean> put = consulClient.putValueWithOptions(key, "", new KeyValueOptions().setAcquireSession(sessionId));
            Boolean res = dealwith(put, "Failed to lock key: " + key);
            if (res) {
                lockMap.put(key, LockUtils.getLockOwner());
            }
            return res;
        }
    }

    @Override
    public boolean acquireLock(String key, long timeout) {
        long startTime = System.currentTimeMillis();
        try {
            key = apNameSpace(key);
            while (System.currentTimeMillis() - startTime < timeout) {
                if (lock(key)) {
                    return true;
                }
                ThreadUtils.sleep(500);
            }
        } catch (Exception e) {
            throw new RegistryException("Acquire the lock: " + key + " error", e);
        }
        return false;
    }

    @Override
    public boolean releaseLock(String key) {
        try {
            key = apNameSpace(key);
            if(!lockMap.containsKey(key) || !lockMap.get(key).equals(LockUtils.getLockOwner())){
                return false;
            }
            Future<Void> future = consulClient.deleteValue(key);
            dealwith(future, "Failed to release lock key: " + key);
            lockMap.remove(key);
            return true;
        } catch (Exception e) {
            throw new RegistryException("Error releasing lock for key: " + key, e);
        }
    }

    @Override
    public void close() {
        kvCacheMap.values().forEach(Watch::stop);
        destorySession();
        consulClient.close();
        consulConnectionStateListener.close();
    }

    public void destorySession() {
        Future<Void> future = consulClient.destroySession(sessionId);
        dealwith(future, "Failed to destory session");
    }



    public static void main(String[] args) throws InterruptedException {
        ConsulRegistryProperties prop = new ConsulRegistryProperties();
        prop.setUrl("http://127.0.0.1:8500");
        prop.setNamespace("test");
        ConsulRegistry consulRegistry = new ConsulRegistry(prop);
        consulRegistry.start();
//        for (int i = 0; i < 2; i++) {
//            new Thread(){
//                @Override
//                public void run() {
//                    boolean a = consulRegistry.acquireLock("t");
//                    try {
//                        Thread.sleep(15000);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }
//                    boolean b = consulRegistry.acquireLock("t");
//                    System.out.println("a="+a+" b="+b);
//                }
//            }.start();
//        }


//        consulRegistry.put("b/1","测试1",false);
//        Thread.sleep(3000);
////
//        consulRegistry.put("x/1","测试3",true);


//        consulRegistry.put("b/2","测试3",false);
//        consulRegistry.put("b/2/1","测试3",false);

//        Collection<String> b = consulRegistry.children("b");

//        Thread.sleep(3000);
//        consulRegistry.subscribe("b", event -> System.out.println("变更-"+event));
//        Thread.sleep(3000);
//        consulRegistry.put("b/3","测试5",false);
//        Thread.sleep(3000);
//        consulRegistry.unsubscribe("b");
//        Thread.sleep(3000);
//        consulRegistry.delete("b/1");

//        consulRegistry.unsubscribe("b");

//        consulRegistry.delete("b");

//        Thread.sleep(3000);
//        System.out.println("删除session前-"+consulRegistry.get("x/1"));
////      consulRegistry.sessionClient.destroySession(consulRegistry.sessionId);
//        System.out.println("删除session后-"+consulRegistry.get("x/1"));

        LockSupport.park();

    }

}
