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

import com.orbitz.consul.model.session.ImmutableSession;

import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.plugin.registry.consul.tmp.SessionRequest;
import org.apache.dolphinscheduler.registry.api.ConnectionListener;
import org.apache.dolphinscheduler.registry.api.Event;
import org.apache.dolphinscheduler.registry.api.Registry;
import org.apache.dolphinscheduler.registry.api.RegistryException;
import org.apache.dolphinscheduler.registry.api.SubscribeListener;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import lombok.NonNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//
//import com.orbitz.consul.cache.KVCache;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.option.ImmutablePutOptions;
import com.orbitz.consul.option.PutOptions;

public final class ConsulRegistry implements Registry {

   private static final Logger log = LoggerFactory.getLogger(ConsulRegistry.class);
   private final ConsulClient consulClient;
   private final Map<String, KVCache> kvCacheMap = new ConcurrentHashMap<>();
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
      String sessionTimeout = formatDuration(prop.getSessionTimeout());
      Duration sessionRefreshTime = prop.getSessionTimeout();
      String namespace = prop.getNamespace();
      String userName = prop.getUserName();
      String password = prop.getPassword();
      String aclToken = prop.getAclToken();
      ConsulClientImpl.ConsulProperties p = new ConsulClientImpl.ConsulProperties();
      try {
         p.setUrl(new URL(url));
      } catch (MalformedURLException e) {
         throw new RuntimeException(e);
      }
      this.consulClient = new ConsulClientImpl(p);
      this.consulConnectionStateListener = new ConsulConnectionStateListener(consulClient);
      this.namespace = namespace;
      consulConnectionStateListener.start();
      createConsulSession(sessionTimeout);
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
            } finally{
               Thread.sleep(sessionRefreshTime.toMillis());
            }
         }
      });
   }

   @Override
   public void start() {
      // The start has been set in the constructor
   }

   private void createConsulSession(String sessionTimeout) {
      try {
         sessionId = consulClient.createSession(SessionRequest.builder().name("consul_registry_session")
                 .lockDelay("15s").ttl(sessionTimeout).behavior("delete").build());
      } catch (Exception e) {
         throw new RegistryException("Failed to create Consul session", e);
      }
   }

   @Override
   public boolean isConnected() {
      try {
         consulClient.ping();
         return true;
      } catch (Exception e) {
         return false;
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

      KVCache kvCache = KVCache.newCache(consulClient, path, 2);
      kvCache.addListener(new ConsulSubscribeDataListener(new MidSubscriveListener(listener)));
      kvCacheMap.put(path, kvCache);
      kvCache.start();
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

//   @Override
//   public void unsubscribe(String path) {
//      path = apNameSpace(path);
//
//      if (kvCacheMap.containsKey(path)) {
//         kvCacheMap.get(path).stop();
//         kvCacheMap.remove(path);
//      }
//   }

   @Override
   public void addConnectionStateListener(ConnectionListener listener) {
      consulConnectionStateListener.addConnectionListener(listener);
   }

   @Override
   public String get(String key) throws RegistryException {
      key = apNameSpace(key);
      try {
         String value = consulClient.getValue(key);
         if(value == null){
            throw  new RegistryException("Key not found");
         }
         return value;
      } catch (RegistryException e){
         throw e;
      } catch (Exception e) {
         throw new RegistryException("Error getting value for key: " + key, e);
      }
   }

   @Override
   public void put(String key, String value, boolean deleteOnDisconnect) {
      try {
         key = apNameSpace(key);
         if (deleteOnDisconnect) {
            consulClient.putValue(key, value,sessionId);
         } else {
            consulClient.putValue(key, value);
         }
      } catch (RegistryException e){
         throw e;
      } catch (Exception e) {
         throw new RegistryException("Error putting key: " + key, e);
      }
   }

   @Override
   public void delete(String key) {
      key = apNameSpace(key);
      try {
         consulClient.deleteKey(key);
      } catch (Exception e) {
         throw new RegistryException("Error deleting key: " + key, e);
      }
   }

   @Override
   public Collection<String> children(String key) {
      try {
         key = apNameSpace(key);

         String prefix = key.endsWith(FOLDER_SEPARATOR) ? key : key + FOLDER_SEPARATOR;
         List<String> keys = consulClient.getKeys(prefix);
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
         return consulClient.getValue(key) != null;
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
         boolean b = consulClient.acquireLock(key, sessionId);
         if (b) {
            lockMap.put(key, LockUtils.getLockOwner());
         }
         return b;
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
         consulClient.releaseLock(key, sessionId);
         lockMap.remove(key);
         return true;
      } catch (Exception e) {
         throw new RegistryException("Error releasing lock for key: " + key, e);
      }
   }

   @Override
   public void close() {
//      kvCacheMap.values().forEach(KVCache::stop);
//      consulClient.destroySession(sessionId);
//      sessionId = null;
//      consulClient.destroy();
//      consulConnectionStateListener.close();
   }


   public static String formatDuration(Duration duration) {
      long seconds = duration.getSeconds();
      long absSeconds = Math.abs(seconds);

      if (absSeconds % 3600 == 0) {
         return String.format("%dh", absSeconds / 3600);
      } else if (absSeconds % 60 == 0) {
         return String.format("%dm", absSeconds / 60);
      } else {
         return String.format("%ds", absSeconds);
      }
   }


   public static void main(String[] args) throws MalformedURLException {
      ConsulClientImpl.ConsulProperties consulProperties = new ConsulClientImpl.ConsulProperties();
      consulProperties.setUrl(new URL("http://127.0.0.1:8500"));

      ConsulClient cc = new ConsulClientImpl(consulProperties);
      String session = cc.createSession(SessionRequest.builder().name("consul_registry_session")
              .lockDelay("15s").ttl("15s").behavior("delete").build());
      System.out.println(session);

      String key = "/nodes/master" + System.nanoTime();
      cc.putValue(key,"127.0.0.1:8080");
      String value1 = cc.getValue(key);






   }



}
