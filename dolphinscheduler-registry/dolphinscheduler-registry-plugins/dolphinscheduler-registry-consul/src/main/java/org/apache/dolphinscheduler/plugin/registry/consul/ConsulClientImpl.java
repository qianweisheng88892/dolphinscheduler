package org.apache.dolphinscheduler.plugin.registry.consul;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedLongs;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.cache.TimeoutInterceptor;
import com.orbitz.consul.config.ClientConfig;
import com.orbitz.consul.model.session.SessionInfo;
import com.orbitz.consul.monitoring.ClientEventCallback;
import com.orbitz.consul.monitoring.ClientEventHandler;
import com.orbitz.consul.option.QueryOptions;
import com.orbitz.consul.util.Jackson;

import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.plugin.registry.consul.tmp.SessionRequest;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.Data;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.internal.Util;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.DELETE;
import retrofit2.http.GET;
import retrofit2.http.PUT;
import retrofit2.http.Path;
import retrofit2.http.QueryMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.orbitz.consul.util.Strings.trimLeadingSlash;

/**
 * 钱维升 创建于 2024/7/13.
 */

public class ConsulClientImpl implements ConsulClient {

   Api api;
   Http http;
   Charset charset = Charset.defaultCharset();

   @Data
   public static class ConsulProperties{
      String aclToken;
      String userName;
      String password;
      URL url;
   }

   public ConsulClientImpl(ConsulProperties prop) {
      URL url = prop.getUrl();
      String username = prop.getUserName();
      String password = prop.getPassword();
      String aclToken = prop.getAclToken();
      Retrofit retrofit;
      ExecutorService executorService = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS,
              new SynchronousQueue<>(), Util.threadFactory("OkHttp Dispatcher", true));
      ConnectionPool connectionPool = new ConnectionPool();
      ClientConfig config =  new ClientConfig();
      OkHttpClient.Builder okHttpClientBuilder = createOkHttpClient(executorService, connectionPool, config);
      addAclTokenInterceptor(aclToken, okHttpClientBuilder);
      addBasicAuthInterceptor(username, password, okHttpClientBuilder);
      OkHttpClient okHttpClient = okHttpClientBuilder.build();
      try {
         retrofit = createRetrofit(
                 buildUrl(url),
                 Jackson.MAPPER,
                 okHttpClient);
      } catch (MalformedURLException e) {
         throw new RuntimeException(e);
      }
      this.api = retrofit.create(Api.class);
      ClientEventCallback eventCallback = new ClientEventCallback(){};
      this.http = new Http(new ClientEventHandler("consul", eventCallback));

   }

   private void addBasicAuthInterceptor(String username, String password, OkHttpClient.Builder okHttpClientBuilder) {
      if(StringUtils.isNoneBlank(username)){
         String credentials = username + ":" + password;
         final String basic = "Basic " + BaseEncoding.base64().encode(credentials.getBytes());
         okHttpClientBuilder.addInterceptor(chain -> {
            Request original = chain.request();

            Request.Builder requestBuilder = original.newBuilder()
                    .header("Authorization", basic)
                    .method(original.method(), original.body());

            Request request = requestBuilder.build();
            return chain.proceed(request);
         });
      }
   }

   private void addAclTokenInterceptor(String aclToken, OkHttpClient.Builder okHttpClientBuilder) {
      if(StringUtils.isNoneBlank(aclToken)){
         okHttpClientBuilder.addInterceptor(chain -> {
            Request original = chain.request();
            HttpUrl originalUrl = original.url();
            String rewrittenUrl;
            if (originalUrl.queryParameterNames().isEmpty()) {
               rewrittenUrl = originalUrl.url().toExternalForm() + "?token=" + aclToken;
            } else {
               rewrittenUrl = originalUrl.url().toExternalForm() + "&token=" + aclToken;
            }
            Request.Builder requestBuilder = original.newBuilder()
                    .url(rewrittenUrl)
                    .method(original.method(), original.body());

            Request request = requestBuilder.build();
            return chain.proceed(request);
         });
      }
   }


   @Override
   public String createSession(SessionRequest sessionRequest) {
      Map<String, String> res = http.extract(api.createSession(sessionRequest, Collections.emptyMap()));
      return res.get("ID");
   }

   @Override
   public void renewSession(String sessionId) {
      http.extract(api.renewSession(sessionId, ImmutableMap.of(), Collections.emptyMap()));
   }

   @Override
   public void destroySession(String sessionId) {
      http.handle(api.destroySession(sessionId, Collections.emptyMap()));
   }

   @Override
   public boolean acquireLock(String key, String session) {
      checkArgument(StringUtils.isNotEmpty(key), "Key must be defined");
      Map<String, Object> query = new HashMap<>();
      query.put("acquire",session);
      query.put("flags",UnsignedLongs.toString(0));
      return http.extract(api.putValue(trimLeadingSlash(key), query));
   }

   @Override
   public boolean releaseLock(String key,String session) {
      checkArgument(StringUtils.isNotEmpty(key), "Key must be defined");
      Map<String, Object> query = new HashMap<>();
      query.put("release",session);
      query.put("flags",UnsignedLongs.toString(0));
      return http.extract(api.putValue(trimLeadingSlash(key), query));
   }


   @Override
   public String getValue(String key) {
      Optional<Map<String, String>> res = getSingleValue(http.extract(api.getValue(trimLeadingSlash(key), Collections.emptyMap()),404));
      String s = res.orElse(new HashMap<>()).get("Value");
      if (s != null) {
         s = new String(BaseEncoding.base64().decode(s), charset);
      }
      return s;
   }

   @Override
   public List<Map<String, String>> getValues(String key) {
      Map<String, Object> query = new HashMap<>();
      query.put("recurse", "true");
      query.put("wait","");
      query.put("index","");
      List<Map<String, String>> extract = http.extract(api.getValue(trimLeadingSlash(key), query), 404);
      return extract;
   }


   @Override
   public List<String> getKeys(String prefix) {
      Map<String, Object> query = new HashMap<>();
      query.put("keys", "true");
      List<String> result = http.extract(api.getKeys(trimLeadingSlash(prefix), query));
      return result == null ? Collections.emptyList() : result;
   }

   @Override
   public void deleteKey(String key) {
      http.handle(api.deleteValues(trimLeadingSlash(key), new HashMap<>()));
   }

   @Override
   public boolean putValue(String key, String value) {
      return http.extract(api.putValue(trimLeadingSlash(key),
              RequestBody.create(MediaType.parse("text/plain; charset=" + charset.name()), value), Collections.emptyMap()));
   }

   @Override
   public boolean putValue(String key, String value, String session) {
      Map<String, Object> query = new HashMap<>();
      query.put("acquire",session);
      return http.extract(api.putValue(trimLeadingSlash(key), RequestBody.create(MediaType.parse("text/plain; charset=" + charset.name()), value), query));
   }

   @Override
   public void ping() {
      try {
         retrofit2.Response<Void> response = api.ping().execute();
         if (!response.isSuccessful()) {
            throw new ConsulException(String.format("Error pinging Consul: %s",
                    response.message()));
         }
      } catch (Exception ex) {
         throw new ConsulException("Error connecting to Consul", ex);
      }
   }



   private Optional<Map<String, String>> getSingleValue(List<Map<String, String>> values) {
      return values != null && values.size() != 0 ? Optional.of(values.get(0)) : Optional.empty();
   }


   private String buildUrl(URL url) {
      return url.toExternalForm().replaceAll("/$", "") + "/v1/";
   }
   private Retrofit createRetrofit(String url, ObjectMapper mapper, OkHttpClient okHttpClient) throws MalformedURLException {
      final URL consulUrl = new URL(url);
      return new Retrofit.Builder()
              .baseUrl(new URL(consulUrl.getProtocol(), consulUrl.getHost(),
                      consulUrl.getPort(), consulUrl.getFile()).toExternalForm())
              .addConverterFactory(JacksonConverterFactory.create(mapper))
              .client(okHttpClient)
              .build();
   }

   private OkHttpClient.Builder createOkHttpClient(ExecutorService executorService, ConnectionPool connectionPool, ClientConfig clientConfig) {
      final OkHttpClient.Builder builder = new OkHttpClient.Builder();
      builder.addInterceptor(new TimeoutInterceptor(clientConfig.getCacheConfig()));
      Dispatcher dispatcher = new Dispatcher(executorService);
      dispatcher.setMaxRequests(Integer.MAX_VALUE);
      dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);
      builder.dispatcher(dispatcher);
      builder.connectionPool(connectionPool);
      return builder;
   }




   interface Api {

      @PUT("session/create")
      Call<Map<String,String>> createSession(@Body SessionRequest value,
                                                 @QueryMap Map<String, String> query);

      @PUT("session/renew/{sessionId}")
      Call<List<Map<String,String>>> renewSession(@Path("sessionId") String sessionId,
                                           @Body Map<String, String> body,
                                           @QueryMap Map<String, String> query);

      @PUT("session/destroy/{sessionId}")
      Call<Void> destroySession(@Path("sessionId") String sessionId,
                                @QueryMap Map<String, String> query);

      @GET("session/info/{sessionId}")
      Call<List<SessionInfo>> getSessionInfo(@Path("sessionId") String sessionId,
                                             @QueryMap Map<String, String> query);

      @GET("session/list")
      Call<List<SessionInfo>> listSessions(@QueryMap Map<String, String> query);

      @GET("status/leader")
      Call<Void> ping();


      @GET("kv/{key}")
      Call<List<Map<String,String>>> getValue(@Path("key") String key,
                                 @QueryMap Map<String, Object> query);

      @GET("kv/{key}")
      Call<List<String>> getKeys(@Path("key") String key,
                                 @QueryMap Map<String, Object> query);

      @DELETE("kv/{key}")
      Call<Void> deleteValues(@Path("key") String key,
                              @QueryMap Map<String, Object> query);

      @PUT("kv/{key}")
      Call<Boolean> putValue(@Path("key") String key,
                             @Body RequestBody data,
                             @QueryMap Map<String, Object> query);
      @PUT("kv/{key}")
      Call<Boolean> putValue(@Path("key") String key,
                             @QueryMap Map<String, Object> query);

   }
}
