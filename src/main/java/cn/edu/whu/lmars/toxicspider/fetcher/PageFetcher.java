package cn.edu.whu.lmars.toxicspider.fetcher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.net.ssl.SSLContext;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.whu.lmars.toxicspider.crawler.Configurable;
import cn.edu.whu.lmars.toxicspider.crawler.CrawlConfig;
import cn.edu.whu.lmars.toxicspider.crawler.authentication.AuthInfo;
import cn.edu.whu.lmars.toxicspider.crawler.authentication.BasicAuthInfo;
import cn.edu.whu.lmars.toxicspider.crawler.authentication.FormAuthInfo;
import cn.edu.whu.lmars.toxicspider.crawler.authentication.NtAuthInfo;
import cn.edu.whu.lmars.toxicspider.crawler.exceptions.PageBiggerThanMaxSizeException;
import cn.edu.whu.lmars.toxicspider.url.URLCanonicalizer;
import cn.edu.whu.lmars.toxicspider.url.WebURL;

/**
 * 网页抓取类。
 * 
 * @author REN
 *
 */
public class PageFetcher extends Configurable {
  protected static final Logger logger = LoggerFactory.getLogger(PageFetcher.class);

  protected PoolingHttpClientConnectionManager connectionManager; //连接管理器
  protected CloseableHttpClient httpClient; //httpClient
  protected final Object mutex = new Object(); //锁
  protected long lastFetchTime = 0; //上次抓取时间
  protected IdleConnectionMonitorThread connectionMonitorThread = null; //闲置链接监视线程

  /**
   * 构造方法
   * @param config CrawlConfig 爬虫配置参数
   */
  public PageFetcher(CrawlConfig config) {
    super(config);

    RequestConfig requestConfig =
        RequestConfig.custom().setExpectContinueEnabled(false).setCookieSpec(CookieSpecs.DEFAULT)
                     .setRedirectsEnabled(false).setSocketTimeout(config.getSocketTimeout())
                     .setConnectTimeout(config.getConnectionTimeout()).build();

    RegistryBuilder<ConnectionSocketFactory> connRegistryBuilder = RegistryBuilder.create();
    connRegistryBuilder.register("http", PlainConnectionSocketFactory.INSTANCE);
    if (config.isIncludeHttpsPages()) {
      try { 
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy() {
          @Override
          public boolean isTrusted(final X509Certificate[] chain, String authType) {
            return true;
          }
        }).build();
        SSLConnectionSocketFactory sslsf =
            new SSLConnectionSocketFactory(sslContext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        connRegistryBuilder.register("https", sslsf);
      } catch (Exception e) {
        logger.warn("Exception thrown while trying to register https");
        logger.debug("Stacktrace", e);
      }
    }

    Registry<ConnectionSocketFactory> connRegistry = connRegistryBuilder.build();
    connectionManager = new PoolingHttpClientConnectionManager(connRegistry);
    connectionManager.setMaxTotal(config.getMaxTotalConnections());
    connectionManager.setDefaultMaxPerRoute(config.getMaxConnectionsPerHost());

    HttpClientBuilder clientBuilder = HttpClientBuilder.create();
    clientBuilder.setDefaultRequestConfig(requestConfig);
    clientBuilder.setConnectionManager(connectionManager);
    clientBuilder.setUserAgent(config.getUserAgentString());
    clientBuilder.setDefaultHeaders(config.getDefaultHeaders());

    if (config.getProxyHost() != null) {
      if (config.getProxyUsername() != null) {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(config.getProxyHost(), config.getProxyPort()),
                                           new UsernamePasswordCredentials(config.getProxyUsername(),
                                                                           config.getProxyPassword()));
        clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      }

      HttpHost proxy = new HttpHost(config.getProxyHost(), config.getProxyPort());
      clientBuilder.setProxy(proxy);
      logger.debug("Working through Proxy: {}", proxy.getHostName());
    }

    httpClient = clientBuilder.build();
    if ((config.getAuthInfos() != null) && !config.getAuthInfos().isEmpty()) {
      doAuthetication(config.getAuthInfos());
    }

    if (connectionMonitorThread == null) {
      connectionMonitorThread = new IdleConnectionMonitorThread(connectionManager);
    }
    connectionMonitorThread.start();
  }

  /**
   * 授权处理
   * @param authInfos
   */
  private void doAuthetication(List<AuthInfo> authInfos) {
    for (AuthInfo authInfo : authInfos) {
      if (authInfo.getAuthenticationType() == AuthInfo.AuthenticationType.BASIC_AUTHENTICATION) {
        doBasicLogin((BasicAuthInfo) authInfo);
      } else if (authInfo.getAuthenticationType() == AuthInfo.AuthenticationType.NT_AUTHENTICATION) {
        doNtLogin((NtAuthInfo) authInfo);
      } else {
        doFormLogin((FormAuthInfo) authInfo);
      }
    }
  }

  /**
   * BASIC authentication<br/>
   * Official Example: https://hc.apache.org/httpcomponents-client-ga/httpclient/examples/org/apache/http/examples
   * /client/ClientAuthentication.java
   * */
  private void doBasicLogin(BasicAuthInfo authInfo) {
    logger.info("BASIC authentication for: " + authInfo.getLoginTarget());
    HttpHost targetHost = new HttpHost(authInfo.getHost(), authInfo.getPort(), authInfo.getProtocol());
    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()),
                                 new UsernamePasswordCredentials(authInfo.getUsername(), authInfo.getPassword()));
    httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
  }

  /**
   * Do NT auth for Microsoft AD sites.
   */
  private void doNtLogin(NtAuthInfo authInfo) {
    logger.info("NT authentication for: " + authInfo.getLoginTarget());
    HttpHost targetHost = new HttpHost(authInfo.getHost(), authInfo.getPort(), authInfo.getProtocol());
    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    try {
      credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()),
              new NTCredentials(authInfo.getUsername(), authInfo.getPassword(),
                      InetAddress.getLocalHost().getHostName(), authInfo.getDomain()));
    } catch (UnknownHostException e) {
      logger.error("Error creating NT credentials", e);
    }
    httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
  }

  /**
   * FORM authentication<br/>
   * Official Example:
   *  https://hc.apache.org/httpcomponents-client-ga/httpclient/examples/org/apache/http/examples/client/ClientFormLogin.java
   */
  private void doFormLogin(FormAuthInfo authInfo) {
    logger.info("FORM authentication for: " + authInfo.getLoginTarget());
    String fullUri =
        authInfo.getProtocol() + "://" + authInfo.getHost() + ":" + authInfo.getPort() + authInfo.getLoginTarget();
    HttpPost httpPost = new HttpPost(fullUri);
    List<NameValuePair> formParams = new ArrayList<>();
    formParams.add(new BasicNameValuePair(authInfo.getUsernameFormStr(), authInfo.getUsername()));
    formParams.add(new BasicNameValuePair(authInfo.getPasswordFormStr(), authInfo.getPassword()));

    try {
      UrlEncodedFormEntity entity = new UrlEncodedFormEntity(formParams, "UTF-8");
      httpPost.setEntity(entity);
      httpClient.execute(httpPost);
      logger.debug("Successfully Logged in with user: " + authInfo.getUsername() + " to: " + authInfo.getHost());
    } catch (UnsupportedEncodingException e) {
      logger.error("Encountered a non supported encoding while trying to login to: " + authInfo.getHost(), e);
    } catch (ClientProtocolException e) {
      logger.error("While trying to login to: " + authInfo.getHost() + " - Client protocol not supported", e);
    } catch (IOException e) {
      logger.error("While trying to login to: " + authInfo.getHost() + " - Error making request", e);
    }
  }

  public PageFetchResult fetchPage(WebURL webUrl)
      throws InterruptedException, IOException, PageBiggerThanMaxSizeException {
    // Getting URL, setting headers & content
    PageFetchResult fetchResult = new PageFetchResult();
    String toFetchURL = webUrl.getURL();
    HttpUriRequest request = null;
    try {
      request = newHttpUriRequest(toFetchURL);
      // 通过加一个同步锁，保证间隔抓取时间不超过规定时间
      synchronized (mutex) {
        long now = (new Date()).getTime();
        if ((now - lastFetchTime) < config.getPolitenessDelay()) {
          Thread.sleep(config.getPolitenessDelay() - (now - lastFetchTime));
        }
        lastFetchTime = (new Date()).getTime();
      }

      CloseableHttpResponse response = httpClient.execute(request);
      fetchResult.setEntity(response.getEntity());
      fetchResult.setResponseHeaders(response.getAllHeaders());

      //设置相应状态码
      int statusCode = response.getStatusLine().getStatusCode();

      //处理重定向( 3xx )
      if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode == HttpStatus.SC_MOVED_TEMPORARILY ||
          statusCode == HttpStatus.SC_MULTIPLE_CHOICES || statusCode == HttpStatus.SC_SEE_OTHER ||
          statusCode == HttpStatus.SC_TEMPORARY_REDIRECT ||
          statusCode == 308) { //https://issues.apache.org/jira/browse/HTTPCORE-389

        Header header = response.getFirstHeader("Location");
        if (header != null) {
          String movedToUrl = URLCanonicalizer.getCanonicalURL(header.getValue(), toFetchURL);
          fetchResult.setMovedToUrl(movedToUrl);
        }
      } else if (statusCode >= 200 && statusCode <= 299) { // 如果是2XX
        fetchResult.setFetchedUrl(toFetchURL);
        String uri = request.getURI().toString();
        if (!uri.equals(toFetchURL)) {
          if (!URLCanonicalizer.getCanonicalURL(uri).equals(toFetchURL)) {
            fetchResult.setFetchedUrl(uri);
          }
        }

        //检查内容长度是否超出上限值
        if (fetchResult.getEntity() != null) {
          long size = fetchResult.getEntity().getContentLength();
          if (size == -1) {
            Header length = response.getLastHeader("Content-Length");
            if (length == null) {
              length = response.getLastHeader("Content-length");
            }
            if (length != null) {
              size = Integer.parseInt(length.getValue());
            }
          }
          if (size > config.getMaxDownloadSize()) {
            response.close();
            throw new PageBiggerThanMaxSizeException(size);
          }
        }
      }

      fetchResult.setStatusCode(statusCode);
      return fetchResult;

    } finally {
      if ((fetchResult.getEntity() == null) && (request != null)) {
        request.abort();
      }
    }
  }

  public synchronized void shutDown() {
    if (connectionMonitorThread != null) {
      connectionManager.shutdown();
      connectionMonitorThread.shutdown();
    }
  }

  /**
   * 对于给定的一个URl创建一个新的HttpUriRequest 请求。默认的是创建HttpGet请求不带有任何配置参数。
   * 子类可以通过覆盖这个方法提供自己的逻辑
   * @param 要抓取的Url
   * @return  HttpUriRequest对象 
   */
  protected HttpUriRequest newHttpUriRequest(String url) {
    return new HttpGet(url);
  }

}
