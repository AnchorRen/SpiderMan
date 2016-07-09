package cn.edu.whu.lmars.toxicspider.url;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
/**
 * URL规范化类。对提取出的URL进行处理，使其规范化。
 * 详情：http://en.wikipedia.org/wiki/URL_normalization
 * 
 * @author REN
 */
public class URLCanonicalizer {

  public static String getCanonicalURL(String url) {
    return getCanonicalURL(url, null);
  }

  /**
   * 对url进行处理，获取规范化后的URL
   * @param href
   * @param context
   * @return
   */
  public static String getCanonicalURL(String href, String context) {

    try {
      URL canonicalURL = new URL(UrlResolver.resolveUrl((context == null) ? "" : context, href));

      String host = canonicalURL.getHost().toLowerCase();
      //如果不能解析到主机，则这个链接是无效链接
      if (Objects.equals(host, "")) {
        // This is an invalid Url.
        return null;
      }

      String path = canonicalURL.getPath();

      /*
       * Normalize: no empty segments (i.e., "//"), no segments equal to
       * ".", and no segments equal to ".." that are preceded by a segment
       * not equal to "..".
       */
      path = new URI(path.replace("\\", "/")).normalize().toString();

      int idx = path.indexOf("//");
      while (idx >= 0) {
        path = path.replace("//", "/");
        idx = path.indexOf("//");
      }

      while (path.startsWith("/../")) {
        path = path.substring(3);
      }

      path = path.trim();

      final LinkedHashMap<String, String> params = createParameterMap(canonicalURL.getQuery());
      final String queryString;
      if ((params != null) && !params.isEmpty()) {
        String canonicalParams = canonicalize(params);//规范化拼接Get请求参数为字符串
        queryString = (canonicalParams.isEmpty() ? "" : ("?" + canonicalParams));
      } else {
        queryString = "";
      }

      if (path.isEmpty()) {
        path = "/";
      }

      //去掉默认的80端口: example.com:80 -> example.com
      int port = canonicalURL.getPort();
      if (port == canonicalURL.getDefaultPort()) {
        port = -1;
      }
      //获取协议 http https...
      String protocol = canonicalURL.getProtocol().toLowerCase(); 
      
      String pathAndQueryString = normalizePath(path) + queryString;
      //构建带有参数的完整链接URL
      URL result = new URL(protocol, host, port, pathAndQueryString);
      return result.toExternalForm();

    } catch (MalformedURLException | URISyntaxException ex) {
      return null;
    }
  }

  /**
   * 把链接中的请求参数部分，存储进Map集合中
   * Takes a query string, separates the constituent name-value pairs, and
   * stores them in a LinkedHashMap ordered by their original order.
   *
   * @return Null if there is no query string.
   */
  private static LinkedHashMap<String, String> createParameterMap(final String queryString) {
    if ((queryString == null) || queryString.isEmpty()) {
      return null;
    }
    //获取参数对字符串
    final String[] pairs = queryString.split("&");
    //创建Map存储参数对
    final Map<String, String> params = new LinkedHashMap<>(pairs.length);

    for (final String pair : pairs) {
      if (pair.isEmpty()) {
        continue;
      }
      
      String[] tokens = pair.split("=", 2); 
      switch (tokens.length) {
        case 1: //参数对只有一个值，key或者value
          if (pair.charAt(0) == '=') {
            params.put("", tokens[0]);
          } else {
            params.put(tokens[0], "");
          }
          break;
        case 2: //参数对包包含不为空的key和value
          params.put(tokens[0], tokens[1]);
          break;
      }
    }
    return new LinkedHashMap<>(params);
  }

  /**
   * 规范化Get请求参数。
   *
   * @param paramsMap
   *            key-value类型参数Map
   * @return Canonical form of query string.
   */
  private static String canonicalize(final LinkedHashMap<String, String> paramsMap) {
    if ((paramsMap == null) || paramsMap.isEmpty()) {
      return "";
    }

    final StringBuilder sb = new StringBuilder(100);
    for (Map.Entry<String, String> pair : paramsMap.entrySet()) {
      final String key = pair.getKey().toLowerCase();
      //如果这个key是sessionId的话，舍弃这个sessionID参数。
      if ("jsessionid".equals(key) || "phpsessid".equals(key) || "aspsessionid".equals(key)) {
        continue;
      }
      if (sb.length() > 0) {
        sb.append('&');
      }
      //追加参数，并使用RFC对参数进行解码
      sb.append(percentEncodeRfc3986(pair.getKey()));
      if (!pair.getValue().isEmpty()) {
        sb.append('=');
        sb.append(percentEncodeRfc3986(pair.getValue()));
      }
    }
    return sb.toString();
  }

  /**
   * 依据RFC3986规范，把参数编码编码为%类型
   *
   * @param string
   *            需要被编码的字符串
   * @return Encoded string per RFC 3986.
   */
  private static String percentEncodeRfc3986(String string) {
    try {
      string = string.replace("+", "%2B");
      string = URLDecoder.decode(string, "UTF-8");
      string = URLEncoder.encode(string, "UTF-8");
      return string.replace("+", "%20").replace("*", "%2A").replace("%7E", "~");
    } catch (Exception e) {
      return string;
    }
  }
  /**
   * 规范化URL的path路径
   * @param path
   * @return
   */
  private static String normalizePath(final String path) {
    return path.replace("%7E", "~").replace(" ", "%20");
  }
}