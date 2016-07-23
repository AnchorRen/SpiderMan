package cn.edu.whu.lmars.toxicspider.crawler;

import java.nio.charset.Charset;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;

import cn.edu.whu.lmars.toxicspider.parser.ParseData;
import cn.edu.whu.lmars.toxicspider.url.WebURL;

/**
 * Page类包含了抓取和解析得到的数据
 * @author REN
 *
 */
public class Page {

  /**
   * 网页的URL
   */
  protected WebURL url;

  /**
   * 重定向标志
   */
  protected boolean redirect;

  /**
   * 要重定向的页面
   */
  protected String redirectedToUrl;

  /**
   * 页面访问状态码
   */
  protected int statusCode;

  /**
   * 页面内容的二进制形式
   */
  protected byte[] contentData;

  /**
   * 网页的contentType
   * For example: "text/html; charset=UTF-8"
   */
  protected String contentType;

  /**
   * 网页内容 的编码方式
   * For example: "gzip"
   */
  protected String contentEncoding;

  /**
   * 网页内容字符集
   * For example: "UTF-8"
   */
  protected String contentCharset;

  /**
   * 网页内容使用的语言
   */
  private String language;

  /**
   * 请求头信息，即浏览器F12查看请求的Response-Hears 部分
   */
  protected Header[] fetchResponseHeaders;

  /**
   * 解析器解析的数据
   */
  protected ParseData parseData;


  public Page(WebURL url) {
    this.url = url;
  }

  /**
   * 从抓取的HttpEntity实体加载网页内容
   * 
   * @param entity HttpEntity
   * @throws Exception 加载失败时抛出
   */
  public void load(HttpEntity entity) throws Exception {

    contentType = null;
    Header type = entity.getContentType();
    if (type != null) {
      contentType = type.getValue();
    }

    contentEncoding = null;
    Header encoding = entity.getContentEncoding();
    if (encoding != null) {
      contentEncoding = encoding.getValue();
    }

    Charset charset = ContentType.getOrDefault(entity).getCharset();
    if (charset != null) {
      contentCharset = charset.displayName();
    }

    contentData = EntityUtils.toByteArray(entity);
  }

  public WebURL getWebURL() {
    return url;
  }

  public void setWebURL(WebURL url) {
    this.url = url;
  }

  public boolean isRedirect() {
    return redirect;
  }

  public void setRedirect(boolean redirect) {
    this.redirect = redirect;
  }

  public String getRedirectedToUrl() {
    return redirectedToUrl;
  }

  public void setRedirectedToUrl(String redirectedToUrl) {
    this.redirectedToUrl = redirectedToUrl;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public void setStatusCode(int statusCode) {
    this.statusCode = statusCode;
  }

  /**
   *  返回请求头信息，即浏览器F12查看请求的Response-Hears 部分
   *
   * @return Header数组
   */
  public Header[] getFetchResponseHeaders() {
    return fetchResponseHeaders;
  }

  public void setFetchResponseHeaders(Header[] headers) {
    fetchResponseHeaders = headers;
  }

  /**
   * @return 解析器解析的数据
   */
  public ParseData getParseData() {
    return parseData;
  }

  public void setParseData(ParseData parseData) {
    this.parseData = parseData;
  }

  /**
   * @return 页面内容的二进制形式
   */
  public byte[] getContentData() {
    return contentData;
  }

  public void setContentData(byte[] contentData) {
    this.contentData = contentData;
  }

  /**
   * @return 网页的contentType
   * For example: "text/html; charset=UTF-8"
   */
  public String getContentType() {
    return contentType;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  /**
   * @return 网页的编码方式
   * For example: "gzip"
   */
  public String getContentEncoding() {
    return contentEncoding;
  }

  public void setContentEncoding(String contentEncoding) {
    this.contentEncoding = contentEncoding;
  }

  /**
   * @return 网页的字符集
   * For example: "UTF-8"
   */
  public String getContentCharset() {
    return contentCharset;
  }

  public void setContentCharset(String contentCharset) {
    this.contentCharset = contentCharset;
  }

  /**
   * @return 网页使用的语言
   */
  public String getLanguage() {
    return language;
  }

  public void setLanguage(String language) {
    this.language = language;
  }
}