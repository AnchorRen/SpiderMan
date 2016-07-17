package cn.edu.whu.lmars.toxicspider.parser;

import java.util.Map;
import java.util.Set;

import cn.edu.whu.lmars.toxicspider.url.WebURL;
/**
 * HTML文档类型实体类
 * @author REN
 *
 */
public class HtmlParseData implements ParseData {

  private String html; //HTML文档
  private String text; //text
  private String title; //title标签
  private Map<String, String> metaTags; //meta标签集合

  private Set<WebURL> outgoingUrls;

  public String getHtml() {
    return html;
  }

  public void setHtml(String html) {
    this.html = html;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public Map<String, String> getMetaTags() {
    return metaTags;
  }

  public void setMetaTags(Map<String, String> metaTags) {
    this.metaTags = metaTags;
  }

  @Override
  public Set<WebURL> getOutgoingUrls() {
    return outgoingUrls;
  }

  @Override
  public void setOutgoingUrls(Set<WebURL> outgoingUrls) {
    this.outgoingUrls = outgoingUrls;
  }

  @Override
  public String toString() {
    return text;
  }
}