package cn.edu.whu.lmars.toxicspider.parser;

import java.util.HashSet;
import java.util.Set;

import cn.edu.whu.lmars.toxicspider.url.WebURL;
/**
 * 纯文本网页类型实体类
 * @author REN
 *
 */
public class TextParseData implements ParseData {

  private String textContent;
  private Set<WebURL> outgoingUrls = new HashSet<>();

  public String getTextContent() {
    return textContent;
  }

  public void setTextContent(String textContent) {
    this.textContent = textContent; 
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
    return textContent;
  }
}