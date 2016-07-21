package cn.edu.whu.lmars.toxicspider.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class HtmlContentHandler extends DefaultHandler {

  private static final int MAX_ANCHOR_LENGTH = 100;

  /**
   * 网页元素枚举类型
   * @author REN
   *
   */
  private enum Element {
    A,
    AREA,
    LINK,
    IFRAME,
    FRAME,
    EMBED,
    IMG,
    BASE,
    META,
    BODY
  }
  
  /**
   * 静态内部类：
   * 		把页面元素都放到Map中，key--element名小写，value--Element元素对象
   * @author REN
   *
   */
  private static class HtmlFactory {
    private static final Map<String, Element> name2Element;

    static {
      name2Element = new HashMap<>();
      for (Element element : Element.values()) {
        name2Element.put(element.toString().toLowerCase(), element);
      }
    }

    public static Element getElement(String name) {
      return name2Element.get(name);
    }
  }

  private String base;
  private String metaRefresh;
  private String metaLocation;
  private final Map<String, String> metaTags = new HashMap<>();

  private boolean isWithinBodyElement;
  //<body>中Text
  private final StringBuilder bodyText;
  //页面中链接类集合
  private final List<ExtractedUrlAnchorPair> outgoingUrls;

  private ExtractedUrlAnchorPair curUrl = null;
  private boolean anchorFlag = false;
  private final StringBuilder anchorText = new StringBuilder();

  public HtmlContentHandler() {
    isWithinBodyElement = false;
    bodyText = new StringBuilder();
    outgoingUrls = new ArrayList<>();
  }

  /**
   * 对网页元素进行处理
   */
  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
    Element element = HtmlFactory.getElement(localName);
    
    //如果是URL链接
    if ((element == Element.A) || (element == Element.AREA) || (element == Element.LINK)) {
      String href = attributes.getValue("href");
      if (href != null) {
        anchorFlag = true;
        addToOutgoingUrls(href, localName);

      }
    } else if (element == Element.IMG) { //如果是图片
      String imgSrc = attributes.getValue("src");
      if (imgSrc != null) {
        addToOutgoingUrls(imgSrc, localName);

      }
    } else if ((element == Element.IFRAME) || (element == Element.FRAME) || (element == Element.EMBED)) {
      String src = attributes.getValue("src");
      if (src != null) {
        addToOutgoingUrls(src, localName);

      }
    } else if (element == Element.BASE) {
      if (base != null) { // We only consider the first occurrence of the Base element.
        String href = attributes.getValue("href");
        if (href != null) {
          base = href;
        }
      }
    } else if (element == Element.META) {
      String equiv = attributes.getValue("http-equiv");
      if (equiv == null) { // This condition covers several cases of XHTML meta
        equiv = attributes.getValue("name");
      }

      String content = attributes.getValue("content");
      if ((equiv != null) && (content != null)) {
        equiv = equiv.toLowerCase();
        metaTags.put(equiv, content);

        // http-equiv="refresh" content="0;URL=http://foo.bar/..."
        if ("refresh".equals(equiv) && (metaRefresh == null)) {
          int pos = content.toLowerCase().indexOf("url=");
          if (pos != -1) {
            metaRefresh = content.substring(pos + 4);
            addToOutgoingUrls(metaRefresh, localName);
          }
        }

        // http-equiv="location" content="http://foo.bar/..."
        if ("location".equals(equiv) && (metaLocation == null)) {
          metaLocation = content;
          addToOutgoingUrls(metaLocation, localName);
        }
      }
    } else if (element == Element.BODY) {
      isWithinBodyElement = true;
    }
  }

  /**
   * 添加页面中的链接到链接集合中
   * @param href 链接
   * @param tag 标签
   */
  private void addToOutgoingUrls(String href, String tag) {
    curUrl = new ExtractedUrlAnchorPair();
    curUrl.setHref(href);
    curUrl.setTag(tag);
    outgoingUrls.add(curUrl);
  }
  /**
   * 
   */
  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    Element element = HtmlFactory.getElement(localName);
    if ((element == Element.A) || (element == Element.AREA) || (element == Element.LINK)) {
      anchorFlag = false;
      if (curUrl != null) {
        String anchor = anchorText.toString().replaceAll("\n", " ").replaceAll("\t", " ").trim();
        if (!anchor.isEmpty()) {
          if (anchor.length() > MAX_ANCHOR_LENGTH) {
            anchor = anchor.substring(0, MAX_ANCHOR_LENGTH) + "...";
          }
          curUrl.setTag(localName);
          curUrl.setAnchor(anchor);
        }
        anchorText.delete(0, anchorText.length());
      }
      curUrl = null;
    } else if (element == Element.BODY) {
      isWithinBodyElement = false;
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    if (isWithinBodyElement) {
      bodyText.append(ch, start, length);

      if (anchorFlag) {
        anchorText.append(new String(ch, start, length));
      }
    }
  }

  public String getBodyText() {
    return bodyText.toString();
  }

  public List<ExtractedUrlAnchorPair> getOutgoingUrls() {
    return outgoingUrls;
  }

  public String getBaseUrl() {
    return base;
  }

  public Map<String, String> getMetaTags() {
    return metaTags;
  }
}
