package cn.edu.whu.lmars.toxicspider.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.whu.lmars.toxicspider.crawler.Configurable;
import cn.edu.whu.lmars.toxicspider.crawler.CrawlConfig;
import cn.edu.whu.lmars.toxicspider.crawler.Page;
import cn.edu.whu.lmars.toxicspider.crawler.exceptions.ParseException;
import cn.edu.whu.lmars.toxicspider.url.URLCanonicalizer;
import cn.edu.whu.lmars.toxicspider.url.WebURL;
import cn.edu.whu.lmars.toxicspider.util.Net;
import cn.edu.whu.lmars.toxicspider.util.Util;

/**
 * 网页内容解析类
 * @author REN
 *
 */
public class Parser extends Configurable {

  protected static final Logger logger = LoggerFactory.getLogger(Parser.class);

  private final HtmlParser htmlParser; //html解析器
  private final ParseContext parseContext; //解析上下文对象

  public Parser(CrawlConfig config) {
    super(config);
    htmlParser = new HtmlParser();
    parseContext = new ParseContext();
  }

  /**
   * 网页内容解析
   * 	会将解析后的数据存储到Page中
   * @param page page
   * @param contextURL 解析的URL
   * @throws NotAllowedContentException
   * @throws ParseException
   */
  public void parse(Page page, String contextURL) throws NotAllowedContentException, ParseException {
    if (Util.hasBinaryContent(page.getContentType())) { // 如果是二进制文件
      BinaryParseData parseData = new BinaryParseData();
      if (config.isIncludeBinaryContentInCrawling()) {//允许抓取二进制文件
        if (config.isProcessBinaryContentInCrawling()) {//允许使用 apache tika工具处理二进制文件
          parseData.setBinaryContent(page.getContentData());
        } else {
          parseData.setHtml("<html></html>");
        }
        page.setParseData(parseData);
        if (parseData.getHtml() == null) {
          throw new ParseException();
        }
        parseData.setOutgoingUrls(Net.extractUrls(parseData.getHtml()));
      } else {
        throw new NotAllowedContentException();
      }
    } else if (Util.hasPlainTextContent(page.getContentType())) { // 如果是纯文本
      try {
        TextParseData parseData = new TextParseData();
        if (page.getContentCharset() == null) {
          parseData.setTextContent(new String(page.getContentData()));
        } else {
          parseData.setTextContent(new String(page.getContentData(), page.getContentCharset()));
        }
        parseData.setOutgoingUrls(Net.extractUrls(parseData.getTextContent()));// 使用Net类中的正则表达式匹配URL
        page.setParseData(parseData);
      } catch (Exception e) {
        logger.error("{}, while parsing: {}", e.getMessage(), page.getWebURL().getURL());
        throw new ParseException();
      }
    } else { 
    	// 如果是HTML文档
      Metadata metadata = new Metadata();
      HtmlContentHandler contentHandler = new HtmlContentHandler();
      try (InputStream inputStream = new ByteArrayInputStream(page.getContentData())) {
        htmlParser.parse(inputStream, contentHandler, metadata, parseContext); //解析Html流，内容存储到contentHandler中
      } catch (Exception e) {
        logger.error("{}, while parsing: {}", e.getMessage(), page.getWebURL().getURL());
        throw new ParseException();
      }

      if (page.getContentCharset() == null) {
        page.setContentCharset(metadata.get("Content-Encoding"));
      }

      HtmlParseData parseData = new HtmlParseData();
      parseData.setText(contentHandler.getBodyText().trim());
      parseData.setTitle(metadata.get(DublinCore.TITLE));
      parseData.setMetaTags(contentHandler.getMetaTags());
      // Please note that identifying language takes less than 10 milliseconds
      LanguageIdentifier languageIdentifier = new LanguageIdentifier(parseData.getText());
      page.setLanguage(languageIdentifier.getLanguage());

      Set<WebURL> outgoingUrls = new HashSet<>();

      String baseURL = contentHandler.getBaseUrl();
      if (baseURL != null) {
        contextURL = baseURL;
      }

      int urlCount = 0;
      /**
       * 遍历解析到contentHandler
       * 中的urls,对其进行规范化处理，并处理相对路径问题。
       */
      for (ExtractedUrlAnchorPair urlAnchorPair : contentHandler.getOutgoingUrls()) {

        String href = urlAnchorPair.getHref();
        if ((href == null) || href.trim().isEmpty()) {
          continue;
        }

        String hrefLoweredCase = href.trim().toLowerCase();
        if (!hrefLoweredCase.contains("javascript:") && !hrefLoweredCase.contains("mailto:") &&
            !hrefLoweredCase.contains("@")) {
          String url = URLCanonicalizer.getCanonicalURL(href, contextURL); //对URL进行规范化
          if (url != null) {
            WebURL webURL = new WebURL();
            webURL.setURL(url);
            webURL.setTag(urlAnchorPair.getTag());
            webURL.setAnchor(urlAnchorPair.getAnchor());
            outgoingUrls.add(webURL);
            urlCount++;
            if (urlCount > config.getMaxOutgoingLinksToFollow()) {
              break;
            }
          }
        }
      }
      parseData.setOutgoingUrls(outgoingUrls);

      try {
        if (page.getContentCharset() == null) {
          parseData.setHtml(new String(page.getContentData()));
        } else {
          parseData.setHtml(new String(page.getContentData(), page.getContentCharset()));
        }

        page.setParseData(parseData);
      } catch (UnsupportedEncodingException e) {
        logger.error("error parsing the html: " + page.getWebURL().getURL(), e);
        throw new ParseException();
      }
    }
  }
}