package cn.edu.whu.lmars.toxicspider.util;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.edu.whu.lmars.toxicspider.url.WebURL;
/**
 * Url提取相关工具类
 * @author REN
 * @date 2016年7月7日 下午2:28:17
 */
public class Net {
	//url链接的正则表达式
  private static final Pattern pattern = initializePattern();

  //提取网页内容中的所有URL链接
  public static Set<WebURL> extractUrls(String input) {
    Set<WebURL> extractedUrls = new HashSet<>();

    if (input != null) {
      Matcher matcher = pattern.matcher(input);
      while (matcher.find()) {
        WebURL webURL = new WebURL();
        String urlStr = matcher.group();
        if (!urlStr.startsWith("http")) {
          urlStr = "http://" + urlStr;
        }

        webURL.setURL(urlStr);
        extractedUrls.add(webURL);
      }
    }

    return extractedUrls;
  }

  /** Singleton like one time call to initialize the Pattern */
  //单例初始化URL提取正则表达式
  private static Pattern initializePattern() {
    return Pattern.compile("\\b(((ht|f)tp(s?)\\:\\/\\/|~\\/|\\/)|www.)" +
                           "(\\w+:\\w+@)?(([-\\w]+\\.)+(com|org|net|gov" +
                           "|mil|biz|info|mobi|name|aero|jobs|museum" +
                           "|travel|[a-z]{2}))(:[\\d]{1,5})?" +
                           "(((\\/([-\\w~!$+|.,=]|%[a-f\\d]{2})+)+|\\/)+|\\?|#)?" +
                           "((\\?([-\\w~!$+|.,*:]|%[a-f\\d{2}])+=?" +
                           "([-\\w~!$+|.,*:=]|%[a-f\\d]{2})*)" +
                           "(&(?:[-\\w~!$+|.,*:]|%[a-f\\d{2}])+=?" +
                           "([-\\w~!$+|.,*:=]|%[a-f\\d]{2})*)*)*" +
                           "(#([-\\w~!$+|.,*:=]|%[a-f\\d]{2})*)?\\b");
  }
}