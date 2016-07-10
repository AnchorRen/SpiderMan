package cn.edu.whu.lmars.toxicspider.robotstxt;

import java.util.StringTokenizer;

public class RobotstxtParser {

  private static final String PATTERNS_USERAGENT = "(?i)^User-agent:.*";
  private static final String PATTERNS_DISALLOW = "(?i)Disallow:.*";
  private static final String PATTERNS_ALLOW = "(?i)Allow:.*";

  private static final int PATTERNS_USERAGENT_LENGTH = 11;
  private static final int PATTERNS_DISALLOW_LENGTH = 9;
  private static final int PATTERNS_ALLOW_LENGTH = 6;

  public static HostDirectives parse(String content, String myUserAgent) {

    HostDirectives directives = null;
    boolean inMatchingUserAgent = false;
    /*
     * The string tokenizer class allows an application to break a string into tokens. 
     * The tokenization method is much simpler than the one used by the StreamTokenizer class. 
     * The StringTokenizer methods do not distinguish among identifiers, numbers, 
     * and quoted strings, nor do they recognize and skip comments. 
     */
    StringTokenizer st = new StringTokenizer(content, "\n\r");
    while (st.hasMoreTokens()) {
      String line = st.nextToken();
      //去掉注释部分
      int commentIndex = line.indexOf('#');
      if (commentIndex > -1) {
        line = line.substring(0, commentIndex);
      }

      // 去除带有Html标签
      line = line.replaceAll("<[^>]+>", "");

      line = line.trim();

      if (line.isEmpty()) {
        continue;
      }

      if (line.matches(PATTERNS_USERAGENT)) {
        String ua = line.substring(PATTERNS_USERAGENT_LENGTH).trim().toLowerCase();
        inMatchingUserAgent = "*".equals(ua) || ua.contains(myUserAgent.toLowerCase());
      } else if (line.matches(PATTERNS_DISALLOW)) {
        if (!inMatchingUserAgent) {
          continue;
        }
        String path = line.substring(PATTERNS_DISALLOW_LENGTH).trim();
        if (path.endsWith("*")) {
          path = path.substring(0, path.length() - 1);
        }
        path = path.trim();
        if (!path.isEmpty()) {
          if (directives == null) {
            directives = new HostDirectives();
          }
          directives.addDisallow(path);
        }
      } else if (line.matches(PATTERNS_ALLOW)) {
        if (!inMatchingUserAgent) {
          continue;
        }
        String path = line.substring(PATTERNS_ALLOW_LENGTH).trim();
        if (path.endsWith("*")) {
          path = path.substring(0, path.length() - 1);
        }
        path = path.trim();
        if (directives == null) {
          directives = new HostDirectives();
        }
        directives.addAllow(path);
      }
    }

    return directives;
  }
}