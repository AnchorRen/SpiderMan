package cn.edu.whu.lmars.toxicspider.robotstxt;

public class RobotstxtConfig {

  /**
   * 爬虫是否遵守Robots.txt协议。
   *http://www.robotstxt.org/
   */
  private boolean enabled = false;

  /**
   * user-agent用于指定爬虫名。默认设置为百度爬虫。
   */
  private String userAgentName = "Baiduspider";

  /**
   * 缓存robots.txt主机的最大数量。
   */
  private int cacheSize = 500;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getUserAgentName() {
    return userAgentName;
  }

  public void setUserAgentName(String userAgentName) {
    this.userAgentName = userAgentName;
  }

  public int getCacheSize() {
    return cacheSize;
  }

  public void setCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
  }
}