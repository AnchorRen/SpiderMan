package cn.edu.whu.lmars.toxicspider.crawler;

/**
 * 爬虫程序中的几个组件继承了这个类，使类具有可配置性。
 * 
 * @author REN
 */
public abstract class Configurable {

  protected CrawlConfig config;

  protected Configurable(CrawlConfig config) {
    this.config = config;
  }

  public CrawlConfig getConfig() {
    return config;
  }
}