package cn.edu.whu.lmars.toxicspider.robotstxt;

public class HostDirectives {

  //如果距离上次抓取指令超过24小时，需要重新抓取
  private static final long EXPIRATION_DELAY = 24 * 60 * 1000L;
  //不允许抓取的目录集
  private final RuleSet disallows = new RuleSet();
  //允许抓取的目录集
  private final RuleSet allows = new RuleSet();
  //当前抓取时间
  private final long timeFetched;
  //上次抓取时间
  private long timeLastAccessed;

  public HostDirectives() {
    timeFetched = System.currentTimeMillis();
  }

  public boolean needsRefetch() {
    return ((System.currentTimeMillis() - timeFetched) > EXPIRATION_DELAY);
  }

  public boolean allows(String path) {
    timeLastAccessed = System.currentTimeMillis();
    return !disallows.containsPrefixOf(path) || allows.containsPrefixOf(path);
  }

  public void addDisallow(String path) {
    disallows.add(path);
  }

  public void addAllow(String path) {
    allows.add(path);
  }

  public long getLastAccessTime() {
    return timeLastAccessed;
  }
}