package cn.edu.whu.lmars.toxicspider.url;

import java.io.Serializable;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * URL实体类
 * @author REN
 */

@Entity
public class WebURL implements Serializable {

  private static final long serialVersionUID = 1L;

  @PrimaryKey
  private String url;

  private int docid; //文档编号（唯一编号0）
  private int parentDocid; //父页面的文档编号
  private String parentUrl; //父网页的URL地址
  private short depth; //当前URL的爬取深度
  private String domain; //域名
  private String subDomain; //子域名
  private String path; //URL路径
  private String anchor; //此网址的说明<a>anchor</a>
  private byte priority; //优先级，数值越小优先级越高
  private String tag; //标签？


  /**
   * @return 分配给此Url的唯一文档Id 
   */
  public int getDocid() {
    return docid;
  }

  public void setDocid(int docid) {
    this.docid = docid;
  }

  /**
   * @return Url
   */
  public String getURL() {
    return url;
  }

  public void setURL(String url) {
    this.url = url;

    int domainStartIdx = url.indexOf("//") + 2;
    int domainEndIdx = url.indexOf('/', domainStartIdx);
    domainEndIdx = (domainEndIdx > domainStartIdx) ? domainEndIdx : url.length();
    domain = url.substring(domainStartIdx, domainEndIdx);
    subDomain = "";
    String[] parts = domain.split("\\.");
    /**
     * 例如  http://www.lmars.whu.edu.cn/index.jsp
     * 分割出 www.lmars.whu.edu.cn
     * 分割为 www lmars whu edu cn
     * 先拼接domain 为 edu.cn 在TLD列表中查找为一个合法网站后缀，
     * 则需要继续拼接钱一个 lmars
     * 所以domain为 lmars
     * 
     * 如果为 www.baidu.com
     * 直接分割出baidu.com 在Tld 中找不到，则域名就是baidu.com
     * 
     */
    if (parts.length > 2) {
      domain = parts[parts.length - 2] + "." + parts[parts.length - 1];
      int limit = 2;
      if (TLDList.getInstance().contains(domain)) {
        domain = parts[parts.length - 3] + "." + domain;
        limit = 3;
      }
      for (int i = 0; i < (parts.length - limit); i++) {
        if (!subDomain.isEmpty()) {
          subDomain += ".";
        }
        subDomain += parts[i];
      }
    }
    path = url.substring(domainEndIdx);
    int pathEndIdx = path.indexOf('?');
    if (pathEndIdx >= 0) {
      path = path.substring(0, pathEndIdx);
    }
  }

  /**
   * @return
   * 		父页面的文档ID
   * 		父网页为第一次发现此Url的网页
   */
  public int getParentDocid() {
    return parentDocid;
  }

  public void setParentDocid(int parentDocid) {
    this.parentDocid = parentDocid;
  }

  /**
   * @return
   * 		父网页的URL
   */
  public String getParentUrl() {
    return parentUrl;
  }

  public void setParentUrl(String parentUrl) {
    this.parentUrl = parentUrl;
  }

  /**
   * @return
   * 		此Url首次被发现时候的爬取深度。
   * 		种子Url深度为0.从种子Url中提取的Url深度为1。
   */
  public short getDepth() {
    return depth;
  }

  public void setDepth(short depth) {
    this.depth = depth;
  }

  /**
   * @return
   * 		此Url的域名
   */
  public String getDomain() {
    return domain;
  }

  public String getSubDomain() {
    return subDomain;
  }

  /**
   * @return
   * 		url的路径
   *       'http://www.lmars.whu.edu.cn/index.jsp' path：'index.jsp'
   */
  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  /**
   * @return
   *      锚文本. 
   *      例如：<a href="example.com">A sample anchor</a>
   *      锚文本为：'A sample anchor'
   */
  public String getAnchor() {
    return anchor;
  }

  public void setAnchor(String anchor) {
    this.anchor = anchor;
  }

  /**
   * @return 
   * 		此Url的爬取优先级。
   * 		数值越低，优先级越高！
   */
  public byte getPriority() {
    return priority;
  }

  public void setPriority(byte priority) {
    this.priority = priority;
  }

  /**
   * @return 
   * 		发现此Url的标签
   * */
  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }
  /**
   * 哈希值计算，以Url哈希值为准
   */
  @Override
  public int hashCode() {
    return url.hashCode();
  }
  /**
   * 重写equals方法，比较Url为准
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if ((o == null) || (getClass() != o.getClass())) {
      return false;
    }

    WebURL otherUrl = (WebURL) o;
    return (url != null) && url.equals(otherUrl.getURL());

  }

  @Override
  public String toString() {
    return url;
  }
}