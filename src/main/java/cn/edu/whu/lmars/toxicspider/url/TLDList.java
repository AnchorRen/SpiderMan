package cn.edu.whu.lmars.toxicspider.url;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 此类是一个单例模式类，包含了一个TLDs列表。
 * 这个列表可以从网上或者本地读取。
 * 
 * @author REN
 */
public class TLDList {

	//网上TLD列表
  private static final String TLD_NAMES_ONLINE_URL = "https://publicsuffix.org/list/effective_tld_names.dat";
  //本地TLD文件
  private static final String TLD_NAMES_TXT_FILENAME = "tld-names.txt";
  private static final Logger logger = LoggerFactory.getLogger(TLDList.class);

  private static boolean onlineUpdate = false;//为false则读取本地文件中数据
  private final Set<String> tldSet = new HashSet<>(10000); //定义用于存储tld的set集合

  private static final TLDList instance = new TLDList(); // Singleton 单例存在，只能在类内部创建类的实例

  private TLDList() { //私有的构造方法，保证不能在外部通过构造器创建实例
    if (onlineUpdate) { //读取在线tld列表，并更新本地文件
      URL url;
      try {
        url = new URL(TLD_NAMES_ONLINE_URL);
      } catch (MalformedURLException e) {
        // This cannot happen... No need to treat it
        logger.error("Invalid URL: {}", TLD_NAMES_ONLINE_URL);
        throw new RuntimeException(e);
      }

      try (InputStream stream = url.openStream()) {
        logger.debug("Fetching the most updated TLD list online");
        int n = readStream(stream); //读取到tld 集合中。
        logger.info("Obtained {} TLD from URL {}", n, TLD_NAMES_ONLINE_URL);
        return;
      } catch (Exception e) {
        logger.error("Couldn't fetch the online list of TLDs from: {}", TLD_NAMES_ONLINE_URL, e);
      }
    }
    //创建本地tld文件
    File f = new File(TLD_NAMES_TXT_FILENAME);
    //如果已经存在，则直接操作
    if (f.exists()) {
      logger.debug("Fetching the list from a local file {}", TLD_NAMES_TXT_FILENAME);
      try (InputStream tldFile = new FileInputStream(f)) {
        int n = readStream(tldFile);
        logger.info("Obtained {} TLD from local file {}", n, TLD_NAMES_TXT_FILENAME);
        return;
      } catch (IOException e) {
        logger.error("Couldn't read the TLD list from local file", e);
      }
    }
    //通过getResourceAsStream方法读取
    try (InputStream tldFile = getClass().getClassLoader().getResourceAsStream(TLD_NAMES_TXT_FILENAME)) {
      int n = readStream(tldFile);
      logger.info("Obtained {} TLD from packaged file {}", n, TLD_NAMES_TXT_FILENAME);
    } catch (IOException e) {
      logger.error("Couldn't read the TLD list from file");
      throw new RuntimeException(e);
    }
  }

  /**
   * 读取输入流中记录到tld Set中，并返回集合中的记录数。
   * @param stream
   * @return
   */
  private int readStream(InputStream stream) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("//")) {
          continue;
        }
        tldSet.add(line);
      }
    } catch (IOException e) {
      logger.warn("Error while reading TLD-list: {}", e.getMessage());
    }
    return tldSet.size();
  }

  /**
   * 创建TLDList实例
   * @return TLDList实例对象
   */
  public static TLDList getInstance() {
    return instance;
  }

 
  /**
   * 设置此值为true，则从网上下载Tld列表
   * @param online
   */
  public static void setUseOnline(boolean online) {
    onlineUpdate = online;
  }

  /**
   * tld集合中是否包含某一条记录
   * @param str
   * @return
   */
  public boolean contains(String str) {
    return tldSet.contains(str);
  }
}
