package cn.edu.whu.lmars.toxicspider.crawler;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import cn.edu.whu.lmars.toxicspider.fetcher.PageFetcher;
import cn.edu.whu.lmars.toxicspider.frontier.DocIDServer;
import cn.edu.whu.lmars.toxicspider.frontier.Frontier;
import cn.edu.whu.lmars.toxicspider.robotstxt.RobotstxtServer;
import cn.edu.whu.lmars.toxicspider.url.TLDList;
import cn.edu.whu.lmars.toxicspider.url.URLCanonicalizer;
import cn.edu.whu.lmars.toxicspider.url.WebURL;
import cn.edu.whu.lmars.toxicspider.util.IO;

/**
 * CrawlerController管理着一次爬行的会话。
 * 通过这个类创建爬虫线程 和 监视爬取的过程
 *
 * @author REN
 */
public class CrawlController extends Configurable {

  static final Logger logger = LoggerFactory.getLogger(CrawlController.class);

  /**
   * customData 用于自定义配置，可以传递到其他爬取相关的组件。
   */
  protected Object customData;

  /**
   * 一旦爬行会话创建完成，控制器会收集爬虫线程的本地数据，并存储到这个List中。
   */
  protected List<Object> crawlersLocalData = new ArrayList<>();

  /**
   * 这次爬行会话是否已经结束？
   */
  protected boolean finished;

  /**
   * 这次爬行会话是否设置为 'shutdown'. 
   * 爬虫线程会监视这个标志，如果设置为shutdown，则爬虫不会在处理新的页面。
   */
  protected boolean shuttingDown;

  protected PageFetcher pageFetcher; //网页抓取器
  protected RobotstxtServer robotstxtServer; //robotstext探测器
  protected Frontier frontier;  //URL队列管理器，管理Berkeley DB中的URL
  protected DocIDServer docIdServer; //文档ID管理器，管理URL 的ID编号

  protected final Object waitingLock = new Object();
  protected final Environment env;

  /**
   * 构造函数初始化爬虫控制器
   * @param config
   * @param pageFetcher
   * @param robotstxtServer
   * @throws Exception
   */
  public CrawlController(CrawlConfig config, PageFetcher pageFetcher, RobotstxtServer robotstxtServer)
      throws Exception {
    super(config);

    config.validate(); //对设置的爬取深度，最大爬取连接数，存储目录是否设置进行验证
    File folder = new File(config.getCrawlStorageFolder());
    if (!folder.exists()) {
      if (folder.mkdirs()) {
        logger.debug("Created folder: " + folder.getAbsolutePath());
      } else {
        throw new Exception(
            "couldn't create the storage folder: " + folder.getAbsolutePath() + " does it already exist ?");
      }
    }

    TLDList.setUseOnline(config.isOnlineTldListUpdate());

    boolean resumable = config.isResumableCrawling(); //是否开启故障恢复功能

    EnvironmentConfig envConfig = new EnvironmentConfig();
    envConfig.setAllowCreate(true);
    envConfig.setTransactional(resumable);
    envConfig.setLocking(resumable);

    File envHome = new File(config.getCrawlStorageFolder() + "/frontier"); //数据存储路径
    if (!envHome.exists()) {
      if (envHome.mkdir()) {
        logger.debug("Created folder: " + envHome.getAbsolutePath());
      } else {
        throw new Exception("Failed creating the frontier folder: " + envHome.getAbsolutePath());
      }
    }

    /**
     * 不开启故障恢复，则删除上一次程序运行未执行的任务
     */
    if (!resumable) {
      IO.deleteFolderContents(envHome);
      logger.info("Deleted contents of: " + envHome + " ( as you have configured resumable crawling to false )");
    }

    env = new Environment(envHome, envConfig);
    docIdServer = new DocIDServer(env, config); //实例化DocIDServer
    frontier = new Frontier(env, config); //实例化URL管理对象

    this.pageFetcher = pageFetcher;
    this.robotstxtServer = robotstxtServer;

    finished = false;
    shuttingDown = false;
  }

  /**
   * 内部爬虫工厂类接口
   * @author REN
   * @param <T>
   */
  public interface WebCrawlerFactory<T extends WebCrawler> {
    T newInstance() throws Exception;
  }

  /**
   * 内部默认爬虫工厂类，继承爬虫工厂类接口
   * @author REN
   * @param <T> 要生产的爬虫类（继承自WebCrawler）
   */
  private static class DefaultWebCrawlerFactory<T extends WebCrawler> implements WebCrawlerFactory<T> {
    final Class<T> _c;

    DefaultWebCrawlerFactory(Class<T> _c) {
      this._c = _c;
    }

    @Override
    public T newInstance() throws Exception {
      try {
        return _c.newInstance();
      } catch (ReflectiveOperationException e) {
        throw e;
      }
    }
  }

  /**
   * 开启爬虫会话，等待创建爬虫实例完成。
   * 这个方法通过java反射，使用默认的爬虫工厂类创建爬虫实例
   *
   * @param _c
   *           爬虫类
   * @param numberOfCrawlers
   * 			此次爬取会话中并发的爬虫线程数
   * @param <T> 
				继承WebCrawler的类
   */
  public <T extends WebCrawler> void start(final Class<T> _c, final int numberOfCrawlers) {
    this.start(new DefaultWebCrawlerFactory<>(_c), numberOfCrawlers, true);
  }

  /**
   * 开启爬虫会话，等待创建爬虫实例完成。
   *
   * @param crawlerFactory
   *            创建爬虫实例的工厂类
   * @param numberOfCrawlers
   *           此次爬取会话中并发的爬虫线程数
   * @param <T> 
   * 			继承WebCrawler的类
   */
  public <T extends WebCrawler> void start(final WebCrawlerFactory<T> crawlerFactory, final int numberOfCrawlers) {
    this.start(crawlerFactory, numberOfCrawlers, true);
  }

  /**
   * 开启爬虫会话，并立即返回。
   *
   * @param crawlerFactory
   *            创建爬虫实例的工厂类
   * @param numberOfCrawlers
   *           此次爬取会话中并发的爬虫线程数
   * @param <T> 
   * 			继承WebCrawler的类
   */
  public <T extends WebCrawler> void startNonBlocking(WebCrawlerFactory<T> crawlerFactory, final int numberOfCrawlers) {
    this.start(crawlerFactory, numberOfCrawlers, false);
  }

  /**
   * 开启爬虫会话，并立即返回。
   * 这个方法通过java反射，使用默认的爬虫工厂类创建爬虫实例
   *
   * @param _c
   *            创建爬虫实例的工厂类
   * @param numberOfCrawlers
   *            此次爬取会话中并发的爬虫线程数
   * @param <T> 继承WebCrawler的类
   */
  public <T extends WebCrawler> void startNonBlocking(final Class<T> _c, final int numberOfCrawlers) {
    this.start(new DefaultWebCrawlerFactory<>(_c), numberOfCrawlers, false);
  }

  /**
   * 启动爬虫
   * @param crawlerFactory 爬虫工厂类
   * @param numberOfCrawlers 爬虫线程数
   * @param isBlocking 创建过程是否阻塞
   */
  protected <T extends WebCrawler> void start(final WebCrawlerFactory<T> crawlerFactory, final int numberOfCrawlers, boolean isBlocking) {
    try {
      finished = false;
      crawlersLocalData.clear();
      final List<Thread> threads = new ArrayList<>(); //爬虫线程集合
      final List<T> crawlers = new ArrayList<>(); //爬虫集合

      for (int i = 1; i <= numberOfCrawlers; i++) {
        T crawler = crawlerFactory.newInstance();
        Thread thread = new Thread(crawler, "Crawler " + i); //每个爬虫创建一个线程
        crawler.setThread(thread);
        crawler.init(i, this);
        thread.start();
        crawlers.add(crawler);
        threads.add(thread);
        logger.info("Crawler {} started", i);
      }

      final CrawlController controller = this;
      /**
       * 监视线程，
       * 		监视爬取过程中的线程，如果有线程意外停止，则重新创建此线程。
       */
      Thread monitorThread = new Thread(new Runnable() {

        @Override
        public void run() {
          try {
            synchronized (waitingLock) {

              while (true) {
                sleep(10); //每隔10秒检查一次
                boolean someoneIsWorking = false; //是否有线程在运行
                for (int i = 0; i < threads.size(); i++) {
                  Thread thread = threads.get(i);
                  if (!thread.isAlive()) {
                    if (!shuttingDown) {
                      logger.info("Thread {} was dead, I'll recreate it", i);
                      T crawler = crawlerFactory.newInstance();
                      thread = new Thread(crawler, "Crawler " + (i + 1));
                      threads.remove(i);
                      threads.add(i, thread);
                      crawler.setThread(thread);
                      crawler.init(i + 1, controller);
                      thread.start();
                      crawlers.remove(i);
                      crawlers.add(i, crawler);
                    }
                  } else if (crawlers.get(i).isNotWaitingForNewURLs()) {
                    someoneIsWorking = true;
                  }
                }
                boolean shut_on_empty = config.isShutdownOnEmptyQueue(); //workQueue为空的时候是否停止爬虫
                if (!someoneIsWorking && shut_on_empty) { //如果没有正在运行的爬虫，且workQueue为空的时候停止
                  /*
                   * 确保没有正常工作的线程了。
                   * 	等待十秒，如果这个过程有其他未完成的线程又爬取到了新的URL并且添加到了任务中，这个时候就可以正确工作了。
                   */
                  logger.info("It looks like no thread is working, waiting for 10 seconds to make sure...");
                  sleep(10); 

                  someoneIsWorking = false;
                  /**
                   * 再次扫描所有线程，确保没有正常工作的线程了。
                   */
                  for (int i = 0; i < threads.size(); i++) {
                    Thread thread = threads.get(i);
                    if (thread.isAlive() && crawlers.get(i).isNotWaitingForNewURLs()) {
                      someoneIsWorking = true;
                    }
                  }
                  if (!someoneIsWorking) {
                    if (!shuttingDown) {
                      long queueLength = frontier.getQueueLength(); //检查是否有新的URL添加到Queue中
                      if (queueLength > 0) {
                        continue;
                      }
                      logger.info(
                          "No thread is working and no more URLs are in queue waiting for another 10 seconds to make " +
                          "sure...");
                      sleep(10); //再等待十秒钟确认
                      queueLength = frontier.getQueueLength();
                      if (queueLength > 0) {
                        continue;
                      }
                    }

                    logger.info("All of the crawlers are stopped. Finishing the process...");
                    // 告知那些等待添加新的URL的线程，没有URL了，可以停止了。
                    frontier.finish();
                    for (T crawler : crawlers) {
                      crawler.onBeforeExit();
                      crawlersLocalData.add(crawler.getMyLocalData()); //线程爬取过程数据存储
                    }

                    logger.info("Waiting for 10 seconds before final clean up...");
                    sleep(10);

                    frontier.close();
                    docIdServer.close();
                    pageFetcher.shutDown();

                    finished = true;
                    waitingLock.notifyAll(); //叫醒主线程可以结束了
                    env.close();

                    return;
                  }
                }
              }
            }
          } catch (Exception e) {
            logger.error("Unexpected Error", e);
          }
        }
      });

      monitorThread.start();
      //主线程是否睡觉
      if (isBlocking) {
        waitUntilFinish();
      }

    } catch (Exception e) {
      logger.error("Error happened", e);
    }
  }

  /**
   * 主线程休眠了
   */
  public void waitUntilFinish() {
    while (!finished) {
      synchronized (waitingLock) {
        if (finished) {
          return;
        }
        try {
          waitingLock.wait();
        } catch (InterruptedException e) {
          logger.error("Error occurred", e);
        }
      }
    }
  }

  public List<Object> getCrawlersLocalData() {
    return crawlersLocalData;
  }

  protected static void sleep(int seconds) {
    try {
      Thread.sleep(seconds * 1000);
    } catch (InterruptedException ignored) {
      // Do nothing
    }
  }

  /**
   * 添加一个种子点。
   *
   * @param pageUrl
   *           种子点URL
   */
  public void addSeed(String pageUrl) {
    addSeed(pageUrl, -1);
  }

  /**
   * 添加一个新的种子点。你可以指定一个特定的文档编号分配给这个种子点。这个文档编号必须是
   * 唯一的。而且，如果你添加了三个种子点，文档编号分别为1,2,7 那么，下一个爬取过程中发现的URL的编号
   * 将会是8.你需要保证种子点的文档编号是递增的顺序。
   *
   * 如果在之前有一个爬取任务并且存储了结果，想要再开启新的爬虫任务并添加之前爬虫用过的文档编号的的种子点。
   * 这时候指定URL的文档编号是很有用处的。
   *
   * @param pageUrl
   *            种子点的URL地址
   * @param docId
   *            想要为此种子点指定的文档编号。
   *
   */
  public void addSeed(String pageUrl, int docId) {
    String canonicalUrl = URLCanonicalizer.getCanonicalURL(pageUrl);
    if (canonicalUrl == null) {
      logger.error("Invalid seed URL: {}", pageUrl);
    } else {
      if (docId < 0) {
    	  /**
    	   * 如何检验爬取到的新URL是否存在？
    	   * 	DocIDServer把整个爬取过程中找到的URL都存储在了DocIDServer中，
    	   * 	通过 docIdServer.getDocId(canonicalUrl) 如果获取到key，说明这个URL已经
    	   * 	添加过了，直接返回。
    	   */
        docId = docIdServer.getDocId(canonicalUrl);
        if (docId > 0) {
          logger.trace("This URL is already seen.");
          return; 
        }
        docId = docIdServer.getNewDocID(canonicalUrl);
      } else {
        try {
          docIdServer.addUrlAndDocId(canonicalUrl, docId);
        } catch (Exception e) {
          logger.error("Could not add seed: {}", e.getMessage());
        }
      }

      WebURL webUrl = new WebURL();
      webUrl.setURL(canonicalUrl);
      webUrl.setDocid(docId);
      webUrl.setDepth((short) 0); //第0层，种子点的层数
      if (robotstxtServer.allows(webUrl)) {
        frontier.schedule(webUrl);
      } else {
        logger.warn("Robots.txt does not allow this seed: {}",
                    pageUrl); 
      }
    }
  }

  /**
   * This function can called to assign a specific document id to a url. This
   * feature is useful when you have had a previous crawl and have stored the
   * Urls and their associated document ids and want to have a new crawl which
   * is aware of the previously seen Urls and won't re-crawl them.
   *
   * Note that if you add three seen Urls with document ids 1,2, and 7. Then
   * the next URL that is found during the crawl will get a doc id of 8. Also
   * you need to ensure to add seen Urls in increasing order of document ids.
   *
   * @param url
   *            the URL of the page
   * @param docId
   *            the document id that you want to be assigned to this URL.
   *
   */
  public void addSeenUrl(String url, int docId) {
    String canonicalUrl = URLCanonicalizer.getCanonicalURL(url);
    if (canonicalUrl == null) {
      logger.error("Invalid Url: {} (can't cannonicalize it!)", url);
    } else {
      try {
        docIdServer.addUrlAndDocId(canonicalUrl, docId);
      } catch (Exception e) {
        logger.error("Could not add seen url: {}", e.getMessage());
      }
    }
  }

  public PageFetcher getPageFetcher() {
    return pageFetcher;
  }

  public void setPageFetcher(PageFetcher pageFetcher) {
    this.pageFetcher = pageFetcher;
  }

  public RobotstxtServer getRobotstxtServer() {
    return robotstxtServer;
  }

  public void setRobotstxtServer(RobotstxtServer robotstxtServer) {
    this.robotstxtServer = robotstxtServer;
  }

  public Frontier getFrontier() {
    return frontier;
  }

  public void setFrontier(Frontier frontier) {
    this.frontier = frontier;
  }

  public DocIDServer getDocIdServer() {
    return docIdServer;
  }

  public void setDocIdServer(DocIDServer docIdServer) {
    this.docIdServer = docIdServer;
  }

  public Object getCustomData() {
    return customData;
  }

  public void setCustomData(Object customData) {
    this.customData = customData;
  }

  public boolean isFinished() {
    return this.finished;
  }

  public boolean isShuttingDown() {
    return shuttingDown;
  }

  /**
   * 设置当前的爬取会话为shutdown。爬虫线程探测到这个标志为true后，就不会再爬取新的网页了。
   */
  public void shutdown() {
    logger.info("Shutting down...");
    this.shuttingDown = true;
    pageFetcher.shutDown();
    frontier.finish();
  }
}
