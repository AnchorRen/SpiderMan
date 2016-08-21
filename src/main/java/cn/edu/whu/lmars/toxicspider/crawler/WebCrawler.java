package cn.edu.whu.lmars.toxicspider.crawler;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.http.HttpStatus;
import org.apache.http.impl.EnglishReasonPhraseCatalog;

import cn.edu.whu.lmars.toxicspider.crawler.exceptions.ContentFetchException;
import cn.edu.whu.lmars.toxicspider.crawler.exceptions.PageBiggerThanMaxSizeException;
import cn.edu.whu.lmars.toxicspider.crawler.exceptions.ParseException;
import cn.edu.whu.lmars.toxicspider.crawler.exceptions.RedirectException;
import cn.edu.whu.lmars.toxicspider.fetcher.PageFetchResult;
import cn.edu.whu.lmars.toxicspider.fetcher.PageFetcher;
import cn.edu.whu.lmars.toxicspider.frontier.DocIDServer;
import cn.edu.whu.lmars.toxicspider.frontier.Frontier;
import cn.edu.whu.lmars.toxicspider.parser.NotAllowedContentException;
import cn.edu.whu.lmars.toxicspider.parser.ParseData;
import cn.edu.whu.lmars.toxicspider.parser.Parser;
import cn.edu.whu.lmars.toxicspider.robotstxt.RobotstxtServer;
import cn.edu.whu.lmars.toxicspider.url.WebURL;
import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jext.Logger;
import uk.org.lidalia.slf4jext.LoggerFactory;

/**
 * 爬虫线程类
 * @author REN
 */
public class WebCrawler implements Runnable {

  protected static final Logger logger = LoggerFactory.getLogger(WebCrawler.class);

  /**
   * 这个id是和实例的爬虫线程联系的。
   */
  protected int myId;

  /**
   * 创建这个爬虫线程的控制器实例。
   * 可用于获取当前爬虫的配置信息和在运行时添加种子点。
   */
  protected CrawlController myController;

  /**
   * 当前运行的爬虫实例所在线程。
   */
  private Thread myThread;

  /**
   * 此解析器用于爬虫实例解析要抓取网页 内容。
   */
  private Parser parser;

  /**
   * 抓取器用于爬虫实例抓取网络中的内容。
   */
  private PageFetcher pageFetcher;

  /**
   * RobottxtServer用于判断当前网页是否允许我们的爬虫爬取此网页。
   */
  private RobotstxtServer robotstxtServer;

  /**
   * DocIDServer 用于映射 URL 和 docID.
   */
  private DocIDServer docIdServer;

  /**
   * 此实例管理着爬取队列。
   */
  private Frontier frontier;

  /**
   * 当前爬虫实例是否在等待着新加入的URL？
   * 控制器 会利用这个字段判断是否所有的爬虫实例都在等待新的URL，
   * 如果是，那么久没有需要处理的URL了，那么爬虫就可以停止了。
   */
  private boolean isWaitingForNewURLs;

  /**
   * 初始化当前爬虫实例。
   * 
   * @param id 
   * 			此爬虫实例的id

   * @param crawlController
   *            管理此次会话的控制器实例
   */
  public void init(int id, CrawlController crawlController) {
    this.myId = id;
    this.pageFetcher = crawlController.getPageFetcher();
    this.robotstxtServer = crawlController.getRobotstxtServer();
    this.docIdServer = crawlController.getDocIdServer();
    this.frontier = crawlController.getFrontier();
    this.parser = new Parser(crawlController.getConfig());
    this.myController = crawlController;
    this.isWaitingForNewURLs = false;
  }

  public int getMyId() {
    return myId;
  }

  public CrawlController getMyController() {
    return myController;
  }

  /**
   * 此函数会在爬虫实例初始化之前调用，可用于设置数据结构或者初始化参数。
   */
  public void onStart() {
	  
    // 默认不做处理，子类可通过继承此类添加自定义功能
  }

  /**
   * 这个函数会在终止此爬虫实例之前调用。
   * 可用于持久化内容中的数据，或者处理一些终止工作。
   */
  public void onBeforeExit() {
    // 默认不处理
  }

  /**
   * 此函数会在一个网页的 Headers 被抓取后调用。
   * 此函数可被子类覆盖，来处理网页不容响应码的一些逻辑关系。
   *	如 404 页面的处理...
   *
   * @param webUrl 网页实体
   * @param statusCode 状态码
   * @param statusDescription 网页状态码描述
   */
  protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
    // 默认不做任何处理
    // 子类可以通过继承覆盖此方法
  }

  /**
   * 此函数会在处理网页的URL之前调用。
   * 用于在处理前进一些处理。如 http://abc.com/def?a=123 - http://abc.com/def
   * 可被子类覆盖。
   *
   * @param curURL 处理前未调整的URL
   * @return 调整后的WebURL
   */
  protected WebURL handleUrlBeforeProcess(WebURL curURL) {
    return curURL;
  }

  /**
   * 此函数在 待处理网页内容大于最大允许值时调用。
   *
   * @param urlStr - 大于最大内容限制的网页
   */
  protected void onPageBiggerThanMaxSize(String urlStr, long pageSize) {
    logger.warn("Skipping a URL: {} which was bigger ( {} ) than max allowed size", urlStr, pageSize);
  }

  /**
   * 此函数在爬虫发生意外http状态码的时候调用。（处3XX之外的状态码）
   * 
   * @param urlStr 错误代码的URL
   * @param statusCode 状态码
   * @param contentType  contentType
   * @param description 错误描述信息
   */
  protected void onUnexpectedStatusCode(String urlStr, int statusCode, String contentType, String description) {
    logger.warn("Skipping URL: {}, StatusCode: {}, {}, {}", urlStr, statusCode, contentType, description);
    // 默认不做处理，可通做子类复写完成一些处理，比如存储进数据操作等。
  }

  /**
   * 当不能抓取到一个网页的内容的时候会调用此函数。
   * 
   * @param webUrl URL 
   */
  protected void onContentFetchError(WebURL webUrl) {
    logger.warn("Can't fetch content of: {}", webUrl.getURL());
  }
  
  /**
   * 当有未处理异常发生的时候，会调用此方法。
   *
   * @param webUrl 
   */
  protected void onUnhandledException(WebURL webUrl, Throwable e) {
    String urlStr = (webUrl == null ? "NULL" : webUrl.getURL());
    logger.warn("Unhandled exception while fetching {}: {}", urlStr, e.getMessage());
    logger.info("Stacktrace: ", e);
    // 默认不做任何处理
    // 子类可以通过继承覆盖此方法
  }
  
  /**
   * 当解析爬取到的内容发生错误的时候，会调用此函数。
   *
   * @param webUrl 
   */
  protected void onParseError(WebURL webUrl) {
    logger.warn("Parsing error of: {}", webUrl.getURL());
    // 默认不做任何处理
    // 子类可以通过继承覆盖此方法
  }

  /**
   * 创建此爬虫实例的CrawlerController实例在终止此爬虫线程之前会调用此函数。
   * 继承WebCrawler的类可复写此函数，来传递本地数据到其控制器。
   * 然后控制器会把这些数据放到一个List中，这样就可以处理爬虫的本地数据了（如果需要）。
   *
   * @return currently NULL
   */
  public Object getMyLocalData() {
    return null;
  }

  @Override
  public void run() {
    onStart();
    while (true) {
      List<WebURL> assignedURLs = new ArrayList<>(50);
      isWaitingForNewURLs = true;
      frontier.getNextURLs(50, assignedURLs); //同步方法，当获取不到数据的时候，就会处于waiting状态。
      isWaitingForNewURLs = false;
      if (assignedURLs.isEmpty()) {
        if (frontier.isFinished()) {
          return;
        }
        try {
          Thread.sleep(3000); //如果没有URL了，那就等待3秒钟。
        } catch (InterruptedException e) {
          logger.error("Error occurred", e);
        }
      } else {
        for (WebURL curURL : assignedURLs) {
          if (myController.isShuttingDown()) {
            logger.info("Exiting because of controller shutdown.");
            return;
          }
          if (curURL != null) {
            curURL = handleUrlBeforeProcess(curURL);
            processPage(curURL); //最重要的一个
            frontier.setProcessed(curURL);
          }
        }
      }
    }
  }

  /**
   * 继承WebCrawler的类应该覆盖该方法，来告诉爬虫给定的URL是否要爬取。默认的继承表示所有的URL都会进行爬取。
   * @param url
   *            是否需要爬取的网页
   *            
   * @param referringPage
   *           发现此url 的父网页
   *           
   * @return  True-爬取、False-不爬取
   */
  public boolean shouldVisit(Page referringPage, WebURL url) {

	  return true;
  }

  /**
   * 继承WebCrawler的类应该覆盖此方法。来处理抓取和解析到数据。
   * @param page
   *            抓取和解析到的数据
   */
  public void visit(Page page) {
	// 默认不做任何处理
	// 子类可以通过继承覆盖此方法
  }

  /**
   * 任务处理函数
   * 	每个爬虫实例都会执行这个函数处理URL
   * @param curURL
   */
  private void processPage(WebURL curURL) {
    PageFetchResult fetchResult = null;
    try {
      if (curURL == null) {
        throw new Exception("Failed processing a NULL url !?");
      }

      fetchResult = pageFetcher.fetchPage(curURL); //pageFetcher:网页内容抓取器，使用HttpClient抓取网页内容。
      int statusCode = fetchResult.getStatusCode();
      handlePageStatusCode(curURL, statusCode, EnglishReasonPhraseCatalog.INSTANCE
          .getReason(statusCode, Locale.ENGLISH)); // Finds the status reason for all known statuses

      Page page = new Page(curURL);
      page.setFetchResponseHeaders(fetchResult.getResponseHeaders());
      page.setStatusCode(statusCode);
      if (statusCode < 200 || statusCode > 299) { // Not 2XX: 2XX status codes indicate success
        if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode == HttpStatus.SC_MOVED_TEMPORARILY ||
            statusCode == HttpStatus.SC_MULTIPLE_CHOICES || statusCode == HttpStatus.SC_SEE_OTHER ||
            statusCode == HttpStatus.SC_TEMPORARY_REDIRECT ||
            statusCode == 308) { // is 3xx  todo follow https://issues.apache.org/jira/browse/HTTPCORE-389

          page.setRedirect(true);
          if (myController.getConfig().isFollowRedirects()) {
            String movedToUrl = fetchResult.getMovedToUrl();
            if (movedToUrl == null) {
              throw new RedirectException(Level.WARN, "Unexpected error, URL: " + curURL + " is redirected to NOTHING");
            }
            page.setRedirectedToUrl(movedToUrl);

            int newDocId = docIdServer.getDocId(movedToUrl);
            if (newDocId > 0) {
              throw new RedirectException(Level.DEBUG, "Redirect page: " + curURL + " is already seen");
            }

            WebURL webURL = new WebURL();
            webURL.setURL(movedToUrl);
            webURL.setParentDocid(curURL.getParentDocid());
            webURL.setParentUrl(curURL.getParentUrl());
            webURL.setDepth(curURL.getDepth());
            webURL.setDocid(-1);
            webURL.setAnchor(curURL.getAnchor());
            if (shouldVisit(page, webURL)) {
              if (robotstxtServer.allows(webURL)) {
                webURL.setDocid(docIdServer.getNewDocID(movedToUrl));
                frontier.schedule(webURL);
              } else {
                logger.debug("Not visiting: {} as per the server's \"robots.txt\" policy", webURL.getURL());
              }
            } else {
              logger.debug("Not visiting: {} as per your \"shouldVisit\" policy", webURL.getURL());
            }
          }
        } else { // All other http codes other than 3xx & 200
          String description = EnglishReasonPhraseCatalog.INSTANCE
              .getReason(fetchResult.getStatusCode(), Locale.ENGLISH); // Finds the status reason for all known statuses
          String contentType =
              fetchResult.getEntity() == null ? "" : fetchResult.getEntity().getContentType().getValue();
          onUnexpectedStatusCode(curURL.getURL(), fetchResult.getStatusCode(), contentType, description);
        }

      } else { // if status code is 200
        if (!curURL.getURL().equals(fetchResult.getFetchedUrl())) {
          if (docIdServer.isSeenBefore(fetchResult.getFetchedUrl())) {
            throw new RedirectException(Level.DEBUG, "Redirect page: " + curURL + " has already been seen");
          }
          curURL.setURL(fetchResult.getFetchedUrl());
          curURL.setDocid(docIdServer.getNewDocID(fetchResult.getFetchedUrl()));
        }

        if (!fetchResult.fetchContent(page)) {
          throw new ContentFetchException();
        }

        parser.parse(page, curURL.getURL()); //对抓取内容进行解析

        ParseData parseData = page.getParseData();
        List<WebURL> toSchedule = new ArrayList<>();
        int maxCrawlDepth = myController.getConfig().getMaxDepthOfCrawling();
        for (WebURL webURL : parseData.getOutgoingUrls()) {
          webURL.setParentDocid(curURL.getDocid());
          webURL.setParentUrl(curURL.getURL());
          int newdocid = docIdServer.getDocId(webURL.getURL());
          if (newdocid > 0) {
            // This is not the first time that this Url is visited. So, we set the depth to a negative number.
            webURL.setDepth((short) -1);
            webURL.setDocid(newdocid);
          } else {
            webURL.setDocid(-1);
            webURL.setDepth((short) (curURL.getDepth() + 1));
            if ((maxCrawlDepth == -1) || (curURL.getDepth() < maxCrawlDepth)) {
              if (shouldVisit(page, webURL)) {
                if (robotstxtServer.allows(webURL)) {
                  webURL.setDocid(docIdServer.getNewDocID(webURL.getURL()));
                  toSchedule.add(webURL);
                } else {
                  logger.debug("Not visiting: {} as per the server's \"robots.txt\" policy", webURL.getURL());
                }
              } else {
                logger.debug("Not visiting: {} as per your \"shouldVisit\" policy", webURL.getURL());
              }
            }
          }
        }
        frontier.scheduleAll(toSchedule);

        visit(page);
      }
    } catch (PageBiggerThanMaxSizeException e) {
      onPageBiggerThanMaxSize(curURL.getURL(), e.getPageSize());
    } catch (ParseException pe) {
      onParseError(curURL);
    } catch (ContentFetchException cfe) {
      onContentFetchError(curURL);
    } catch (RedirectException re) {
      logger.log(re.level, re.getMessage());
    } catch (NotAllowedContentException nace) {
      logger.debug("Skipping: {} as it contains binary content which you configured not to crawl", curURL.getURL());
    } catch (Exception e) {
      onUnhandledException(curURL, e);
    } finally {
      if (fetchResult != null) {
        fetchResult.discardContentIfNotConsumed();
      }
    }
  }

  public Thread getThread() {
    return myThread;
  }

  public void setThread(Thread myThread) {
    this.myThread = myThread;
  }

  public boolean isNotWaitingForNewURLs() {
    return !isWaitingForNewURLs;
  }
}
