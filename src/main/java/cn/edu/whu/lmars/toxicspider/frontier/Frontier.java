package cn.edu.whu.lmars.toxicspider.frontier;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import cn.edu.whu.lmars.toxicspider.crawler.Configurable;
import cn.edu.whu.lmars.toxicspider.crawler.CrawlConfig;
import cn.edu.whu.lmars.toxicspider.url.WebURL;

public class Frontier extends Configurable {
  protected static final Logger logger = LoggerFactory.getLogger(Frontier.class);
  
  private static final String DATABASE_NAME = "PendingURLsDB";
  //批量处理大小
  private static final int IN_PROCESS_RESCHEDULE_BATCH_SIZE = 100;
  
  protected WorkQueues workQueues;

  protected InProcessPagesDB inProcessPages;

  //同步锁
  protected final Object mutex = new Object();
  protected final Object waitingList = new Object();

  protected boolean isFinished = false;

  //已经安排好未爬取的数量
  protected long scheduledPages;

  //计数器
  protected Counters counters;

  public Frontier(Environment env, CrawlConfig config) {
    super(config);
    this.counters = new Counters(env, config);
    try {
    	//创建工作队列数据库
      workQueues = new WorkQueues(env, DATABASE_NAME, config.isResumableCrawling());
      if (config.isResumableCrawling()) {
    	  //初始化未处理任务数量
        scheduledPages = counters.getValue(Counters.ReservedCounterNames.SCHEDULED_PAGES);
        //初始化上次运行时候数据库
        inProcessPages = new InProcessPagesDB(env);
        long numPreviouslyInProcessPages = inProcessPages.getLength();
        if (numPreviouslyInProcessPages > 0) {
          logger.info("Rescheduling {} URLs from previous crawl.", numPreviouslyInProcessPages);
          scheduledPages -= numPreviouslyInProcessPages;
          
          //把inProcessPages 数据库中数据全部加入到工作队列数据库中（PendingURLsDB)
          List<WebURL> urls = inProcessPages.get(IN_PROCESS_RESCHEDULE_BATCH_SIZE);
          while (!urls.isEmpty()) {
            scheduleAll(urls);
            inProcessPages.delete(urls.size());
            urls = inProcessPages.get(IN_PROCESS_RESCHEDULE_BATCH_SIZE);
          }
        }
      } else {
        inProcessPages = null;
        scheduledPages = 0;
      }
    } catch (DatabaseException e) {
      logger.error("Error while initializing the Frontier", e);
      workQueues = null;
    }
  }

  /**
   * 批量添加任务到工作队列数据库中
   * @param urls 要添加的WebURL 集合
   */
  public void scheduleAll(List<WebURL> urls) {
    int maxPagesToFetch = config.getMaxPagesToFetch();
    synchronized (mutex) {
    	//此变量标示已经新加入到数据库中的数据数量
      int newScheduledPage = 0;
      for (WebURL url : urls) {
    	  //如果需要调度的数量大于最大限制数，则结束
        if ((maxPagesToFetch > 0) && ((scheduledPages + newScheduledPage) >= maxPagesToFetch)) {
          break;
        }

        try {
        	//添加到数据库中
          workQueues.put(url);
          newScheduledPage++;
        } catch (DatabaseException e) {
          logger.error("Error while putting the url in the work queue", e);
        }
      }
      if (newScheduledPage > 0) {
        scheduledPages += newScheduledPage; //更新已经调度的链接数量
        counters.increment(Counters.ReservedCounterNames.SCHEDULED_PAGES, newScheduledPage);
      }
      synchronized (waitingList) {
        waitingList.notifyAll();
      }
    }
  }

  /**
   * 单个任务添加
   * @param url
   */
  public void schedule(WebURL url) {
    int maxPagesToFetch = config.getMaxPagesToFetch();
    synchronized (mutex) {
      try {
        if (maxPagesToFetch < 0 || scheduledPages < maxPagesToFetch) {
          workQueues.put(url);
          scheduledPages++;
          counters.increment(Counters.ReservedCounterNames.SCHEDULED_PAGES);
        }
      } catch (DatabaseException e) {
        logger.error("Error while putting the url in the work queue", e);
      }
    }
  }

  /**
   * 批量获取任务，并添加到InProcessPages中
   * @param max 批量获取的任务数
   * @param result 获取到的任务集合
   */
  public void getNextURLs(int max, List<WebURL> result) {
    while (true) {
      synchronized (mutex) {
        if (isFinished) {
          return;
        }
        try {
          List<WebURL> curResults = workQueues.get(max); //批量获取任务
          workQueues.delete(curResults.size()); //删除原任务队列中刚获取的任务
          if (inProcessPages != null) {
            for (WebURL curPage : curResults) {
              inProcessPages.put(curPage); //添加批量获取的任务到正在执行的数据库中。
            }
          }
          result.addAll(curResults);
        } catch (DatabaseException e) {
          logger.error("Error while getting next urls", e);
        }

        if (result.size() > 0) {
          return; //获取到任务，则返回
        }
      }

      try {
        synchronized (waitingList) { //未获取到任务，则等待其他线程添加任务
          waitingList.wait();
        }
      } catch (InterruptedException ignored) {
        // Do nothing
      }
      if (isFinished) { //如果所有任务已经结束，则退出。
        return;
      }
    }
  }

  /**
   * 设置一个WebURL为已经处理完成
   * @param webURL
   */
  public void setProcessed(WebURL webURL) {
	  //已处理任务计数器加1
    counters.increment(Counters.ReservedCounterNames.PROCESSED_PAGES);
    if (inProcessPages != null) {
      if (!inProcessPages.removeURL(webURL)) { //从数据库中移除这个已经处理过的任务
        logger.warn("Could not remove: {} from list of processed pages.", webURL.getURL());
      }
    }
  }

  /**
   * 获取工作队列中任务数量
   * @return
   */
  public long getQueueLength() {
    return workQueues.getLength();
  }

  /**
   * 获取任务列表中任务数量
   * @return
   */
  public long getNumberOfAssignedPages() {
    return inProcessPages.getLength();
  }

  /**
   * 获取已经处理过的任务数量
   * @return
   */
  public long getNumberOfProcessedPages() {
    return counters.getValue(Counters.ReservedCounterNames.PROCESSED_PAGES);
  }

  public boolean isFinished() {
    return isFinished;
  }

  /**
   * 关闭数据库连接
   */
  public void close() {
    workQueues.close();
    counters.close();
    if (inProcessPages != null) {
      inProcessPages.close();
    }
  }

  /**
   * 如果任务已经结束，则唤醒所有等待线程。
   */
  public void finish() {
    isFinished = true;
    synchronized (waitingList) {
      waitingList.notifyAll();
    }
  }
}