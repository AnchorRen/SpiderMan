package cn.edu.whu.lmars.toxicspider.frontier;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import cn.edu.whu.lmars.toxicspider.url.WebURL;

/**
 * 这个类维护已经加入到爬虫队列中但是没有爬取的网页数据。
 * 用于恢复到原先的爬虫。
 * @author REN
 */
public class InProcessPagesDB extends WorkQueues {
  private static final Logger logger = LoggerFactory.getLogger(InProcessPagesDB.class);

  private static final String DATABASE_NAME = "InProcessPagesDB";

  public InProcessPagesDB(Environment env) {
    super(env, DATABASE_NAME, true); //调用父类构造方法进行初始化
    long docCount = getLength();
    if (docCount > 0) {
      logger.info("Loaded {} URLs that have been in process in the previous crawl.", docCount);
    }
  }

  /**
   * 删除指定webURL 对应的数据库中的值
   * @param webUrl
   * @return
   */
  public boolean removeURL(WebURL webUrl) {
    synchronized (mutex) {
      DatabaseEntry key = getDatabaseEntryKey(webUrl);
      DatabaseEntry value = new DatabaseEntry();
      Transaction txn = beginTransaction();
      try (Cursor cursor = openCursor(txn)) {
        OperationStatus result = cursor.getSearchKey(key, value, null);

        if (result == OperationStatus.SUCCESS) {
          result = cursor.delete();
          if (result == OperationStatus.SUCCESS) {
            return true;
          }
        }
      } finally {
        commit(txn);
      }
    }
    return false;
  }
}