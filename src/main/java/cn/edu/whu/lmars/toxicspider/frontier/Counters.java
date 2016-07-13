package cn.edu.whu.lmars.toxicspider.frontier;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import cn.edu.whu.lmars.toxicspider.crawler.Configurable;
import cn.edu.whu.lmars.toxicspider.crawler.CrawlConfig;
import cn.edu.whu.lmars.toxicspider.util.Util;

/**
 * 计数器类，用于记录已经处理的URL的数量（PROCESSED_PAGES），
 * 和尚未处理的URL的数量（SCHEDULED_PAGES）。
 * @author REN
 *
 */
public class Counters extends Configurable {
  private static final Logger logger = LoggerFactory.getLogger(Counters.class);

  /**
   * 静态内部类，维护两个常量，用于标示已经处理的任务数量，和尚未处理的任务量。
   * @author REN
   *
   */
  public static class ReservedCounterNames {
    public static final String SCHEDULED_PAGES = "Scheduled-Pages"; //未处理的任务
    public static final String PROCESSED_PAGES = "Processed-Pages"; //已经处理的任务
  }


  private static final String DATABASE_NAME = "Statistics";
  
  //此数据库用于储存两个计数器的数量
  protected Database statisticsDB = null;
  protected Environment env;

  protected final Object mutex = new Object();
  
  //存储两个计数器的数量
  protected Map<String, Long> counterValues;

  /**
   * 此构造函数对计数器数据库，计数器Map等进行初始化
   * @param env
   * @param config
   */
  public Counters(Environment env, CrawlConfig config) {
    super(config);

    this.env = env;
    this.counterValues = new HashMap<>();

    /*
     * 如果爬虫设置为可恢复的，我们需要保存数据到一个事务数据库中，
     * 来确保爬虫突然崩溃或者意外停止时候数据不会丢失。
     */
    if (config.isResumableCrawling()) {
      DatabaseConfig dbConfig = new DatabaseConfig();
      dbConfig.setAllowCreate(true);
      dbConfig.setTransactional(true);
      dbConfig.setDeferredWrite(false); // //true为进行缓冲写库,false则不进行缓冲写库
      //创建数据备份数据库
      statisticsDB = env.openDatabase(null, DATABASE_NAME, dbConfig);

      OperationStatus result;
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      Transaction tnx = env.beginTransaction(null, null);
      /**
       * A database cursor. Cursors are used for operating on collections of records, 
       * for iterating over a database, and for saving handles to individual records, 
       * so that they can be modified after they have been read. 
       */
      Cursor cursor = statisticsDB.openCursor(tnx, null);
      result = cursor.getFirst(key, value, null);

      //通过数据库光标cursor，遍历数据库
      while (result == OperationStatus.SUCCESS) {
        if (value.getData().length > 0) {
          String name = new String(key.getData());
          long counterValue = Util.byteArray2Long(value.getData());
          counterValues.put(name, counterValue);
        }
        result = cursor.getNext(key, value, null);
      }
      cursor.close();
      tnx.commit();
    }
  }

  /**
   * 取计数器的值
   * @param name
   * @return
   */
  public long getValue(String name) {
    synchronized (mutex) {
      Long value = counterValues.get(name);
      if (value == null) {
        return 0;
      }
      return value;
    }
  }

  /**
   * 设置计数器的值
   * @param name
   * @param value
   */
  public void setValue(String name, long value) {
    synchronized (mutex) {
      try {
        counterValues.put(name, value);
        if (statisticsDB != null) {
          Transaction txn = env.beginTransaction(null, null);
          statisticsDB.put(txn, new DatabaseEntry(name.getBytes()), new DatabaseEntry(Util.long2ByteArray(value)));
          txn.commit();
        }
      } catch (Exception e) {
        logger.error("Exception setting value", e);
      }
    }
  }

  /**
   * 计数器数量+1
   * @param name
   */
  public void increment(String name) {
    increment(name, 1);
  }

  /**
   * 计数器数量+addition
   * @param name
   * @param addition
   */
  public void increment(String name, long addition) {
    synchronized (mutex) {
      long prevValue = getValue(name);
      setValue(name, prevValue + addition);
    }
  }

  /**
   * 关闭数据库连接
   */
  public void close() {
    try {
      if (statisticsDB != null) {
        statisticsDB.close();
      }
    } catch (DatabaseException e) {
      logger.error("Exception thrown while trying to close statisticsDB", e);
    }
  }
}