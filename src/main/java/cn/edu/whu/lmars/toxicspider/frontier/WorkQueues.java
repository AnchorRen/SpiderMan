package cn.edu.whu.lmars.toxicspider.frontier;

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import cn.edu.whu.lmars.toxicspider.url.WebURL;
import cn.edu.whu.lmars.toxicspider.util.Util;

/**
 * 工作队列
 * @author REN
 *
 */
public class WorkQueues {
  private final Database urlsDB;
  private final Environment env;

  private final boolean resumable;

  private final WebURLTupleBinding webURLBinding;

  protected final Object mutex = new Object();

  /**
   * 构造函数，初始化数据库和对WebURL 和 元组Tuple进行绑定
   * @param env
   * @param dbName
   * @param resumable
   */
  public WorkQueues(Environment env, String dbName, boolean resumable) {
    this.env = env;
    this.resumable = resumable;
    DatabaseConfig dbConfig = new DatabaseConfig();
    dbConfig.setAllowCreate(true);
    dbConfig.setTransactional(resumable);
    dbConfig.setDeferredWrite(!resumable);
    urlsDB = env.openDatabase(null, dbName, dbConfig);
    webURLBinding = new WebURLTupleBinding();
  }

  /**
   * 根据resumable 的值确定是否开启事务，
   * resumable 为 true，则开启事务。
   * @return 
   */
  protected Transaction beginTransaction() {
    return resumable ? env.beginTransaction(null, null) : null;
  }

  /**
   * 事务提交
   * @param tnx
   */
  protected static void commit(Transaction tnx) {
    if (tnx != null) {
      tnx.commit();
    }
  }

  /**
   * 开启光标
   * @param txn
   * @return
   */
  protected Cursor openCursor(Transaction txn) {
    return urlsDB.openCursor(txn, null);
  }

  /**
   * 遍历数据库中数据，取出URL，转换为WebURL对象，存储进List集合，并返回
   * @param max 最大记录数
   * @return 小于等于最大记录数的WebURL对象集合
   */
  public List<WebURL> get(int max) {
   
	  synchronized (mutex) {
      List<WebURL> results = new ArrayList<>(max);
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      Transaction txn = beginTransaction();
      try (Cursor cursor = openCursor(txn)) {
        OperationStatus result = cursor.getFirst(key, value, null);
        int matches = 0;
        while ((matches < max) && (result == OperationStatus.SUCCESS)) {
          if (value.getData().length > 0) {
            results.add(webURLBinding.entryToObject(value));
            matches++;
          }
          result = cursor.getNext(key, value, null);
        }
      }
      commit(txn);
      return results;
    }
  }

  /**
   * 从数据库前面开始，删除指定数量的数据
   * @param count
   */
  public void delete(int count) {
    synchronized (mutex) {
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      Transaction txn = beginTransaction();
      try (Cursor cursor = openCursor(txn)) {
        OperationStatus result = cursor.getFirst(key, value, null);
        int matches = 0;
        while ((matches < count) && (result == OperationStatus.SUCCESS)) {
          cursor.delete();
          matches++;
          result = cursor.getNext(key, value, null);
        }
      }
      commit(txn);
    }
  }

  /**
   *	用于存储URLs的key决定了它们被爬取的顺序。低的key值将会被优先爬取。
   * 这里，我们的key设置为 6 byte。第一个字节为Url 的　Priority.第二个字节为
   * 发现这个Url的爬取深度。后面四个字节为URL的docId。
   * 结果是：
   * 		priority越小的会优先爬取，priority相同的话，那些在更小 depth的
   *  将会被优先爬取，如果depth也相同的话，那些越早发现的将会被优先爬取。
   */
  protected static DatabaseEntry getDatabaseEntryKey(WebURL url) {
    byte[] keyData = new byte[6];
    keyData[0] = url.getPriority();
    keyData[1] = ((url.getDepth() > Byte.MAX_VALUE) ? Byte.MAX_VALUE : (byte) url.getDepth());
    Util.putIntInByteArray(url.getDocid(), keyData, 2);
    return new DatabaseEntry(keyData);
  }

  /**
   * 把WebURL对象转化为 DatabaseEntry类型value值，并存储进数据库中
   * @param url
   */
  public void put(WebURL url) {
    DatabaseEntry value = new DatabaseEntry();
    webURLBinding.objectToEntry(url, value);//转WebURL为EntrySet
    Transaction txn = beginTransaction();
    urlsDB.put(txn, getDatabaseEntryKey(url), value);
    commit(txn); 
  }

  /**
   * 获取Berkeley 数据库中数据记录数量
   * @return
   */
  public long getLength() {
    return urlsDB.count();
  }

  /**
   * 关闭数据库连接
   */
  public void close() {
    urlsDB.close();
  }
}