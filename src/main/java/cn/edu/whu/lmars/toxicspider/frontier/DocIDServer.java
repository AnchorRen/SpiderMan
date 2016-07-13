package cn.edu.whu.lmars.toxicspider.frontier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;

import cn.edu.whu.lmars.toxicspider.crawler.Configurable;
import cn.edu.whu.lmars.toxicspider.crawler.CrawlConfig;
import cn.edu.whu.lmars.toxicspider.util.Util;

/**
 * Doc文档编号ID数据库
 * 
 * @author REN
 *
 */
public class DocIDServer extends Configurable {
	private static final Logger logger = LoggerFactory.getLogger(DocIDServer.class);

	// 定义数据库docIDs
	private final Database docIDsDB;
	private static final String DATABASE_NAME = "DocIDs";

	// 对象锁
	private final Object mutex = new Object();
	// 最后编号
	private int lastDocID;

	/**
	 * 初始化数据库设置
	 * 
	 * @param env
	 * @param config
	 */
	public DocIDServer(Environment env, CrawlConfig config) {
		super(config);
		// 数据库环境设置。
		DatabaseConfig dbConfig = new DatabaseConfig();
		// 当设置为true时，没有数据库的环境，也可以打开。否则就不能打开
		dbConfig.setAllowCreate(true);

		// 设置事务,如果设置为可以恢复，则设置事务，为后面恢复做准备。
		dbConfig.setTransactional(config.isResumableCrawling());
		dbConfig.setDeferredWrite(!config.isResumableCrawling());

		lastDocID = 0;
		// 初始化数据库
		docIDsDB = env.openDatabase(null, DATABASE_NAME, dbConfig);
		if (config.isResumableCrawling()) {
			int docCount = getDocCount(); // 获取数据库中数据量
			if (docCount > 0) {
				logger.info("Loaded {} URLs that had been detected in previous crawl.", docCount);
				lastDocID = docCount; // 因为是按照自然数依次排序，所以最后一个value值就是总的记录数
			}
		}
	}

	/**
	 * 返回一个数据库中已经存在的URL的 docID，不存在则返回-1
	 * 
	 * @param url
	 *            要返回docID的URL
	 * @return the docid of the url if it is seen before. Otherwise -1 is
	 *         returned.
	 */
	public int getDocId(String url) {

		// 同步锁,防止多线程操作的时候出现脏读
		synchronized (mutex) {
			// 数据库操作的状态
			OperationStatus result = null;
			// database key and data items as a byte array.
			DatabaseEntry value = new DatabaseEntry();
			try {
				DatabaseEntry key = new DatabaseEntry(url.getBytes());
				result = docIDsDB.get(null, key, value, null);

			} catch (Exception e) {
				logger.error("Exception thrown while getting DocID", e);
				return -1;
			}

			if ((result == OperationStatus.SUCCESS) && (value.getData().length > 0)) {
				// 如果数据库中存在，则返回此URL的docID
				return Util.byteArray2Int(value.getData());
			}

			return -1;
		}
	}

	/**
	 * 获取url 的 docID
	 * 如果DB中不存在这个URL，则把这个URL存储进DB中，并返回新创建的docID
	 * 
	 * @param url
	 * @return
	 */
	public int getNewDocID(String url) {

		synchronized (mutex) {
			try {
				// DB中有，则返回，没有则新创建
				int docID = getDocId(url);
				if (docID > 0) {
					return docID;
				}

				++lastDocID;
				docIDsDB.put(null, new DatabaseEntry(url.getBytes()), new DatabaseEntry(Util.int2ByteArray(lastDocID)));
				return lastDocID;
			} catch (Exception e) {
				logger.error("Exception thrown while getting new DocID", e);
				return -1;
			}
		}
	}

	/**
	 * 指定url和docID添加到数据库中
	 * @param url
	 * @param docId
	 * @throws Exception
	 */
	public void addUrlAndDocId(String url, int docId) throws Exception {
		synchronized (mutex) {
			if (docId <= lastDocID) {
				throw new Exception("Requested doc id: " + docId + " is not larger than: " + lastDocID);
			}

			// 确保之前没有为这个URL指定过 docID
			int prevDocid = getDocId(url);
			if (prevDocid > 0) {
				if (prevDocid == docId) {
					return;
				}
				throw new Exception("Doc id: " + prevDocid + " is already assigned to URL: " + url);
			}

			// 添加新URL到DB中，并指定其docID
			docIDsDB.put(null, new DatabaseEntry(url.getBytes()), new DatabaseEntry(Util.int2ByteArray(docId)));
			lastDocID = docId;
		}
	}

	/**
	 * 此url是否已经存在于数据库中
	 * 
	 * @param url
	 * @return 存在则返回true
	 */
	public boolean isSeenBefore(String url) {
		return getDocId(url) != -1;
	}

	/**
	 * 获取数据库中的记录数
	 * 
	 * @return
	 */
	public final int getDocCount() {
		try {
			return (int) docIDsDB.count();
		} catch (DatabaseException e) {
			logger.error("Exception thrown while getting DOC Count", e);
			return -1;
		}
	}

	/**
	 * 关闭数据库连接
	 */
	public void close() {
		try {
			docIDsDB.close();
		} catch (DatabaseException e) {
			logger.error("Exception thrown while closing DocIDServer", e);
		}
	}
}