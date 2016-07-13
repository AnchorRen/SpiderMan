package cn.edu.whu.lmars.toxicspider.fetcher;

import java.util.concurrent.TimeUnit;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
/**
 * 闲置链接监视线程，监视过期无效链接进行关闭。
 * 
 * @author REN
 *
 */
public class IdleConnectionMonitorThread extends Thread {

  private final PoolingHttpClientConnectionManager connMgr;
  
  private volatile boolean shutdown; //此变量所有线程都可见。

  public IdleConnectionMonitorThread(PoolingHttpClientConnectionManager connMgr) {
    super("Connection Manager");
    this.connMgr = connMgr;
  }

  @Override
  public void run() {
    try {
      while (!shutdown) {
        synchronized (this) {
          wait(5000);
          // 关闭过期无效链接
          connMgr.closeExpiredConnections();
          // 可选。关闭闲置30秒的连接。
          connMgr.closeIdleConnections(30, TimeUnit.SECONDS);
        }
      }
    } catch (InterruptedException ignored) {
      // terminate
    }
  }

  public void shutdown() {
    shutdown = true;
    synchronized (this) {
      notifyAll();
    }
  }
}