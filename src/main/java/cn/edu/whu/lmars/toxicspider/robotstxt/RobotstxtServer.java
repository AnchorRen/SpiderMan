package cn.edu.whu.lmars.toxicspider.robotstxt;

import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.whu.lmars.toxicspider.crawler.Page;
import cn.edu.whu.lmars.toxicspider.crawler.exceptions.PageBiggerThanMaxSizeException;
import cn.edu.whu.lmars.toxicspider.fetcher.PageFetchResult;
import cn.edu.whu.lmars.toxicspider.fetcher.PageFetcher;
import cn.edu.whu.lmars.toxicspider.url.WebURL;
import cn.edu.whu.lmars.toxicspider.util.Util;

public class RobotstxtServer {

	private static final Logger logger = LoggerFactory.getLogger(RobotstxtServer.class);

	protected RobotstxtConfig config;
	//缓存Host robots信息到一个map中。
	protected final Map<String, HostDirectives> host2directivesCache = new HashMap<>();

	protected PageFetcher pageFetcher;
	//构造函数
	public RobotstxtServer(RobotstxtConfig config, PageFetcher pageFetcher) {
		this.config = config;
		this.pageFetcher = pageFetcher;
	}

	private static String getHost(URL url) {
		return url.getHost().toLowerCase();
	}

	/**
	 * 查看当前Url的路径是否允许被抓取
	 * 
	 * @param webURL
	 * @return
	 */
	public boolean allows(WebURL webURL) {
		if (config.isEnabled()) {
			try {
				URL url = new URL(webURL.getURL());
				String host = getHost(url);
				String path = url.getPath();

				HostDirectives directives = host2directivesCache.get(host);

				if ((directives != null) && directives.needsRefetch()) {
					synchronized (host2directivesCache) {
						host2directivesCache.remove(host);
						directives = null;
					}
				}

				if (directives == null) {
					directives = fetchDirectives(url);
				}

				return directives.allows(path); // 当前路径是否允许抓取
			} catch (MalformedURLException e) {
				logger.error("Bad URL in Robots.txt: " + webURL.getURL(), e);
			}
		}

		return true;
	}

	private HostDirectives fetchDirectives(URL url) {
		WebURL robotsTxtUrl = new WebURL();
		String host = getHost(url);
		String port = ((url.getPort() == url.getDefaultPort()) || (url.getPort() == -1)) ? "" : (":" + url.getPort());
		// robots.txt 文件的Url路径
		robotsTxtUrl.setURL("http://" + host + port + "/robots.txt");
		HostDirectives directives = null;
		PageFetchResult fetchResult = null;
		try {
			fetchResult = pageFetcher.fetchPage(robotsTxtUrl);
			if (fetchResult.getStatusCode() == HttpStatus.SC_OK) {
				Page page = new Page(robotsTxtUrl);
				fetchResult.fetchContent(page);
				if (Util.hasPlainTextContent(page.getContentType())) {
					String content;
					if (page.getContentCharset() == null) {
						content = new String(page.getContentData());
					} else {
						content = new String(page.getContentData(), page.getContentCharset());
					}
					directives = RobotstxtParser.parse(content, config.getUserAgentName());
				} else if (page.getContentType().contains("html")) { 
					String content = new String(page.getContentData());
					directives = RobotstxtParser.parse(content, config.getUserAgentName());
				} else {
					logger.warn("Can't read this robots.txt: {}  as it is not written in plain text, contentType: {}",
							robotsTxtUrl.getURL(), page.getContentType());
				}
			} else {
				logger.debug("Can't read this robots.txt: {}  as it's status code is {}", robotsTxtUrl.getURL(),
						fetchResult.getStatusCode());
			}
		} catch (SocketException | UnknownHostException | SocketTimeoutException | NoHttpResponseException se) {
			//robot.txt 不存在
		} catch (PageBiggerThanMaxSizeException pbtms) {
			logger.error("Error occurred while fetching (robots) url: {}, {}", robotsTxtUrl.getURL(),
					pbtms.getMessage());
		} catch (Exception e) {
			logger.error("Error occurred while fetching (robots) url: " + robotsTxtUrl.getURL(), e);
		} finally {
			if (fetchResult != null) {
				fetchResult.discardContentIfNotConsumed();
			}
		}

		if (directives == null) {
			// We still need to have this object to keep track of the time we
			// fetched it
			directives = new HostDirectives();
		}
		synchronized (host2directivesCache) {
			//当host robots缓存数量超过限制，则把一些上次更新时间小于某一值得robots移除，以腾出空间存储新的。
			if (host2directivesCache.size() == config.getCacheSize()) {
				String minHost = null;
				long minAccessTime = Long.MAX_VALUE;
				for (Map.Entry<String, HostDirectives> entry : host2directivesCache.entrySet()) {
					if (entry.getValue().getLastAccessTime() < minAccessTime) {
						minAccessTime = entry.getValue().getLastAccessTime();
						minHost = entry.getKey();
					}
				}
				host2directivesCache.remove(minHost); //移除
			}
			host2directivesCache.put(host, directives);//添加新的
		}
		return directives;
	}
}