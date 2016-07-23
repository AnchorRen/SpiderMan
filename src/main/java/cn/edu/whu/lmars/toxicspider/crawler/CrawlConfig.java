package cn.edu.whu.lmars.toxicspider.crawler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import cn.edu.whu.lmars.toxicspider.crawler.authentication.AuthInfo;

/**
 * 爬虫的配置类，包含了爬虫的配置信息
 * 
 * @author REN
 *
 */
public class CrawlConfig {

	/**
	 * 此文件夹用于存储爬虫爬取过程中的中间爬取数据，不要手动修改。
	 */
	private String crawlStorageFolder;

	/**
	 * 如果设置为true，则可以恢复之前意外停止或者崩溃的爬虫。 但是恢复过程会花费一定时间。
	 */
	private boolean resumableCrawling = false;

	/**
	 * 爬虫的爬取深度。 如果设置为-1 则为不限制深度。
	 */
	private int maxDepthOfCrawling = -1;

	/**
	 * 最大的网页爬取数量。 设置为-1则为不限制深度。
	 */
	private int maxPagesToFetch = -1;

	/**
	 * 代表爬虫的user-agent. 可以通过设置这个参数进行伪装浏览器或者百度谷歌等搜索引擎。 详情：
	 * http://en.wikipedia.org/wiki/User_agent
	 */
	private String userAgentString = "Baiduspider";

	/**
	 * 默认的请求头信息。
	 */
	private Collection<BasicHeader> defaultHeaders = new HashSet<BasicHeader>();

	/**
	 * 对同一个主机发送连续两个请求的时间间隔（毫秒）。 如果间隔过小，则访问过频繁可能导致当前IP被封。
	 */
	private int politenessDelay = 200;

	/**
	 * 是否抓取https 开头的网站。 这种网站为加密网站。比如一些涉及支付的网页。
	 */
	private boolean includeHttpsPages = true;

	/**
	 * 是否抓取二进制的内容，比如图片、音乐、压缩文件等
	 */
	private boolean includeBinaryContentInCrawling = false;

	/**
	 * 是否使用Apache TIKA 处理二进制的内容，比如图片、音乐、压缩文件等
	 */
	private boolean processBinaryContentInCrawling = false;

	/**
	 * 每个主机的最大连接数
	 */
	private int maxConnectionsPerHost = 100;

	/**
	 * 最大总连接数
	 */
	private int maxTotalConnections = 100;

	/**
	 * Socket 超时时间
	 */
	private int socketTimeout = 20000;

	/**
	 * Connection 连接超时时间
	 */
	private int connectionTimeout = 30000;

	/**
	 * 每个网页的最大子链接数限制。
	 */
	private int maxOutgoingLinksToFollow = 5000;

	/**
	 * 网页最大下载量。 网页内容大于这个值将不会被抓取。
	 */
	private int maxDownloadSize = 1048576;

	/**
	 * 是否抓取重定向的链接？
	 */
	private boolean followRedirects = true;

	/**
	 * 每次运行的时候是否通过网络更新TLD列表？
	 * TRUE:从网上下载最新的有效tld表。（https://publicsuffix.org/list/effective_tld_names.
	 * dat） FALSE： 使用程序中resource中的tld-names.txt
	 */
	private boolean onlineTldListUpdate = false;

	/**
	 * 当工作队列 workQuene为空的时候，是否停止爬虫程序
	 */
	private boolean shutdownOnEmptyQueue = true;

	/**
	 * 需要使用代理的话，设置代理主机
	 */
	private String proxyHost = null;

	/**
	 * 设置代理的代理端口号
	 */
	private int proxyPort = 80;

	/**
	 * 如果代理需要用户名和密码授权， 需要设置代理的username
	 */
	private String proxyUsername = null;

	/**
	 * 如果代理需要用户名和密码授权， 需要设置代理的password
	 */
	private String proxyPassword = null;

	/**
	 * 爬虫可能需要的认证信息。
	 */
	private List<AuthInfo> authInfos;

	/**
	 * 验证部分配置信息。
	 * 
	 * @throws Exception
	 *             on Validation fail
	 */
	public void validate() throws Exception {
		if (crawlStorageFolder == null) {
			throw new Exception("Crawl storage folder is not set in the CrawlConfig.");
		}
		if (politenessDelay < 0) {
			throw new Exception("Invalid value for politeness delay: " + politenessDelay);
		}
		if (maxDepthOfCrawling < -1) {
			throw new Exception("Maximum crawl depth should be either a positive number or -1 for unlimited depth.");
		}
		if (maxDepthOfCrawling > Short.MAX_VALUE) {
			throw new Exception("Maximum value for crawl depth is " + Short.MAX_VALUE);
		}
	}

	public String getCrawlStorageFolder() {
		return crawlStorageFolder;
	}

	public void setCrawlStorageFolder(String crawlStorageFolder) {
		this.crawlStorageFolder = crawlStorageFolder;
	}

	public boolean isResumableCrawling() {
		return resumableCrawling;
	}

	public void setResumableCrawling(boolean resumableCrawling) {
		this.resumableCrawling = resumableCrawling;
	}

	public int getMaxDepthOfCrawling() {
		return maxDepthOfCrawling;
	}

	public void setMaxDepthOfCrawling(int maxDepthOfCrawling) {
		this.maxDepthOfCrawling = maxDepthOfCrawling;
	}

	public int getMaxPagesToFetch() {
		return maxPagesToFetch;
	}

	public void setMaxPagesToFetch(int maxPagesToFetch) {
		this.maxPagesToFetch = maxPagesToFetch;
	}

	public String getUserAgentString() {
		return userAgentString;
	}

	public void setUserAgentString(String userAgentString) {
		this.userAgentString = userAgentString;
	}

	public Collection<BasicHeader> getDefaultHeaders() {
		return new HashSet<>(defaultHeaders);
	}

	public void setDefaultHeaders(Collection<? extends Header> defaultHeaders) {
		Collection<BasicHeader> copiedHeaders = new HashSet<>();
		for (Header header : defaultHeaders) {
			copiedHeaders.add(new BasicHeader(header.getName(), header.getValue()));
		}
		this.defaultHeaders = copiedHeaders;
	}

	public int getPolitenessDelay() {
		return politenessDelay;
	}

	public void setPolitenessDelay(int politenessDelay) {
		this.politenessDelay = politenessDelay;
	}

	public boolean isIncludeHttpsPages() {
		return includeHttpsPages;
	}

	public void setIncludeHttpsPages(boolean includeHttpsPages) {
		this.includeHttpsPages = includeHttpsPages;
	}

	public boolean isIncludeBinaryContentInCrawling() {
		return includeBinaryContentInCrawling;
	}

	public void setIncludeBinaryContentInCrawling(boolean includeBinaryContentInCrawling) {
		this.includeBinaryContentInCrawling = includeBinaryContentInCrawling;
	}

	public boolean isProcessBinaryContentInCrawling() {
		return processBinaryContentInCrawling;
	}

	public void setProcessBinaryContentInCrawling(boolean processBinaryContentInCrawling) {
		this.processBinaryContentInCrawling = processBinaryContentInCrawling;
	}

	public int getMaxConnectionsPerHost() {
		return maxConnectionsPerHost;
	}

	public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
		this.maxConnectionsPerHost = maxConnectionsPerHost;
	}

	public int getMaxTotalConnections() {
		return maxTotalConnections;
	}

	public void setMaxTotalConnections(int maxTotalConnections) {
		this.maxTotalConnections = maxTotalConnections;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public int getMaxOutgoingLinksToFollow() {
		return maxOutgoingLinksToFollow;
	}

	public void setMaxOutgoingLinksToFollow(int maxOutgoingLinksToFollow) {
		this.maxOutgoingLinksToFollow = maxOutgoingLinksToFollow;
	}

	public int getMaxDownloadSize() {
		return maxDownloadSize;
	}

	public void setMaxDownloadSize(int maxDownloadSize) {
		this.maxDownloadSize = maxDownloadSize;
	}

	public boolean isFollowRedirects() {
		return followRedirects;
	}

	public void setFollowRedirects(boolean followRedirects) {
		this.followRedirects = followRedirects;
	}

	public boolean isShutdownOnEmptyQueue() {
		return shutdownOnEmptyQueue;
	}

	public void setShutdownOnEmptyQueue(boolean shutdown) {
		shutdownOnEmptyQueue = shutdown;
	}

	public boolean isOnlineTldListUpdate() {
		return onlineTldListUpdate;
	}

	public void setOnlineTldListUpdate(boolean online) {
		onlineTldListUpdate = online;
	}

	public String getProxyHost() {
		return proxyHost;
	}

	public void setProxyHost(String proxyHost) {
		this.proxyHost = proxyHost;
	}

	public int getProxyPort() {
		return proxyPort;
	}

	public void setProxyPort(int proxyPort) {
		this.proxyPort = proxyPort;
	}

	public String getProxyUsername() {
		return proxyUsername;
	}

	public void setProxyUsername(String proxyUsername) {
		this.proxyUsername = proxyUsername;
	}

	public String getProxyPassword() {
		return proxyPassword;
	}

	public void setProxyPassword(String proxyPassword) {
		this.proxyPassword = proxyPassword;
	}

	public List<AuthInfo> getAuthInfos() {
		return authInfos;
	}

	public void addAuthInfo(AuthInfo authInfo) {
		if (this.authInfos == null) {
			this.authInfos = new ArrayList<AuthInfo>();
		}

		this.authInfos.add(authInfo);
	}

	public void setAuthInfos(List<AuthInfo> authInfos) {
		this.authInfos = authInfos;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Crawl storage folder: " + getCrawlStorageFolder() + "\n");
		sb.append("Resumable crawling: " + isResumableCrawling() + "\n");
		sb.append("Max depth of crawl: " + getMaxDepthOfCrawling() + "\n");
		sb.append("Max pages to fetch: " + getMaxPagesToFetch() + "\n");
		sb.append("User agent string: " + getUserAgentString() + "\n");
		sb.append("Include https pages: " + isIncludeHttpsPages() + "\n");
		sb.append("Include binary content: " + isIncludeBinaryContentInCrawling() + "\n");
		sb.append("Max connections per host: " + getMaxConnectionsPerHost() + "\n");
		sb.append("Max total connections: " + getMaxTotalConnections() + "\n");
		sb.append("Socket timeout: " + getSocketTimeout() + "\n");
		sb.append("Max total connections: " + getMaxTotalConnections() + "\n");
		sb.append("Max outgoing links to follow: " + getMaxOutgoingLinksToFollow() + "\n");
		sb.append("Max download size: " + getMaxDownloadSize() + "\n");
		sb.append("Should follow redirects?: " + isFollowRedirects() + "\n");
		sb.append("Proxy host: " + getProxyHost() + "\n");
		sb.append("Proxy port: " + getProxyPort() + "\n");
		sb.append("Proxy username: " + getProxyUsername() + "\n");
		sb.append("Proxy password: " + getProxyPassword() + "\n");
		return sb.toString();
	}
}
