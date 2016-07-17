package cn.edu.whu.lmars.toxicspider.parser;

import java.util.Set;

import cn.edu.whu.lmars.toxicspider.url.WebURL;
/**
 * 数据解析接口
 * @author REN
 *
 */
public interface ParseData {

	/**
	 * 获取子URLs
	 * @return WebURL 集合
	 */
  Set<WebURL> getOutgoingUrls();

  void setOutgoingUrls(Set<WebURL> outgoingUrls);

  @Override
  String toString();
}