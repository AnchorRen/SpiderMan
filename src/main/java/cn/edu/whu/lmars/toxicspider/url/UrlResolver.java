package cn.edu.whu.lmars.toxicspider.url;

/**
 * Url解析器
 * 
 * @author REN
 */
public final class UrlResolver {

	/**
	 * 在父URL中发现的URL链接是相对路径的，转换为相应的绝对路径 http://www.faqs.org/rfcs/rfc1808.html"
	 * Section 4 for more details.
	 *
	 * @param baseUrl
	 *            基URL，用来规范化用
	 * @param relativeUrl
	 *            此基URl对应的相对URL
	 * @return 规范化后的URL
	 */
	public static String resolveUrl(final String baseUrl, final String relativeUrl) {
		if (baseUrl == null) {
			throw new IllegalArgumentException("Base URL must not be null");
		}

		if (relativeUrl == null) {
			throw new IllegalArgumentException("Relative URL must not be null");
		}

		final Url url = resolveUrl(parseUrl(baseUrl.trim()), relativeUrl.trim());
		return url.toString();
	}

	/**
	 * 返回一个字符串中指定字符首次出现的索引位置。
	 *
	 * @param s
	 *            要搜索的字符串
	 * @param searchChar
	 *            要搜索的字符
	 * @param beginIndex
	 *            搜索的其实索引位置
	 * @param endIndex
	 *            结束搜索的索引位置
	 * @return 返回在起始和终止位置之间首次出现要搜索字符的索引，如果未找到则返回-1。
	 */
	private static int indexOf(final String s, final char searchChar, final int beginIndex, final int endIndex) {
		for (int i = beginIndex; i < endIndex; i++) {
			if (s.charAt(i) == searchChar) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * 	根据特定规则解析一个url
	 * 参考："http://www.faqs.org/rfcs/rfc1808.html":
	 *
	 * Section 2.4: Parsing a URL
	 *
	 * @param spec
	 *            需要解析的内容
	 * @return 
	 * 			解析后的内容
	 */
	private static Url parseUrl(final String spec) {
		final Url url = new Url();
		int startIndex = 0;
		int endIndex = spec.length();

		/**
		 * 查找URL中“#”后面的部分，在URL的请求过程中#好后面的部分不会发送到服务器， 作用是在HTMl页面进行定位的。
		 */
		final int crosshatchIndex = indexOf(spec, '#', startIndex, endIndex);

		if (crosshatchIndex >= 0) {
			url.fragment_ = spec.substring(crosshatchIndex + 1, endIndex);
			endIndex = crosshatchIndex;
		}
		/**
		 * 获取网络协议 例如 http://www.baidu.com schame ： http
		 */
		final int colonIndex = indexOf(spec, ':', startIndex, endIndex);

		if (colonIndex > 0) {
			final String scheme = spec.substring(startIndex, colonIndex);
			if (isValidScheme(scheme)) {
				url.scheme_ = scheme;
				startIndex = colonIndex + 1;
			}
		}
		/**
		 * 如果要解析的字符串以双斜线 '//'开始，那么从双斜线之后开始一直到最后的子字符串中 的下一个 '/' 就是网络位置符号了。如果后面没有
		 * '/' ，则整个string添加到path_ 双斜线需要移除。
		 */
		final int locationStartIndex;
		int locationEndIndex;

		if (spec.startsWith("//", startIndex)) {
			locationStartIndex = startIndex + 2;
			locationEndIndex = indexOf(spec, '/', locationStartIndex, endIndex);
			if (locationEndIndex >= 0) {
				startIndex = locationEndIndex;
			}
		} else {
			locationStartIndex = -1;
			locationEndIndex = -1;
		}
		/**
		 * 如果string中含有问号 '?' ，则问号后面的内容为get请求的参数。 这部分内容需要截取下来作为url 的参数query_.
		 */
		final int questionMarkIndex = indexOf(spec, '?', startIndex, endIndex);

		if (questionMarkIndex >= 0) {
			if ((locationStartIndex >= 0) && (locationEndIndex < 0)) {
				locationEndIndex = questionMarkIndex;
				startIndex = questionMarkIndex;
			}
			url.query_ = spec.substring(questionMarkIndex + 1, endIndex);
			endIndex = questionMarkIndex;
		}
		/**
		 * 如果要解析的字符串中包含分号 ';'，则从分号开始一直到最后的部分是 参数部分。
		 * 如果分号在字符串的最后或者 不包含分号，则参数部分为空。
		 * 在下次继续之前，要移除分号以及分号后面的部分。
		 */
		final int semicolonIndex = indexOf(spec, ';', startIndex, endIndex);

		if (semicolonIndex >= 0) {
			if ((locationStartIndex >= 0) && (locationEndIndex < 0)) {
				locationEndIndex = semicolonIndex;
				startIndex = semicolonIndex;
			}
			url.parameters_ = spec.substring(semicolonIndex + 1, endIndex);
			endIndex = semicolonIndex;
		}
		/**
		 * 解析url路径
		 * 
		 * 经过上面的步骤，剩下的字符串为Url的 path 还有可能在之前的 '/'，
		 * 虽然初始的斜线 '/' 不是Url path 的一部分，但是解析器必须记录这个斜线是否出现了，
		 * 因为后续步骤需要通过这个斜线区分绝对路径和相对路径。而，通过存储path前面的斜线
		 * '/' 就能够很简单的做到这点。
		 */
		if ((locationStartIndex >= 0) && (locationEndIndex < 0)) {
			locationEndIndex = endIndex;
		} else if (startIndex < endIndex) {
			url.path_ = spec.substring(startIndex, endIndex);
		}
		if ((locationStartIndex >= 0) && (locationEndIndex >= 0)) {
			url.location_ = spec.substring(locationStartIndex, locationEndIndex);
		}
		return url;
	}

	/*
	 * 检验一个schame是否为有效的。
	 */
	private static boolean isValidScheme(final String scheme) {
		final int length = scheme.length();
		if (length < 1) {
			return false;
		}
		char c = scheme.charAt(0);
		if (!Character.isLetter(c)) {
			return false;
		}
		for (int i = 1; i < length; i++) {
			c = scheme.charAt(i);
			if (!Character.isLetterOrDigit(c) && (c != '.') && (c != '+') && (c != '-')) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 解决绝对路径和相对路径问题
	 * http://www.faqs.org/rfcs/rfc1808.html"
	 *
	 * Section 4: Resolving Relative URLs
	 *
	 * @param baseUrl
	 *            baseUrl
	 * @param relativeUrl
	 *            relativeUrl
	 * @return 
	 * 			处理后的Url
	 */
	private static Url resolveUrl(final Url baseUrl, final String relativeUrl) {
		final Url url = parseUrl(relativeUrl);
		/**
		 * step 1:
		 * 		如果baseURL为空，那么内含的Url就认为是绝对路径，直接返回。
		 */
		if (baseUrl == null) {
			return url;
		}
		/**
		 * 如果内嵌Url为空，它继承于baseUrl，那个返回baseUrl
		 */
		if (relativeUrl.isEmpty()) {
			return new Url(baseUrl);
		}
		/**
		 * b) 
		 * 		如果内嵌Url以一个schame开始，那么久认为这个url为绝对路径。
		 */
		if (url.scheme_ != null) {
			return url;
		}
		/**
		 *  c)
		 *  	否则，内嵌Url继承baseUrl的schame。
		 */
		url.scheme_ = baseUrl.scheme_;
		/**
		 * step3:
		 * 		如果内嵌Url的 location_不为空，则返回。
		 * 		否则内嵌Url继承baseUrl的location_
		 */
		if (url.location_ != null) {
			return url;
		}
		url.location_ = baseUrl.location_;
		/**
		 * step4:
		 * 		如果内嵌Url以'/' 开头，路径不是相对路径，返回。
		 */
		if ((url.path_ != null) && ((!url.path_.isEmpty()) && (url.path_.charAt(0) == '/'))) {
			url.path_ = removeLeadingSlashPoints(url.path_);
			return url;
		}
		/**
		 * step 5:
		 * 
		 * 		如果内嵌Url 的path_为空，而且不是以 '/' 开头，那么内嵌url继承baseUrl的path_
		 */
		if (url.path_ == null) {
			url.path_ = baseUrl.path_;
			/**
			 * a)
			 * 		如果内嵌Url的 参数不为空，则返回。
			 * 		否则继承baseUrl的 parameters_
			 */
			if (url.parameters_ != null) {
				return url;
			}
			url.parameters_ = baseUrl.parameters_;
			/**
			 *  b)
			 *  	如果内嵌Url的 query_不为空，则返回。
			 *  	否则，继承baseUrl的 query_.返回
			 */
			if (url.query_ != null) {
				return url;
			}
			url.query_ = baseUrl.query_;
			return url;
		}
		/**
		 * step 6:
		 * 		最后baseUrl的 path（最右的 '/'后面的部分，如果没有'/'就是整个path）需要移除，
		 * 		然后内嵌Url的path追加到baseUrl的这个位置。
		 */
		final String basePath = baseUrl.path_;
		String path = "";

		if (basePath != null) {
			final int lastSlashIndex = basePath.lastIndexOf('/');

			if (lastSlashIndex >= 0) {
				path = basePath.substring(0, lastSlashIndex + 1);
			}
		} else {
			path = "/";
		}
		path = path.concat(url.path_);
		/**
		 *  a)
		 *  	所有的 "./", '.' 就是完整路径分隔符，需要移除
		 */
		int pathSegmentIndex;

		while ((pathSegmentIndex = path.indexOf("/./")) >= 0) {
			path = path.substring(0, pathSegmentIndex + 1).concat(path.substring(pathSegmentIndex + 3));
		}
		/**
		 *  b)
		 *  	如果路径以 '.'结尾，则需要移除 '.'
		 */
		if (path.endsWith("/.")) {
			path = path.substring(0, path.length() - 1);
		}
		/**
		 *  c)
		 *  	在所有的 "/../" 需要移除。
		 *  	出去所有的这些segment需要迭代移除进行。
		 */
		while ((pathSegmentIndex = path.indexOf("/../")) > 0) {
			final String pathSegment = path.substring(0, pathSegmentIndex);
			final int slashIndex = pathSegment.lastIndexOf('/');

			if (slashIndex < 0) {
				continue;
			}
			if (!"..".equals(pathSegment.substring(slashIndex))) {
				path = path.substring(0, slashIndex + 1).concat(path.substring(pathSegmentIndex + 4));
			}
		}
		/**
		 * d)
		 * 		如果path 以 "/.." 结尾，需要移除
		 */
		if (path.endsWith("/..")) {
			final String pathSegment = path.substring(0, path.length() - 3);
			final int slashIndex = pathSegment.lastIndexOf('/');

			if (slashIndex >= 0) {
				path = path.substring(0, slashIndex + 1);
			}
		}

		path = removeLeadingSlashPoints(path);

		url.path_ = path;
		/**
		 * step 7
		 * 		返回最终拼接处理好的url
		 */
		return url;
	}

	/**
	 * 以"/.."开头的url需要移除这部分
	 */
	private static String removeLeadingSlashPoints(String path) {
		while (path.startsWith("/..")) {
			path = path.substring(3);
		}
		return path;
	}

	/**
	 * 静态内部类 URL:统一资源定位符
	 */
	private static class Url {

		String scheme_;
		String location_;
		String path_;
		String parameters_;
		String query_;
		String fragment_;

		/**
		 * 空构造函数
		 */
		public Url() {
		}

		/**
		 * 通过指定的Url创建Url对象
		 *
		 * @param url
		 */
		public Url(final Url url) {
			scheme_ = url.scheme_;
			location_ = url.location_;
			path_ = url.path_;
			parameters_ = url.parameters_;
			query_ = url.query_;
			fragment_ = url.fragment_;
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();

			if (scheme_ != null) {
				sb.append(scheme_);
				sb.append(':');
			}
			if (location_ != null) {
				sb.append("//");
				sb.append(location_);
			}
			if (path_ != null) {
				sb.append(path_);
			}
			if (parameters_ != null) {
				sb.append(';');
				sb.append(parameters_);
			}
			if (query_ != null) {
				sb.append('?');
				sb.append(query_);
			}
			if (fragment_ != null) {
				sb.append('#');
				sb.append(fragment_);
			}
			return sb.toString();
		}
	}
}
