package cn.edu.whu.lmars.toxicspider.parser;

/**
 *
 *	不允许解析异常类
 */
public class NotAllowedContentException extends Exception {
  public NotAllowedContentException() {
    super("Not allowed to parse this type of content");
  }
}