package cn.edu.whu.lmars.toxicspider.tests;

import org.junit.Test;

import cn.edu.whu.lmars.toxicspider.url.WebURL;


/**
 * Created by Avi on 8/19/2014.
 *
 */
public class WebURLTest {

  @Test
  public void testNoLastSlash() {
    WebURL webUrl = new WebURL();
    webUrl.setURL("http://google.com");
  }
}