package cn.edu.whu.lmars.toxicspider.frontier;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import cn.edu.whu.lmars.toxicspider.url.WebURL;

/**
 * TupleBinding：An abstract EntryBinding that treats a key or data entry as a tuple; 
 * it includes predefined bindings for Java primitive types. 
 * 
 * This class takes care of converting the entries to/from TupleInput and TupleOutput objects. 
 * Its two abstract methods must be implemented by a concrete subclass to convert between tuples and key or data objects.
	
	WebURL 和 元组 绑定
 * @author REN
 *
 */
public class WebURLTupleBinding extends TupleBinding<WebURL> {

  @Override
  public WebURL entryToObject(TupleInput input) {
    WebURL webURL = new WebURL();
    webURL.setURL(input.readString());
    webURL.setDocid(input.readInt());
    webURL.setParentDocid(input.readInt());
    webURL.setParentUrl(input.readString());
    webURL.setDepth(input.readShort());
    webURL.setPriority(input.readByte());
    webURL.setAnchor(input.readString());
    return webURL;
  }

  @Override
  public void objectToEntry(WebURL url, TupleOutput output) {
    output.writeString(url.getURL());
    output.writeInt(url.getDocid());
    output.writeInt(url.getParentDocid());
    output.writeString(url.getParentUrl());
    output.writeShort(url.getDepth());
    output.writeByte(url.getPriority());
    output.writeString(url.getAnchor());
  }
}