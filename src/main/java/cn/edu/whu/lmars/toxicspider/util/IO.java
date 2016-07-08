package cn.edu.whu.lmars.toxicspider.util;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO工具类
 * @author REN
 * @date 2016年6月7日 下午2:27:29
 */
public class IO {
	private static final Logger logger = LoggerFactory.getLogger(IO.class);

	/**
	 * 删除此文件夹和文件夹下的所有文件
	 * @param folder 文件目录路径
	 * @return
	 */
	public static boolean deleteFolder(File folder) {
		return deleteFolderContents(folder) && folder.delete();
	}
	/**
	 * 删除文件夹里面的所有文件
	 *
	 */
	public static boolean deleteFolderContents(File folder) {
		logger.debug("Deleting content of: " + folder.getAbsolutePath());
		File[] files = folder.listFiles();
		for (File file : files) {
			if (file.isFile()) { //如果是标准文件，则删除文件
				if (!file.delete()) {
					return false;
				}
			} else {
				if (!deleteFolder(file)) {//如果是文件夹则递归调用deleteFolder()方法
					return false;
				}
			}
		}
		return true;
	}
}