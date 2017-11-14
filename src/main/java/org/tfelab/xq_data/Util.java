package org.tfelab.xq_data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.tfelab.io.requester.Task;
import org.tfelab.io.requester.account.AccountWrapper;
import org.tfelab.io.requester.account.AccountWrapperImpl;
import org.tfelab.util.FileUtil;
import org.tfelab.xq_data.proxy.ProxyManager;
import org.tfelab.xq_data.task.UserExtractTask;

import java.util.LinkedList;
import java.util.List;

public class Util {

	public static final Logger logger = LogManager.getLogger(Util.class.getName());

	public static List<Task> initSeedUserTask() {

		AccountWrapper accountWrapper = new AccountWrapperImpl().setProxyGroup(ProxyManager.abuyun_g);

		String[] ids = FileUtil.readFileByLines("id_list").split("\n");
		System.err.println(ids.length);
		List<Task> tasks = new LinkedList<>();
		for(String id:ids) {
			Task t = UserExtractTask.generateTask(id, 1, accountWrapper);
			tasks.add(t);
		}
		return tasks;
	}

	public static void main(String[] args) throws Exception {
		//getAllStockInfo();
		Util.initSeedUserTask();
	}

}
