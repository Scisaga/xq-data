package org.tfelab.xq_data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import one.rewind.io.requester.Task;
import one.rewind.io.requester.account.Account;
import one.rewind.io.requester.account.AccountImpl;
import one.rewind.util.FileUtil;
import org.tfelab.xq_data.proxy.ProxyManager;
import org.tfelab.xq_data.task.UserExtractTask;

import java.util.LinkedList;
import java.util.List;

public class Util {

	public static final Logger logger = LogManager.getLogger(Util.class.getName());

	public static List<Task> initSeedUserTask() {

		Account account = new AccountImpl(null, null, null).
				setProxyGroup(ProxyManager.abuyun_g);

		String[] ids = FileUtil.readFileByLines("id_list").split("\n");
		System.err.println(ids.length);

		List<Task> tasks = new LinkedList<>();
		for(String id:ids) {
			Task t = UserExtractTask.generateTask(id, 1, account);
			tasks.add(t);
		}

		return tasks;
	}

	public static void main(String[] args) throws Exception {
		//getAllStockInfo();
		Util.initSeedUserTask();
	}

}
