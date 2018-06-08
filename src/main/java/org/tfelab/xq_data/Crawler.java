package org.tfelab.xq_data;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jdk.internal.cmm.SystemResourcePressureImpl;
import one.rewind.io.requester.BasicRequester;
import one.rewind.io.requester.Task;
import one.rewind.io.requester.chrome.ChromeDriverRequester;
import one.rewind.io.requester.proxy.IpDetector;
import one.rewind.io.requester.proxy.Proxy;
import one.rewind.util.Configs;
import one.rewind.util.NetworkUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.tfelab.xq_data.proxy.ProxyManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Crawler<T extends Task> {

	public static final Logger logger = LogManager.getLogger(Crawler.class.getName());
	public static String LOCAL_IP = IpDetector.getIp() + " :: " + NetworkUtil.getLocalIp();

	protected static Crawler instance;

	public static Crawler getInstance() {

		if (instance == null) {
			synchronized (ProxyManager.class) {
				if (instance == null) {
					instance = new Crawler();
				}
			}
		}

		return instance;
	}

	// 此处不应人为设定
	static int REQUEST_PER_SECOND_LIMIT = 20;

	// 单次请求TIMEOUT
	static int CONNECT_TIMEOUT = 120000;
	static int RETRY_LIMIT = 10;

	// 初始化配置参数
	static {

		try {
			REQUEST_PER_SECOND_LIMIT = Configs.getConfig(BasicRequester.class).getInt("requestPerSecondLimit");
		} catch (Exception e) {
			logger.error(e);
		}
	}

	private Map<String, Distributor> distributors = new HashMap<>();

	private Crawler() {

	}

	public boolean tasksDone(String className) throws InterruptedException {

		int count = 0;
		Distributor d = distributors.get(className);
		if(d == null) return true;

		for(int i=0; i<3; i++) {
			count += d.taskQueue == null? 0 : d.taskQueue.size();
			count += d.executor.getActiveCount();
			count += d.executor.getQueue().size();
			Thread.sleep(4000);
		}

		if(count == 0) return true;
		return false;
	}

	/**
	 *
	 * @param className
	 */
	void createDistributor(String className) {
		Distributor d = new Distributor(className);
		d.setPriority(7);
		d.start();
		distributors.put(className, d);
	}

	/**
	 *
	 * @param t
	 */
	public void addTask(T t) {

		if(t == null) return;
		String className = t.getClass().getSimpleName();

		if (distributors.get(className) == null) {
			createDistributor(className);
		}

		if(t.getPriority().equals(Task.Priority.HIGH)) {
			try {
				distributors.get(className).distribute(t);
			} catch (Exception e) {
				logger.error("Error add prior task. ", e);
			}
		} else {

			distributors.get(className).taskQueue.add(t);
		}

	}

	/**
	 *
	 * @param ts
	 */
	public void addTask(List<T> ts) {

		if(ts == null) return;

		for(T t : ts) {
			addTask(t);
		}
	}

	/**
	 * 任务指派类
	 */
	class Distributor extends Thread {

		BlockingQueue<T> taskQueue;

		private volatile boolean done = false;

		ThreadPoolExecutor executor =  new ThreadPoolExecutor(
				2 * REQUEST_PER_SECOND_LIMIT,
				4 * REQUEST_PER_SECOND_LIMIT,
				0, TimeUnit.MICROSECONDS,
				new ArrayBlockingQueue<Runnable>(1000000));

		/**
		 *
		 * @param taskQueueName
		 */
		public Distributor (String taskQueueName) {

			taskQueue = new LinkedBlockingQueue();

			executor.setThreadFactory(new ThreadFactoryBuilder()
					.setNameFormat(taskQueueName + "Operator-Worker-%d").build());

			this.setName(this.getClass().getSimpleName() + "-" + taskQueueName);
		}

		/**
		 *
		 */
		public void run() {

			logger.info("{} started.", this.getName());

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error(e);
			}

			while(!done) {

				T t = null;

				try {

					t = taskQueue.take();
					distribute(t);

				} catch (InterruptedException e) {
					logger.error(e);
				} catch (Exception e) {
					logger.error("{}", t.toJSON(), e);
					System.exit(0);
				}
			}
		}

		public void distribute(T t) throws Exception {

			Proxy proxy = null;

			/**
			 * 根据AccountWrapper设定proxy
			 */
			if(t.getAccount() != null) {

				if(t.getAccount().getProxyId() != null) {
					proxy = ProxyManager.getInstance().getProxyById(t.getAccount().getProxyId());
					if(proxy != null) {
						t.setProxy(proxy);
					} else {
						logger.error("No available proxy. Exit.");
						System.exit(0);
					}

				}
				else if(t.getAccount().getProxyGroup() != null) {
					proxy = ProxyManager.getInstance().getProxyByGroup(t.getAccount().getProxyGroup());
					if(proxy != null) {
						t.setProxy(proxy);
					} else {
						logger.error("No available proxy. Exit.");
						System.exit(0);
					}
				}
			}

			/*// TODO Degenerated Case.
			if(t.getRetryCount() > 1) {
				t.setProxy(Proxy.getValidProxy("aliyun"));
			}*/

			Operator o = new Operator(t);
			if(proxy != null) {
				ProxyManager.getInstance().waits(proxy);
			}

			if(Thread.currentThread().getClass().equals(Distributor.class))
				waitForOperators();

			executor.submit(o);

			logger.info("Executor task queue active: {}, queue: {} ", executor.getActiveCount(), executor.getQueue().size());
		}

		/**
		 *
		 * @throws InterruptedException
		 */
		private void waitForOperators() throws InterruptedException {

			if(executor.getQueue().size() > 6 * REQUEST_PER_SECOND_LIMIT) {

			 	long sleepTime = 1 * Math.round(executor.getQueue().size() / ( REQUEST_PER_SECOND_LIMIT));

				logger.warn("Wait for operators {}s.", sleepTime);
				Thread.sleep(sleepTime * 1000);
			}
		}

		public void setDone() {
			this.done = true;
		}

		/**
		 *
		 */
		class Operator implements Runnable {

			T t;

			public Operator(T t) {
				this.t = t;
			}

			public void run() {

				try {

					t.setResponse();

					if(t.getRequester_class() != null
							&& t.getRequester_class().equals(ChromeDriverRequester.class.getSimpleName()))
					{
						ChromeDriverRequester.getInstance().submit(t);
					} else {
						BasicRequester.getInstance().submit(t, CONNECT_TIMEOUT);
					}

					for (Runnable runnable : t.doneCallBacks) {
						runnable.run();
					}

					StatManager.getInstance().count();

					/**
					 * 重试逻辑
					 */
					if (t.getExceptions().size() > 0) {

						for(Throwable e : t.getExceptions()) {
							logger.error("Fetch Error: {}.", t.getUrl(), e);
						}

						if(t.getRetryCount() < RETRY_LIMIT) {
							t.addRetryCount();
							addTask(t);
						} else {
							t.insert();
						}

						return;
					}

					logger.info("{} duration: {}", t.getUrl(), t.getDuration());

				} catch (Exception e) {

					logger.error(e);

				}
			}
		}
	}

	/**
	 *
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		StatManager.getInstance();
		Crawler crawler = Crawler.getInstance();
		crawler.addTask(Util.initSeedUserTask());
	}
}
