package org.tfelab.xq_data;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.api.RBlockingQueue;
import org.redisson.client.RedisTimeoutException;
import one.rewind.util.Configs;
import one.rewind.db.RedissonAdapter;
import one.rewind.json.JSON;
import one.rewind.io.requester.BasicRequester;
import one.rewind.io.requester.chrome.ChromeDriverRequester;
import one.rewind.io.requester.Task;
import one.rewind.io.requester.proxy.Proxy;
import org.tfelab.xq_data.model.ProxyImpl;
import one.rewind.io.requester.proxy.IpDetector;
import org.tfelab.xq_data.proxy.ProxyManager;
import one.rewind.txt.DateFormatUtil;
import one.rewind.util.NetworkUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.Date;

public class Crawler {

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

	private Map<Class<? extends Task>, Distributor<? extends Task>> distributors = new HashMap<>();

	private Crawler() {

	}

	public boolean tasksDone(Class<? extends Task> clazz) throws InterruptedException {

		int count = 0;
		Distributor d = distributors.get(clazz);
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
	 * @param clazz
	 */
	void createDistributor(Class<? extends Task> clazz) {
		Distributor d = new Distributor(clazz.getSimpleName() + "-queue", clazz);
		d.setPriority(7);
		d.start();
		distributors.put(clazz, d);
	}

	/**
	 *
	 * @param t
	 */
	public void addTask(Task t) {

		if(t == null) return;

		if (distributors.get(t.getClass()) == null) {
			createDistributor(t.getClass());
		}

		if(t.getPriority().equals(Task.Priority.HIGH)) {
			try {
				distributors.get(t.getClass()).distribute(t.toJSON());
			} catch (Exception e) {
				logger.error("Error add prior task. ", e);
			}
		} else {
			distributors.get(t.getClass()).taskQueue.offer(t.toJSON());
		}

	}

	/**
	 *
	 * @param ts
	 */
	public void addTask(List<Task> ts) {

		if(ts == null) return;

		for(Task t : ts) {
			addTask(t);
		}
	}

	/**
	 * 任务指派类
	 */
	class Distributor<T extends Task> extends Thread {

		RBlockingQueue<String> taskQueue;

		private volatile boolean done = false;

		ThreadPoolExecutor executor =  new ThreadPoolExecutor(
				2 * REQUEST_PER_SECOND_LIMIT,
				4 * REQUEST_PER_SECOND_LIMIT,
				0, TimeUnit.MICROSECONDS,
				new ArrayBlockingQueue<Runnable>(1000000));

		Class<T> clazz;

		/**
		 *
		 * @param taskQueueName
		 * @param clazz
		 */
		public Distributor (String taskQueueName, Class<T> clazz) {

			taskQueue = RedissonAdapter.redisson.getBlockingQueue(taskQueueName);
			taskQueue.clear();

			executor.setThreadFactory(new ThreadFactoryBuilder()
					.setNameFormat(taskQueueName + "Operator-Worker-%d").build());

			this.clazz = clazz;

			this.setName(this.getClass().getSimpleName() + "-" + taskQueueName);
		}

		/**
		 *
		 */
		public void run() {

			logger.info("Distributor {} started.", this.getName());

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error(e);
			}

			while(!done) {

				T t = null;
				String json = null;
				try {

					json = taskQueue.take();
					distribute(json);

				} catch (InterruptedException e) {
					logger.error(e);
				} catch (RedisTimeoutException e) {
					logger.error(e);
				} catch (Exception e) {
					logger.error("{}", json, e);
					System.exit(0);
				}
			}
		}

		public void distribute(String json) throws Exception {

			T t = JSON.fromJson(json, clazz);
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

				t.setResponse();

				if(t.getRequester_class() != null
						&& t.getRequester_class().equals(ChromeDriverRequester.class.getSimpleName()))
				{
					ChromeDriverRequester.getInstance().submit(t);
				} else {
					BasicRequester.getInstance().submit(t, CONNECT_TIMEOUT);
				}

				for(Runnable runnable : t.doneCallBacks) {
					System.err.println("AAA");
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
						try {
							t.insert();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

					return;
				}

				logger.info("{} duration: {}", t.getUrl(), t.getDuration());

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
