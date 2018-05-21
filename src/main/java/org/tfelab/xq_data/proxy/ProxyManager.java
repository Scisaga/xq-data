package org.tfelab.xq_data.proxy;

import com.j256.ormlite.dao.Dao;
import one.rewind.db.DaoManager;
import one.rewind.io.requester.proxy.Proxy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import one.rewind.db.RedissonAdapter;
import one.rewind.io.requester.proxy.Proxy;
import org.tfelab.xq_data.model.ProxyImpl;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ProxyManager {

	public static final Logger logger = LogManager.getLogger(ProxyManager.class.getName());

	protected static ProxyManager instance;

	public static String aliyun_g = "aliyun";

	public static String abuyun_g = "abuyun";

	public static ProxyManager getInstance() {

		if (instance == null) {
			synchronized (ProxyManager.class) {
				if (instance == null) {
					instance = new ProxyManager();
				}
			}
		}

		return instance;
	}


	private ConcurrentHashMap<String, RAtomicLong> lastRequestTime = new ConcurrentHashMap<>();

	private ProxyManager() {}

	/**
	 *
	 * @param proxy
	 */
	public void waits(Proxy proxy) {

		RLock lock = RedissonAdapter.redisson.getLock(proxy.getInfo());
		lock.lock(10, TimeUnit.SECONDS);

		if(lastRequestTime.get(proxy.getId()) == null) {
			lastRequestTime.put(proxy.getId(), RedissonAdapter.redisson.getAtomicLong("proxy-" + proxy.getId() + "-last-request-time"));
			lastRequestTime.get(proxy.getId()).set(System.currentTimeMillis());
		}

		long wait_time = lastRequestTime.get(proxy.getId()).get() + (long) Math.ceil(1000D / (double) proxy
		.getRequestPerSecondLimit()) - System.currentTimeMillis();

		if(wait_time > 0) {
			logger.info("Wait {} ms.", wait_time);
			try {
				Thread.sleep(wait_time);
			} catch (InterruptedException e) {
				logger.error(e);
			}
		}

		lastRequestTime.get(proxy.getId()).set(System.currentTimeMillis());
		lock.unlock();
	}

	/**
	 *
	 * @return
	 * @throws Exception
	 */
	public List<ProxyImpl> getAll() throws Exception {
		Dao dao = DaoManager.getDao(ProxyImpl.class);
		return dao.queryForEq("source", ProxyImpl.Source.ALIYUN_HOST);
	}

	/**
	 *
	 * @param id
	 * @return
	 */
	public Proxy getProxyById(String id) throws Exception {

		Dao<ProxyImpl, String> dao = DaoManager.getDao(ProxyImpl.class);
		return dao.queryForId(id);
	}

	/**
	 *
	 * @param group
	 * @return
	 * @throws Exception
	 */
	public Proxy getProxyByGroup(String group) throws Exception {

		Dao<ProxyImpl, String> dao = DaoManager.getDao(ProxyImpl.class);

		List<ProxyImpl> proxies = dao.queryForEq("group", group);

		if(proxies.size() > 0) {
			return proxies.get(0);
		} else {
			return null;
		}
	}
}
