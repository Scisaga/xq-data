package org.tfelab.xq_data.model;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;
import one.rewind.db.DBName;
import one.rewind.db.DaoManager;
import one.rewind.io.requester.BasicRequester;
import one.rewind.io.requester.Task;
import one.rewind.io.requester.proxy.Proxy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;

@DBName(value = "xueqiu")
@DatabaseTable(tableName = "proxies")
public class ProxyImpl extends Proxy {

	private static final Logger logger = LogManager.getLogger(ProxyImpl.class.getName());

	public enum Source {
		ALIYUN_HOST,
		OTHERS
	}

	@DatabaseField(dataType = DataType.ENUM_STRING, width = 32)
	public Source source = Source.OTHERS;

	public ProxyImpl() {}

	/**
	 *
	 * @param group
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 * @param location
	 */
	public ProxyImpl(String group, String host, int port, String username, String password, String location, int request_per_second_limit) {
		super(group, host, port, username, password, location, request_per_second_limit);
	}

	public void setAliyunHost() {
		source = Source.ALIYUN_HOST;
	}

	/**
	 *
	 * @return
	 * @throws Exception
	 */
	@Override
	public boolean success() throws Exception {
		return true;
	}

	/**
	 * TODO
	 * 由于只采集一个目标网站
	 * 简化出错处理：当目标网站封禁IP后，回调failed()
	 *
	 */
	@Override
	public boolean failed() throws Exception {

		this.status = Status.INVALID;
		this.enable = false;
		this.update();
		return true;
	}

	private Runnable failedCallback;

	public void setFailedCallback(Runnable callback) {
		this.failedCallback = callback;
	}

	public float testSpeed(String url) throws MalformedURLException, URISyntaxException {

		float speedAvg = 0;

		for(int i=0; i<40; i++) {

			Task t = new Task(url);
			BasicRequester.getInstance().submit(t);

			speedAvg += (double) t.getResponse().getSrc().length / ( (double) t.getDuration() );

		}

		return speedAvg / 20;
	}

	/**
	 * TODO
	 */
	@Override
	public boolean timeout() throws Exception {
		return true;
	}
}
