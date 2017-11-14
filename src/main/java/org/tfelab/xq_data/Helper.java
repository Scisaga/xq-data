package org.tfelab.xq_data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.tfelab.db.PooledDataSource;
import org.tfelab.db.Refacter;
import org.tfelab.xq_data.model.TaskTrace;

public class Helper {

	private static final Logger logger = LogManager.getLogger(Helper.class.getName());

	/**
	 * 谨慎使用
	 */
	public static void initDB() {

		logger.info("Init db tables...");

		try {

			Refacter.dropTables("org.tfelab.xq_data.model");
			Refacter.createTables("org.tfelab.xq_data.model");

			logger.info("Create db tables done.");

		} catch (Exception e) {
			logger.error("Error create tables.", e);
		}
	}


	public static void main(String[] args) throws Exception {
		//initDB();
		Refacter.createTable(TaskTrace.class);
	}
}
