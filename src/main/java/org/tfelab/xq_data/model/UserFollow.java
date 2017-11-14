package org.tfelab.xq_data.model;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.tfelab.db.DBName;
import org.tfelab.db.OrmLiteDaoManager;
import org.tfelab.db.PooledDataSource;
import org.tfelab.json.JSON;
import org.tfelab.json.JSONable;
import org.tfelab.txt.DateFormatUtil;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Date;
import java.util.List;

@DatabaseTable(tableName = "user_follows")
@DBName(value = "xq_data")
public class UserFollow implements JSONable {

	private static final Logger logger = LogManager.getLogger(UserFollow.class.getName());

	@DatabaseField(dataType = DataType.INTEGER, generatedId = true)
	public int id;

	@DatabaseField(dataType = DataType.STRING, width = 32, canBeNull = false, uniqueCombo = true)
	public String user_id;

	@DatabaseField(dataType = DataType.STRING, width = 32, canBeNull = false, uniqueCombo = true)
	public String follow_id;

	@DatabaseField(dataType = DataType.DATE, canBeNull = false)
	public transient Date insert_time = new Date();

	public UserFollow () {}
	/**
	 *
	 * @return
	 * @throws Exception
	 */
	public boolean insert() throws Exception{

		Dao<UserFollow, String> dao = OrmLiteDaoManager.getDao(UserFollow.class);

		if (dao.create(this) == 1) {
			return true;
		}

		return false;
	}

	/**
	 *
	 * @param items
	 * @throws Exception
	 */
	public static void insertBatch(List<org.tfelab.xq_data.model.UserFollow> items) throws Exception {

		Connection conn = PooledDataSource.getDataSource("xq_data").getConnection();

		String sql = "INSERT IGNORE INTO user_follows (`user_id`, `follow_id`, `insert_time`) values ";

		int count = 0;
		for(UserFollow item : items) {

				sql += "('"+item.user_id+"', "+item.follow_id+", '"+ DateFormatUtil.dff.print(item.insert_time.getTime())+"'), ";
				count ++;

		}

		sql = sql.substring(0, sql.length() - 2);

		//System.err.println(sql);
		if(count > 0) {
			Statement stmt = conn.createStatement();

			try {
				stmt.execute(sql);
			} catch (Exception e) {
				logger.error(sql, e);
			}

			stmt.close();
		}
		conn.close();
	}

	@Override
	public String toJSON() {
		return JSON.toJson(this);
	}
}

