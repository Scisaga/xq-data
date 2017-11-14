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

@DatabaseTable(tableName = "posts")
@DBName(value = "xq_data")
public class Post implements JSONable {

	private static final Logger logger = LogManager.getLogger(Post.class.getName());

	@DatabaseField(dataType = DataType.STRING, width = 32, canBeNull = false, id = true)
	public String id;

	@DatabaseField(dataType = DataType.STRING, width = 32, canBeNull = false)
	public String user_id;

	@DatabaseField(dataType = DataType.STRING, width = 128)
	public String title;

	@DatabaseField(dataType = DataType.STRING, width = 32)
	public String retweeted_id;

	@DatabaseField(dataType = DataType.DATE)
	public Date create_time;

	@DatabaseField(dataType = DataType.INTEGER, width = 11)
	public int retweet_count;

	@DatabaseField(dataType = DataType.INTEGER, width = 11)
	public int reply_count;

	@DatabaseField(dataType = DataType.INTEGER, width = 11)
	public int fav_count;

	@DatabaseField(dataType = DataType.INTEGER, width = 11)
	public int like_count;

	@DatabaseField(dataType = DataType.INTEGER, width = 11)
	public int reward_count;

	@DatabaseField(dataType = DataType.INTEGER, width = 11)
	public int reward_amount;

	@DatabaseField(dataType = DataType.STRING, width = 4096)
	public String description;

	@DatabaseField(dataType = DataType.DATE, canBeNull = false)
	public transient Date insert_time = new Date();

	public Post () {}
	/**
	 *
	 * @return
	 * @throws Exception
	 */
	public boolean insert() throws Exception{

		Dao<Post, String> dao = OrmLiteDaoManager.getDao(Post.class);

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
	public static void insertBatch(List<Post> items) throws Exception {

		Connection conn = PooledDataSource.getDataSource("xq_data").getConnection();

		String sql = "INSERT IGNORE INTO posts (`id`, `user_id`, `title`, `retweeted_id`, `create_time`, `retweet_count`, `reply_count`, `fav_count`, `like_count`, `reward_count`, `reward_amount`, `description`, `insert_time`) values ";

		int count = 0;
		for(Post item : items) {

			sql += "('"+item.id+"', '"
					+item.user_id+"', '"
					+item.title+"',  '"
					+item.retweeted_id+"',  '"
					+DateFormatUtil.dff.print(item.create_time.getTime())+"', "
					+item.retweet_count+", "
					+item.reply_count+", "
					+item.fav_count+", "
					+item.like_count+", "
					+item.reward_count+", "
					+item.reward_amount+", '"
					+item.description.replaceAll("'","\\'")+"', '" //TODO 需要确认是否可以消除英文单引号无法入库的问题
					+ DateFormatUtil.dff.print(item.insert_time.getTime())+"'), ";
			count ++;
		}

		sql = sql.substring(0, sql.length() - 2);

//		System.err.println(sql);
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

