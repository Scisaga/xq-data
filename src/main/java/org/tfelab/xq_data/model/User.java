package org.tfelab.xq_data.model;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;
import one.rewind.db.DBName;
import one.rewind.db.DaoManager;
import one.rewind.json.JSON;
import one.rewind.json.JSONable;

import java.util.Date;
import java.util.List;

@DatabaseTable(tableName = "users")
@DBName(value = "xq_data")
public class User implements JSONable {

	@DatabaseField(dataType = DataType.STRING, width = 32, canBeNull = false, id = true)
	public String id;

	@DatabaseField(dataType = DataType.STRING, width = 128, canBeNull = false)
	public String name;

	@DatabaseField(dataType = DataType.STRING, width = 2048)
	public String description;

	@DatabaseField(dataType = DataType.STRING, width = 1)
	public String gender;

	@DatabaseField(dataType = DataType.INTEGER, width = 11)
	public int follow_count;

	@DatabaseField(dataType = DataType.INTEGER, width = 11)
	public int fans_count;

	@DatabaseField(dataType = DataType.STRING, width = 4)
	public String shiming;

	@DatabaseField(dataType = DataType.INTEGER, width = 1)
	public int article_count;

	@DatabaseField(dataType = DataType.STRING, width = 64)
	public String location;

	@DatabaseField(dataType = DataType.INTEGER, width = 11)
	public int post_count;

	@DatabaseField(dataType = DataType.INTEGER, width = 11)
	public int stock_count;

	@DatabaseField(dataType = DataType.INTEGER, width = 11)
	public int seed = 0;

	@DatabaseField(dataType = DataType.DATE, canBeNull = false)
	public transient Date insert_time = new Date();

	@DatabaseField(dataType = DataType.DATE, canBeNull = false)
	public transient Date update_time = new Date();

	public User () {}
	/**
	 *
	 * @return
	 * @throws Exception
	 */
	public boolean insert() throws Exception{

		Dao<User, String> dao = DaoManager.getDao(User.class);

		if (dao.create(this) == 1) {
			return true;
		}

		return false;
	}

	/**
	 *
	 * @return
	 * @throws Exception
	 */
	public boolean update() throws Exception{

		Dao<User, String> dao = DaoManager.getDao(User.class);

		if (dao.update(this) == 1) {
			return true;
		}

		return false;
	}

	public static User getUserById(String id) throws Exception {
		Dao<User, String> dao = DaoManager.getDao(User.class);
		return dao.queryBuilder().where().eq("id", id).queryForFirst();
	}

	@Override
	public String toJSON() {
		return JSON.toJson(this);
	}
}
