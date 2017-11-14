package org.tfelab.xq_data.model;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.tfelab.db.DBName;
import org.tfelab.db.OrmLiteDaoManager;
import org.tfelab.json.JSON;
import org.tfelab.json.JSONable;

import java.util.Date;

@DatabaseTable(tableName = "task_traces")
@DBName(value = "xq_data")
public class TaskTrace implements JSONable{

	private static final Logger logger = LogManager.getLogger(TaskTrace.class.getName());

	@DatabaseField(generatedId = true)
	private transient Long id;

	@DatabaseField(dataType = DataType.STRING, width = 32, canBeNull = false, uniqueCombo = true)
	public String code;

	@DatabaseField(dataType = DataType.STRING, width = 64, canBeNull = false, uniqueCombo = true)
	public String type;

	@DatabaseField(dataType = DataType.STRING, width = 11, canBeNull = false, uniqueCombo = true)
	public String page;

	@DatabaseField(dataType = DataType.DATE, canBeNull = false)
	public transient Date insert_time = new Date();

	public TaskTrace() {};

	public TaskTrace(String code, Class clazz, String page) {
		this.code = code;
		this.type = clazz.getSimpleName();
		this.page = page;
	}

	/**
	 *
	 * @return
	 * @throws Exception
	 */
	public boolean insert() throws Exception{

		Dao<TaskTrace, String> dao = OrmLiteDaoManager.getDao(TaskTrace.class);

		if (dao.create(this) == 1) {
			return true;
		}

		return false;
	}

	/**
	 *
	 * @param code
	 * @param clazz
	 * @param page
	 * @return
	 * @throws Exception
	 */
	public static TaskTrace getTaskTrace(String code, Class clazz, String page) throws Exception {
		Dao<TaskTrace, String> dao = OrmLiteDaoManager.getDao(TaskTrace.class);
		return dao.queryBuilder().where().eq("code", code)
				.and().eq("type", clazz.getSimpleName())
				.and().eq("page", page).queryForFirst();

	}

	@Override
	public String toJSON() {
		return JSON.toJson(this);
	}
}