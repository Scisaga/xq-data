package org.tfelab.xq_data.task;

import org.tfelab.io.requester.Task;
import org.tfelab.io.requester.account.AccountWrapper;
import org.tfelab.txt.DateFormatUtil;
import org.tfelab.xq_data.model.User;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserColumnExtractTask extends Task {

	public static UserColumnExtractTask generateTask(String id, AccountWrapper aw) {

		String url = "https://xueqiu.com/statuses/original/show.json?user_id=" + id;

		try {
			UserColumnExtractTask t = new UserColumnExtractTask(id, url);
			t.setAccount(aw);
			return t;
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		return null;
	}

	private UserColumnExtractTask(String id, String url) throws MalformedURLException, URISyntaxException {
		super(url);
		this.setParam("id", id);
	}

	public List<Task> postProc() {

		String src = getResponse().getText();

		User user = null;
		try {
			user = User.getUserById(this.getParamString("id"));
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		if(user == null) return null;

		Map<String, String> patterns = new HashMap<>();
		patterns.put("article_count", "\"total\":(?<T>.+?),");

		for(String key : patterns.keySet()) {

			Pattern p = Pattern.compile(patterns.get(key));
			Matcher m = p.matcher(src);

			if(m.find()) {

				try {
					Field f = user.getClass().getDeclaredField(key);

					if(f.getType().equals(Date.class)) {
						f.set(user, DateFormatUtil.parseTime(m.group("T")));
					}
					else if (f.getType().equals(int.class)) {
						f.set(user, Integer.valueOf(m.group("T")));
					}
					else if (f.getType().equals(float.class)) {
						f.set(user, Float.valueOf(m.group("T")));
					}
					else if (f.getType().equals(double.class)) {
						f.set(user, Double.valueOf(m.group("T")));
					}
					else {
						f.set(user, m.group("T"));
					}

				} catch (Exception e) {
					e.printStackTrace();
					return null;
				}
			}

		}

		try {
			user.update();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;

	}
}