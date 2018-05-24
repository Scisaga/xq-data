package org.tfelab.xq_data.task;

import one.rewind.io.requester.Task;
import one.rewind.io.requester.account.Account;
import one.rewind.txt.DateFormatUtil;
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

	/**
	 *
	 * @param id
	 * @param account
	 * @return
	 */
	public static UserColumnExtractTask generateTask(String id, Account account) {

		String url = "https://xueqiu.com/statuses/original/show.json?user_id=" + id;

		try {
			UserColumnExtractTask t = new UserColumnExtractTask(id, url);
			t.setAccount(account);
			return t;
		} catch (MalformedURLException | URISyntaxException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 *
	 * @param id
	 * @param url
	 * @throws MalformedURLException
	 * @throws URISyntaxException
	 */
	private UserColumnExtractTask(String id, String url) throws MalformedURLException, URISyntaxException {

		super(url);
		this.setParam("id", id);

		this.addDoneCallback(() -> {

			String src = getResponse().getText();

			User user = null;
			try {
				user = User.getUserById(this.getParamString("id"));
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}

			if(user == null) return;

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
						return;
					}
				}

			}

			try {
				user.update();
			} catch (Exception e) {
				e.printStackTrace();
			}

		});
	}
}