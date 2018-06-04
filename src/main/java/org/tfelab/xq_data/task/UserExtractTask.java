package org.tfelab.xq_data.task;

import com.google.common.collect.ImmutableList;
import org.redisson.misc.Hash;
import one.rewind.io.requester.Task;
import one.rewind.io.requester.account.Account;
import one.rewind.io.requester.account.AccountImpl;
import one.rewind.txt.DateFormatUtil;
import org.tfelab.xq_data.Crawler;
import org.tfelab.xq_data.model.ProxyImpl;
import org.tfelab.xq_data.model.User;
import org.tfelab.xq_data.proxy.ProxyManager;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserExtractTask extends Task {

	/**
	 *
	 * @return
	 */
	public static HashMap<String, String> genHeaders() {

		HashMap<String, String> headers = new HashMap<String, String>();
		headers.put("Host", "xueqiu.com");
		headers.put("Connection", "keep-alive");
		headers.put("Cache-Control", "no-cache");
		headers.put("Pragma", "no-cache");
		headers.put("Upgrade-Insecure-Requests", "1");
		headers.put("mobile", "Genymotion Google Ne");
		headers.put("Accept-Language", "zh-CN,zh;q=0.8");
		headers.put("User-Agent", " Musical.ly/2017062601 (Android; Genymotion Google Nexus 5X - 6.0.0 - API 23 - 1080x1920 6.0;rv:23)");
		headers.put("Cookie", "aliyungf_tc=AQAAAMiGVWzvLg4AjKj2ciS1Je1s3EyB; s=fj12f8sm1f; xq_a_token=6708d101a456578c98ea1779ae898687fe465bcb; xq_r_token=0cbb786896425c8f2a853545bade9309fbc75601; u=221510583256048; device_id=a6e44a23cc1eaa8f39950d3af5bd687e; Hm_lvt_1db88642e346389874251b5a1eded6e3=1510583257; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1510599858");
		headers.put("Accept-Encoding", "gzip");

		return headers;
	}

	/**
	 *
	 * @param id
	 * @param seed
	 * @param account
	 * @return
	 */
	public static UserExtractTask generateTask(String id, int seed, Account account) {

		String url = "https://xueqiu.com/u/" + id;

		try {
			UserExtractTask t = new UserExtractTask(id, seed, url);
			t.setAccount(account);
			return t;
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 *
	 * @param id
	 * @param seed
	 * @param url
	 * @throws MalformedURLException
	 * @throws URISyntaxException
	 */
	private UserExtractTask(String id, int seed, String url) throws MalformedURLException, URISyntaxException {

		super(url);
		this.setParam("id", id);
		this.setParam("seed", seed);

		this.addDoneCallback(() -> {

			String src = getResponse().getText();

			try {
				if(User.getUserById(this.getParamString("id")) != null) {
					return;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			User user = new User();
			user.id = this.getParamString("id");

			Map<String, String> patterns = new HashMap<>();
			patterns.put("name", "(?s)<h2>(?<T>.+?)</h2>");
			patterns.put("description", "</div><p>(?<T>.+?)</p><profiles-pannel>");
			patterns.put("gender", "class=\"profiles__icon--(?<T>.{1})");
			patterns.put("follow_count", "<strong>(?<T>\\d+?)</strong> 关注");
			patterns.put("fans_count","<strong>(?<T>\\d+?)</strong> 粉丝");
			patterns.put("shiming", "(?<T>实名认证)");
			patterns.put("location", "<li> <i class=\"iconfont\">&#xe63f;</i>(?<T>.+?)</li>");
			patterns.put("post_count", "帖子<span>(?<T>\\d+?)</span>");
			patterns.put("stock_count", "股票<span>(?<T>\\d+?)</span>");

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
				user.insert();
			} catch (Exception e) {
				e.printStackTrace();
			}

			/*List<Task> tasks = new LinkedList<>();

			Task t = UserColumnExtractTask.generateTask(user.id, this.getAccount());
			if(t != null) {
				t.setPriority(Priority.HIGH);
				tasks.add(t);
			}

			Task t_ = PostExtractTask.generateTask(user.id, 1, this.getAccount());
			if(t_ != null) {
				t_.setPriority(Priority.HIGH);
				tasks.add(t_);
			}

			// 如果是种子用户
			if(getParamInt("seed") == 1) {
				tasks = ImmutableList.of(UserFollowExtractTask.generateTask(user.id, 1, this.getAccount()));
			}

			Crawler.getInstance().addTask(tasks);*/
		});
	}

	public static void main(String[] args) throws Exception {

		ProxyImpl proxy = new ProxyImpl(
				ProxyManager.aliyun_g, "10.0.0.51", 49999, "", "", "SG", 40);

		proxy.insert();

		Account accountWrapper = new AccountImpl(null, null, null)
				.setProxyGroup(ProxyManager.aliyun_g);

		Crawler crawler = Crawler.getInstance();

		Task t = generateTask("7500022239", 1, accountWrapper);
		crawler.addTask(t);
	}
}