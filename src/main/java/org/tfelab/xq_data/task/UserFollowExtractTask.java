package org.tfelab.xq_data.task;

import com.google.common.collect.ImmutableList;
import org.tfelab.io.requester.Task;
import org.tfelab.io.requester.account.AccountWrapper;
import org.tfelab.io.requester.account.AccountWrapperImpl;
import org.tfelab.xq_data.Crawler;
import org.tfelab.xq_data.model.TaskTrace;
import org.tfelab.xq_data.model.User;
import org.tfelab.xq_data.model.UserFollow;
import org.tfelab.xq_data.proxy.ProxyManager;
import org.tfelab.txt.DateFormatUtil;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class UserFollowExtractTask extends Task {

	/**
	 *
	 * @param id
	 * @param page
	 * @param accountWrapper
	 * @return
	 */
	public static UserFollowExtractTask generateTask(String id, int page, AccountWrapper accountWrapper) {

		if(page > 10000) return null;

		TaskTrace tt = null;
		try {
			tt = TaskTrace.getTaskTrace(id, UserFollowExtractTask.class, String.valueOf(page));
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(tt != null) {
			return null;
		}

		String url = "https://xueqiu.com/friendships/groups/members.json?uid=" + id + "&page=" + page + "&gid=0";
		try {
			UserFollowExtractTask t =  new UserFollowExtractTask(id, page, url);
			t.setAccount(accountWrapper);
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
	 * @param page
	 * @param url
	 * @throws MalformedURLException
	 * @throws URISyntaxException
	 */
	private UserFollowExtractTask(String id, int page, String url) throws MalformedURLException, URISyntaxException {

		super(url);

		this.setParam("id", id);
		this.setParam("page", page);
	}

	/**
	 * 自定义后处理方法
	 * 解析并保存数据
	 * 同时生成翻页任务
	 */
	public List<Task> postProc() {

		String id = getParamString("id");
		int page = getParamInt("page");

		List<Task> tasks = new LinkedList<>();

		List<UserFollow> rs = new LinkedList<>();

		try {

			String src = getResponse().getText();

			Pattern recordPattern = Pattern.compile("\"id\":(?<id>\\d+),");
			Matcher recordMatcher = recordPattern.matcher(src);
			while (recordMatcher.find()) {

				UserFollow uf = new UserFollow();
				uf.user_id = id;
				uf.follow_id = recordMatcher.group("id");
				rs.add(uf);

				if(User.getUserById(uf.follow_id) == null) {
					tasks.add(UserExtractTask.generateTask(uf.follow_id, 0, this.getAccountWrapper()));
				}

			}

			if(rs.size() > 0) {

				try {
					UserFollow.insertBatch(rs);
				} catch (Exception e) {
					e.printStackTrace();
				}

				Task t = generateTask(id, page + 1, this.getAccountWrapper());
				if(t != null) {
					t.setPrior();
					tasks.add(t);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		TaskTrace tt = new TaskTrace(id, UserFollowExtractTask.class, String.valueOf(page));
		try {
			tt.insert();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return tasks;
	}

	public static void main(String[] args) throws Exception {

		AccountWrapper accountWrapper = new AccountWrapperImpl().setProxyGroup(ProxyManager.aliyun_g);

		Crawler crawler = Crawler.getInstance();


	}
}
