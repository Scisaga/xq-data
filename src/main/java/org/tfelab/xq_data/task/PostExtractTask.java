package org.tfelab.xq_data.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import one.rewind.io.requester.Task;
import one.rewind.io.requester.account.Account;
import one.rewind.io.requester.account.AccountImpl;
import org.tfelab.xq_data.Crawler;
import org.tfelab.xq_data.model.Post;
import org.tfelab.xq_data.model.TaskTrace;
import org.tfelab.xq_data.model.User;
import org.tfelab.xq_data.model.UserFollow;
import org.tfelab.xq_data.proxy.ProxyManager;
import one.rewind.txt.DateFormatUtil;

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
public class PostExtractTask extends Task {

	/**
	 *
	 * @param id
	 * @param page
	 * @param account
	 * @return
	 */
	public static PostExtractTask generateTask(String id, int page, Account account) {

		if(page > 10000) return null;

		TaskTrace tt = null;
		try {
			tt = TaskTrace.getTaskTrace(id, PostExtractTask.class, String.valueOf(page));
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(tt != null) {
			return null;
		}

		String url = "https://xueqiu.com/v4/statuses/user_timeline.json?page=" + page + "&user_id=" + id;
		try {
			PostExtractTask t =  new PostExtractTask(id, page, url);

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
	 * @param page
	 * @param url
	 * @throws MalformedURLException
	 * @throws URISyntaxException
	 */
	private PostExtractTask(String id, int page, String url) throws MalformedURLException, URISyntaxException {

		super(url, UserExtractTask.genHeaders(), null,null, null);
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

		try {

			String src = new String(getResponse().getSrc(), "UTF-8");

			List<Post> posts = new LinkedList<>();

			ObjectMapper mapper = new ObjectMapper();
			JsonNode json = null;
			json = mapper.readTree(src);

			for(JsonNode subNode : json.get("statuses")) {

				Post post = new Post();
				post.id = subNode.get("id").asText();
				post.user_id = subNode.get("user_id").asText();
				post.title = subNode.get("title").asText();
				post.retweeted_id = subNode.get("retweet_status_id").asText();
				post.create_time = DateFormatUtil.parseTime(subNode.get("created_at").asText());
				post.retweet_count = subNode.get("retweet_count").asInt();
				post.reply_count = subNode.get("reply_count").asInt();
				post.fav_count = subNode.get("reply_count").asInt();
				post.like_count = subNode.get("like_count").asInt();
				post.reward_count = subNode.get("reward_count").asInt();
				post.reward_amount = subNode.get("reward_amount").asInt();
				post.description = subNode.get("description").asText();

				posts.add(post);

/*				System.err.println("============================================");
				System.err.println(post.id);
				System.err.println(subNode.get("retweeted_status").toString());
				System.err.println(subNode.get("retweeted_status") == null);*/

				if(!subNode.get("retweeted_status").toString().equals("null")) {

					subNode = subNode.get("retweeted_status");

					Post post_ = new Post();
					post_.id = subNode.get("id").asText();
					post_.user_id = subNode.get("user_id").asText();
					post_.title = subNode.get("title").asText();
					post_.retweeted_id = subNode.get("retweet_status_id").asText();
					post_.create_time = DateFormatUtil.parseTime(subNode.get("created_at").asText());
					post_.retweet_count = subNode.get("retweet_count").asInt();
					post_.reply_count = subNode.get("reply_count").asInt();
					post_.fav_count = subNode.get("reply_count").asInt();
					post_.like_count = subNode.get("like_count").asInt();
					post_.reward_count = subNode.get("reward_count").asInt();
					post_.reward_amount = subNode.get("reward_amount").asInt();
					post_.description = subNode.get("description").asText();

					posts.add(post_);
				}

			}

			if(posts.size() > 0) {

				try {
					Post.insertBatch(posts);
				} catch (Exception e) {
					e.printStackTrace();
				}

				Task t = generateTask(id, page + 1, this.getAccount());
				if(t != null) {
					t.setPriority(Priority.HIGH);
					tasks.add(t);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		TaskTrace tt = new TaskTrace(id, PostExtractTask.class, String.valueOf(page));
		try {
			tt.insert();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return tasks;
	}

	public static void main(String[] args) throws Exception {

		Account account = new AccountImpl().setProxyGroup(ProxyManager.aliyun_g);

		Crawler crawler = Crawler.getInstance();

		Task t = PostExtractTask.generateTask("7566572378", 1, account);
		crawler.addTask(t);
	}
}
