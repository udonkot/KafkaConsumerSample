package com.example.kafka.consumer;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaConsumer サンプルAP
 *
 * @author Administrator
 *
 */
public class ConsumerMain {
	/** Consumerプロパティ */
	/* ConsumerグループID */
	static final String GROUP_ID = "group.id";
	/* 接続先サーバ */
	static final String BOOTSTRAP_SERVER = "bootstrap.servers";
	/* 自動オフセットコミット */
	static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
	/* 自動オフセットコミット間隔 */
	static final String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
	/* メッセージKyeデシリアライズ */
	static final String KEY_DESERIALIZER = "key.deserializer";
	/* メッセージValueデシリアライズ */
	static final String VALUE_DESERIALIZER = "value.deserializer";

	/** AP制御用プロパティ */
	/* トピック名 */
	static final String TOPIC_NAME = "topic.name";
	/* パーティション数 */
	static final String PARTITIONS_NUM = "partitions.num";
	/* ポーリング回数 */
	static final String POLL_CNT = "poll.cnt";
	/* ポーリング待ち時間 */
	static final String WAIT_TIME = "wait.time";
	/* 接続モード */
	static final String CONNECT_TYPE = "connect.type";
	/* commitSync()実行有無 */
	static final String ENABLE_COMMIT_SYNC = "enable.commit.sync";

	/** ロガー */
	private static final Logger log = LoggerFactory.getLogger(ConsumerMain.class);

	/**
	 * main
	 *
	 * @param args
	 *            プロパティファイル
	 */
	public static void main(String[] args) {
		try {
			outputLogMsg("[[[[[ START ConsumerSample ]]]]]");

			// Consumerプロパティ取得
			Properties cosumerConfig = new Properties();
			InputStream is;
			is = new FileInputStream(new File(args[0]));
			cosumerConfig.load(is);

			// AP制御用プロパティ取得
			String topicName = cosumerConfig.getProperty(TOPIC_NAME);
			long waitTime = Long.parseLong(cosumerConfig.getProperty(WAIT_TIME));
			long pollCnt = Long.parseLong(cosumerConfig.getProperty(POLL_CNT));
			int partitionsNum = Integer.parseInt(cosumerConfig.getProperty(PARTITIONS_NUM));
			int connectType = Integer.parseInt(cosumerConfig.getProperty(CONNECT_TYPE));
			boolean commitSync = (cosumerConfig.getProperty(ENABLE_COMMIT_SYNC).equals("true")) ? true : false;

			String logMsg = null;

			// Consumer接続
			KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cosumerConfig);
			if (connectType == 0) {
				// assignで接続
				List<TopicPartition> partitions = new ArrayList<>();
				for (int i = 0; i < partitionsNum; i++) {
					logMsg = "GetPartitions topicName:" + topicName + "/parition:" + i;
					outputLogMsg(logMsg);
					partitions.add(new TopicPartition(topicName, i));
				}
				consumer.assign(partitions);
			} else {
				// subscribeで接続
				consumer.subscribe(Arrays.asList(topicName));
			}

			long i = 1;
			// while (true) {
			while (i <= pollCnt) {
				// メッセージ存在チェック
				if (connectType == 0 && !needPoll(consumer)) {
					logMsg = "count=" + i + " skipped";
					outputLogMsg(logMsg);
					i++;
					continue;
				}

				// メッセージ取得
				ConsumerRecords<String, String> records = consumer.poll(100);
				logMsg = "count=" + i + " Records=" + records.count();
				outputLogMsg(logMsg);
				if (0 < records.count()) {
					outputLogMsg("****************************************");

					for (ConsumerRecord<String, String> record : records) {
						StringBuffer sb = new StringBuffer("topicName=").append(topicName)
								.append(" partition=").append(record.partition())
								.append(" offset=").append(record.offset())
								.append(" key=").append(record.key())
								.append(" value=").append(record.value());
						outputLogMsg(sb.toString());
					}
					outputLogMsg("****************************************");
					// partitions.forEach(partition -> {
					// consumer.committed(partition);
					// });
				}
				Thread.sleep(waitTime);
				i++;
			}

			// プロパティ有効時はcommitSync()実行
			if (commitSync) {
				consumer.commitSync();
			}
			consumer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		outputLogMsg("[[[[[ END ConsumerSample ]]]]]");

	}

	/**
	 * 未受信メッセージ有無判定 トピック最終オフセット位置と最終コミット位置を比較し、異なる場合は未受信メッセージありとみなす
	 *
	 * @param consumer
	 * @return true:未受信メッセージ有／false:未受信メッセージ無
	 */
	private static boolean needPoll(Consumer<?, ?> consumer) {
		Set<TopicPartition> assignment = consumer.assignment();
		Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
		TopicPartition key = assignment.iterator().next();
		OffsetAndMetadata committed = consumer.committed(key);
		if (committed == null) {
			// コミット情報なし=未受信レコード有の可能性がある
			outputLogMsg("consumer is null.");
			return true;
		}
		outputLogMsg("endOffset.get()=" + endOffsets.get(key) + "/ committed.offset()=" + committed.offset());

		// true=オフセット位置が異なるため未受信レコード有の可能性がある
		return endOffsets.get(key) != committed.offset();
	}

	/**
	 * コンソールとロガーにメッセージ出力する
	 *
	 * @param msg
	 */
	private static void outputLogMsg(String msg) {
		System.out.println(msg);
		log.debug(msg);
	}

}
