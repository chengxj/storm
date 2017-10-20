package com.smartlog.spout;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class RandomSentenceSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	Random _rand;

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	public void nextTuple() {
		Utils.sleep(100);
		String[] sentences = new String[]{"abc", "def", "ghi"};
		String sentence = sentences[_rand.nextInt(sentences.length)];
		_collector.emit(new Values(sentence), new Object());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	
	public void ack(Object id) {
		System.out.println("receieve ack for id" + id);
	}
	
	public void fail(Object id) {
		
	}

}
