package TwitterFeed;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.vdurmont.emoji.EmojiManager;
import com.vdurmont.emoji.EmojiParser;

import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LiveTwitterFeed {

  public static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException, UnsupportedEncodingException {
    // Create an appropriately sized blocking queue
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

    // Define our endpoint: By default, delimited=length is set (we need this for our processor)
    // and stall warnings are on.
    StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
    endpoint.stallWarnings(false);

    Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

    // Create a new BasicClient. By default gzip is enabled.
    BasicClient client = new ClientBuilder()
            .name("JackHenryTwitter")
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(queue))
            .build();

    // Establish a connection
    client.connect();

    int totalNumberOfTweetsReceived = 0;
    int totalNumberOfTweetsReceivedInTenMinutes = 0;
    int countOfTweetsWithURL = 0;
    int countOfTweetsWithImage = 0;
    int countOfTweetsWithEmojis = 0;
    HashMap<String, Integer> topHashtags = new HashMap<String, Integer>();
    HashMap<String, Integer> topURLs = new HashMap<String, Integer>();
    HashMap<String, Integer> topEmojis = new HashMap<String, Integer>();
    long startTime = System.currentTimeMillis();
    while (!client.isDone()) {
    	String msg = queue.poll(5, TimeUnit.SECONDS);
//    	System.out.println(msg);
		if(msg.contains("created_at")){
			totalNumberOfTweetsReceived++;
			totalNumberOfTweetsReceivedInTenMinutes++;
			String[] tweetArray = msg.split("\"text\":\"");
			tweetArray = tweetArray[1].split("\"");
			String tweet = tweetArray[0];
			if(msg.contains("{\"url\":\"")){
				countOfTweetsWithURL++;
				Pattern pattern = Pattern.compile("\"urls\":\\[\\{\"url\":\"(.*?)\"");
				Matcher matcher = pattern.matcher(msg);
				if (matcher.find()){
					String url = matcher.group(1).replaceAll("\\/*", "");
					int count = topURLs.containsKey(url) ? topURLs.get(url) : 0;
					topURLs.put(url, count + 1);
				}
			}
			
			//find tweets with twitter images
			if(msg.contains("pic.twitter.com")){
				countOfTweetsWithImage++;
			}
			if(msg.contains("instagram.com\\/p\\/")){
				countOfTweetsWithImage++;
			}

			//add to topHashtags hashmap
			if(tweet.contains("#")){				
				Pattern pattern = Pattern.compile("#(.*?) ");
				Matcher matcher = pattern.matcher(tweet);
				while (matcher.find()){
					String hashtag = matcher.group(1);						
					int count = topHashtags.containsKey(hashtag) ? topHashtags.get(hashtag) : 0;
					topHashtags.put(hashtag, count + 1);
				}
			}
			//emojis
			Pattern pattern = Pattern.compile("(\\\\u[\\d|a-zA-Z]{4}\\\\u[\\d|a-zA-Z]{4})");	//unicode
			Matcher matcher = pattern.matcher(tweet);
			Boolean alreadyCountedEmoji = false;
			while (matcher.find()){
				String emoji = matcher.group(1);	//if tweet contains unicode
				emoji = emoji.replace("\\","");		//format the unicode to get hex value
				String[] arr = emoji.split("u");
				String text = "";
				for(int i = 1; i < arr.length; i++){
				    int hexVal = Integer.parseInt(arr[i], 16);
				    text += (char)hexVal;
				}

				String emojiString = EmojiParser.parseToUnicode(text);	//convert hex value back to unicode (couldn't do this properly with original emoji string because of the literal \)
				if(EmojiManager.isEmoji(emojiString)){	//if the unicode values represent an emoji, add them to the hashmap
					int count = topEmojis.containsKey(emojiString) ? topEmojis.get(emojiString) : 0;
					topEmojis.put(emojiString, count + 1);
					if(alreadyCountedEmoji == false){	//only count a tweet with emoji's once, even if it has multiple emojis in it.
						countOfTweetsWithEmojis++;
						alreadyCountedEmoji = true;					
					}
				}
			}			
		}
	    long endTime = System.currentTimeMillis();
	    if(endTime - startTime > 60000/*600000*/){
	    	System.out.printf("Total number of tweets deleted and created: %d\n", client.getStatsTracker().getNumMessages());
	    	getStats(totalNumberOfTweetsReceived, totalNumberOfTweetsReceivedInTenMinutes, countOfTweetsWithURL, countOfTweetsWithImage,
	    			topHashtags, topURLs, countOfTweetsWithEmojis, topEmojis);
	        startTime = System.currentTimeMillis();
	    }
    }
  }

  private static void getStats(Integer totalNumberOfTweetsReceived, Integer totalNumberOfTweetsReceivedInTenMinutes, Integer countOfTweetsWithURL,
		  Integer countOfTweetsWithImage, HashMap<String, Integer> topHashtags, HashMap<String, Integer> topURLs, Integer countOfTweetsWithEmojis,
		  HashMap<String, Integer> topEmojis) {
	    System.out.printf("%d tweets received.\n", totalNumberOfTweetsReceived);
	    System.out.printf("Average tweets/second: %d.\n", totalNumberOfTweetsReceived/600);
	    System.out.printf("Average tweets/minute: %d.\n", totalNumberOfTweetsReceived/10);
	    System.out.printf("Average tweets/hour: %d.\n", totalNumberOfTweetsReceived*6);
	    System.out.printf("Tweets that contain a URL: %.0f%%\n", (float) countOfTweetsWithURL/(float) totalNumberOfTweetsReceived*100);
	    System.out.printf("Tweets that contain an image: %.0f%%\n", (float) countOfTweetsWithImage/(float) totalNumberOfTweetsReceived*100);

	    Map.Entry<String,Integer> numberOneHashtag = new AbstractMap.SimpleEntry<String, Integer>("", 0);
	    Map.Entry<String,Integer> numberTwoHashtag = new AbstractMap.SimpleEntry<String, Integer>("", 0);
	    Map.Entry<String,Integer> numberThreeHashtag = new AbstractMap.SimpleEntry<String, Integer>("", 0);
	    for (Entry<String, Integer> entry : topHashtags.entrySet()){
	    	Integer entryValue = entry.getValue();
	    	if(entryValue > numberOneHashtag.getValue()){
	    		numberOneHashtag = entry;
	    	} else if(entryValue > numberTwoHashtag.getValue()){
	    		numberTwoHashtag = entry;
	    	} else if(entryValue > numberThreeHashtag.getValue()){
	    		numberThreeHashtag = entry;
	    	}
	    }
	    System.out.println("Top hastags are: 1. " + numberOneHashtag.getKey() + " 2. " + numberTwoHashtag.getKey() + " 3. " + numberThreeHashtag.getKey());
	    

	    Map.Entry<String,Integer> numberOneURL = new AbstractMap.SimpleEntry<String, Integer>("", 0);
	    Map.Entry<String,Integer> numberTwoURL = new AbstractMap.SimpleEntry<String, Integer>("", 0);
	    Map.Entry<String,Integer> numberThreeURL = new AbstractMap.SimpleEntry<String, Integer>("", 0);
	    for (Entry<String, Integer> entry : topURLs.entrySet()){
	    	Integer entryValue = entry.getValue();
	    	if(entryValue > numberOneURL.getValue()){
	    		numberOneURL = entry;
	    	} else if(entryValue > numberTwoURL.getValue()){
	    		numberTwoURL = entry;
	    	} else if(entryValue > numberThreeURL.getValue()){
	    		numberThreeURL = entry;
	    	}
	    }
	    System.out.println("Top URLs are: 1. " + numberOneURL.getKey() + " 2. " + numberTwoURL.getKey() + " 3. " + numberThreeURL.getKey());
	    System.out.printf("Tweets that contain emojis: %.0f%%\n", (float) countOfTweetsWithEmojis/(float) totalNumberOfTweetsReceived*100);
	    
	    Map.Entry<String,Integer> numberOneEmoji = new AbstractMap.SimpleEntry<String, Integer>("", 0);
	    Map.Entry<String,Integer> numberTwoEmoji = new AbstractMap.SimpleEntry<String, Integer>("", 0);
	    Map.Entry<String,Integer> numberThreeEmoji = new AbstractMap.SimpleEntry<String, Integer>("", 0);
	    for (Entry<String, Integer> entry : topEmojis.entrySet()){
	    	Integer entryValue = entry.getValue();
	    	if(entryValue > numberOneEmoji.getValue()){
	    		numberOneEmoji = entry;
	    	} else if(entryValue > numberTwoEmoji.getValue()){
	    		numberTwoEmoji = entry;
	    	} else if(entryValue > numberThreeEmoji.getValue()){
	    		numberThreeEmoji = entry;
	    	}
	    }

	    System.out.println("Top emojis are 1. " + EmojiParser.parseToAliases(numberOneEmoji.getKey()) + " 2. " + EmojiParser.parseToAliases(numberTwoEmoji.getKey())
	    		+ " 3. " + EmojiParser.parseToAliases(numberThreeEmoji.getKey()));
	    totalNumberOfTweetsReceivedInTenMinutes = 0;
  }

public static void main(String[] args) throws UnsupportedEncodingException {
    try {
      LiveTwitterFeed.run(args[0], args[1], args[2], args[3]);
    } catch (InterruptedException e) {
      System.out.println(e);
    }
  }
}
