import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
//import edu.stanford.nlp.pipeline.CoreNLPProtos.Sentiment;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import java.util.Properties;

import org.apache.log4j.Logger;
public class NLP {
	private static final Logger LOG = Logger.getLogger(NLP.class);
	private StanfordCoreNLP pipeline;
	private StanfordCoreNLP tokenizer;
	public NLP() {
		Properties pipelineProps = new Properties();
		Properties tokenizerProps = null;
		pipelineProps.setProperty("annotators", "parse, sentiment");
		pipelineProps.setProperty("enforceRequirements", "false");
		tokenizerProps = new Properties();
		tokenizerProps.setProperty("annotators", "tokenize, ssplit");
		
		tokenizer = new StanfordCoreNLP(tokenizerProps);
	    pipeline = new StanfordCoreNLP(pipelineProps);
    }
	public int findSentiment(String tweet) {

        int mainSentiment = 0;
        int sentimentInt=0;
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = tokenizer.process(tweet);
            pipeline.annotate(annotation);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
//            	String sentiment=sentence.get(SentimentCoreAnnotations.SentimentClass.class);
            	Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                //if (partText.length() > longest) {
//                if(sentiment.equals("Positive")){
//                	sentimentInt=1;
//                }else if(sentiment.equals("Negative")){
//                	sentimentInt=-1;
//                }else if(sentiment.equals("Very negative")){
//                	sentimentInt=-2;
//                }else if(sentiment.equals("Very positive")){
//                	sentimentInt=2;
//                }else if(sentiment.equals("Neutral")){
//                	sentimentInt=0;
//                }
//                    mainSentiment += sentimentInt;
                mainSentiment+=sentiment;
                    //LOG.info("Sentiment of sentence is: "+sentimentInt+" : "+partText);
                    longest = partText.length();
                //}

            }
        }
        return mainSentiment;
    }
}
