# Import Modules ------------------------------------------

from gensim.parsing import preprocess_string
from gensim.parsing import strip_punctuation
from gensim.parsing import strip_tags
from gensim.parsing import strip_short
from gensim.parsing import strip_multiple_whitespaces
from gensim.parsing import strip_non_alphanum
from gensim.parsing import strip_numeric
from gensim.parsing import remove_stopwords
from gensim.parsing import stem_text
from joblib import load
import praw 
import re
import sqlite3
from textblob import TextBlob

if __name__ == "__main__":

    # Initialise Reddit Instance ------------------------------

    reddit = praw.Reddit(
        "nrlStream", 
        user_agent=""
        )

    # Connect to Database -------------------------------------

    con = sqlite3.connect("data/database/analysed_nrl_comments.db")

    # Text Cleaning Functions ---------------------------------

    CUSTOM_FILTERS = [
        lambda x: x.lower(), 
        strip_punctuation,
        strip_numeric,
        strip_non_alphanum,
        strip_multiple_whitespaces,
        strip_tags, 
        remove_stopwords,
        lambda x: strip_short(x, minsize=5),
        stem_text]

    # Read Topic Model ----------------------------------------

    topic_model = load("models/nrl_reddit_topic_model.joblib")  # Load LDA model

    # Stream Comments from NRL Sub-Reddit ---------------------

    for comment in reddit.subreddit("nrl").stream.comments(skip_existing=False):
        if re.search("[A-Za-z]+ vs [A-Za-z]+", comment.submission.title):

            text_sentiment = TextBlob(comment.body.lower()).sentiment 
            pre_process_comment = preprocess_string(comment.body, CUSTOM_FILTERS)  

            lda_comment = {
                "thread_title": comment.submission.title,
                "created_utc": comment.created_utc,  
                "comment_topid": int(topic_model.choose_best_label(pre_process_comment)[0]),
                "comment_sentiment_polarity": text_sentiment.polarity,  
                "comment_sentiment_subjectivity": text_sentiment.subjectivity
            }

            con.execute(
                """
                    INSERT INTO NRLCOMMENTSTREAM (
                        thread_title, 
                        created_utc, 
                        comment_topid, 
                        comment_sentiment_polarity, 
                        comment_sentiment_subjectivity)
                    VALUES (
                        :thread_title, 
                        :created_utc, 
                        :comment_topid, 
                        :comment_sentiment_polarity, 
                        :comment_sentiment_subjectivity)
                    """,
                lda_comment)  
            
            con.commit()





            
