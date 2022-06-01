# Build LDA model based on extracted Rugby Union Threads Comments

# Libraries ---------------------------
from gensim.corpora import Dictionary
from gensim.models import LdaModel
from joblib import dump
import pandas as pd
import sqlite3
from gsdmm import MovieGroupProcess
from gensim.parsing import preprocess_string
from gensim.parsing import strip_punctuation
from gensim.parsing import strip_tags
from gensim.parsing import strip_short
from gensim.parsing import strip_multiple_whitespaces
from gensim.parsing import strip_non_alphanum
from gensim.parsing import strip_numeric
from gensim.parsing import remove_stopwords
from gensim.parsing import stem_text
import numpy as np

# Connect to database -----------------

con = sqlite3.connect("data/database/nrl_comment_extract.db")

# Offensive Word List -----------------

offensive_words = pd.read_csv("https://www.cs.cmu.edu/~biglou/resources/bad-words.txt",
                                 header=None)

# Extract data ------------------------

nrl_match_comments = pd.read_sql(
    sql="""
        SELECT body
        FROM NRLMatchCommentsReddit
    """,
    con=con
)

# Clean comments ----------------------

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

# Remove stopwords and offensive words
nrl_match_comments.loc[:, "comment_tokens"] = nrl_match_comments["body"].apply(
    lambda x: preprocess_string(x, CUSTOM_FILTERS))

nrl_match_comments.loc[:, "comment_tokens"] = nrl_match_comments["comment_tokens"].apply(lambda comment: [word for word in comment if word not in offensive_words[0].tolist()])

# Run Modelling -----------------------

# Initialise dictionary based on comments
nrl_dictionary = Dictionary(nrl_match_comments["comment_tokens"])
nrl_vocab_len = len(nrl_dictionary)

# Run short text topic modelling
mgp = MovieGroupProcess(K=6, n_iters=60, alpha=.25, beta=.3)
mgp.fit(nrl_match_comments["comment_tokens"].tolist(), nrl_vocab_len)

# Explore extracted topics

def top_words(sorted_clusters, n_words):
    """
        Using the trained model, iterates through clusters and returns the top N words.

        sorted_clusters: The indices used to order the clusters based on the number of documents.
        n_words: The number of words per cluster that should be returned.
    """

    for cluster in sorted_clusters:
        sort_dicts = sorted(
            mgp.cluster_word_distribution[cluster].items(),
            key=lambda item: item[1],  # Sort the list of tuples by the word counts (item[0] would refer to the word).
            reverse=True)[:n_words]
        print('Cluster %s : %s'%(cluster,sort_dicts))
        print('-'*120)


doc_count = np.array(mgp.cluster_doc_count)  # Count docs per cluster.
sorted_clusters = doc_count.argsort()[::-1]  # Indices that will sort the array.

top_words(sorted_clusters, 10)

dump(mgp, 
    "models/nrl_reddit_topic_model.joblib")  




