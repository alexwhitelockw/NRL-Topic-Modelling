# Run Initial Reddit Extract

# The purpose of this script is to run the initial extraction of reddit
# threads and comments associated with the NRL subreddit. Specifically,
# the match threads.

from functions.extract_reddit_comments import extract_thread_titles
from functions.extract_reddit_comments import extract_thread_comments
import pandas as pd
import sqlite3


if __name__ == "__main__":

    con = sqlite3.connect("data/database/nrl_comment_extract.db")  # Initialise database

    nrl_match_threads = extract_thread_titles(  # Extract threads from NRL Match Threads
        bot_name="nrlStream",
        user_description="",
        subreddit_title="NRL",
        thread_search_terms="Round"  # All match threads have 'round' at the start
    )

    nrl_match_comments = extract_thread_comments(  # Extract comments from extracted threads
        bot_name="nrlStream",
        user_description="",
        thread_id_dict=nrl_match_threads
    )

    pd.DataFrame.from_dict(nrl_match_threads).to_sql(  # Push threads to database - replacing existing data
        name="NRLMatchThreadsReddit",
        con=con,
        if_exists="replace"
    )

    pd.DataFrame.from_dict(nrl_match_comments).to_sql(  # Push comments to database - replacing existing data
        name="NRLMatchCommentsReddit",
        con=con,
        if_exists="replace"
    )

