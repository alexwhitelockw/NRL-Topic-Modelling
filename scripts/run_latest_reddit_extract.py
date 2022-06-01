# Run Latest Reddit Extract

# The purpose of this script is to run the latest extraction of reddit
# threads and comments associated with the NRL subreddit. Specifically,
# the match threads.

from functions.create_data_backup import backup_extract_tables
from functions.extract_reddit_comments import extract_thread_titles
from functions.extract_reddit_comments import extract_thread_comments
import pandas as pd
import sqlite3


if __name__ == "__main__":

    db_fp = "data/database/nrl_comment_extract.db"  # Filepath to the database

    db_con = sqlite3.connect(db_fp)  # Initialise database connection

    previous_thread_ids = pd.read_sql(  # From dataframe to list
        con=db_con,
        sql="SELECT id from NRLMatchThreadsReddit"
    ).iloc[:,0].to_list()

    nrl_match_threads = extract_thread_titles(  # Extract threads from NRL Match Threads
        bot_name="nrlStream",
        user_description="",
        subreddit_title="NRL",
        thread_search_terms="Round",  # All match threads have 'round' at the start
        previous_thread_ids=previous_thread_ids
    )

    if len(nrl_match_threads) != 0:  # Make sure there are new match threads before searching for new comments

        backup_extract_tables(db_fp=db_fp)  # Create a backup of the current table

        nrl_match_comments = extract_thread_comments(  # Extract comments from extracted threads
            bot_name="nrlStream",
            user_description="",
            thread_id_dict=nrl_match_threads
        )

        pd.DataFrame.from_dict(nrl_match_threads).to_sql(  # Push threads to database - replacing existing data
            name="NRLMatchThreadsReddit",
            con=db_con,
            if_exists="append"
        )

        pd.DataFrame.from_dict(nrl_match_comments).to_sql(  # Push comments to database - replacing existing data
            name="NRLMatchCommentsReddit",
            con=db_con,
            if_exists="append"
        )

        print("New data appended to dataframe")

    else:
        print("No new data. Process terminated.")

