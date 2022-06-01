from datetime import date
from datetime import datetime
from datetime import timezone
from gensim.parsing import preprocess_string
import pandas as pd
import praw 
import re
import time

# Function to extract thread details (id, created time, and title) based on subreddit title and
# thread title search. The argument of previous_thread_ids enables the extraction of latest
# threads without pulling older threads. Does not pull any threads that are occuring on the 
# current date (timezone UTC).
def extract_thread_titles(bot_name, user_description, subreddit_title, thread_search_terms, previous_thread_ids=None):
    with praw.Reddit(bot_name, user_agent=f"{user_description}") as reddit_account:
        thread_list = []  # Initialise an empty list
        current_utc_date = int(datetime.now(timezone.utc).date().strftime("%s"))  # Current date in UTC timezone
        subreddit_search = reddit_account.subreddit(subreddit_title)  # Search for a subreddit based on title
        subreddit_details = subreddit_search.search(thread_search_terms)  # Search for a given thread
        if previous_thread_ids == None:
            for subreddit_detail in subreddit_details:  # Iterate through results and extract details
                if re.search("[A-Za-z]+ vs [A-Za-z]+", subreddit_detail.title) and subreddit_detail.created_utc < current_utc_date:  # Check if thread follows regex pattern
                    thread_dict = {
                        "id": subreddit_detail.id,
                        "created_utc": subreddit_detail.created_utc,
                        "title": subreddit_detail.title
                    }
                    thread_list.append(thread_dict)  # Append dictionary to list
        else:
            for subreddit_detail in subreddit_details:  # Iterate through results and extract details
                if re.search("[A-Za-z]+ vs [A-Za-z]+", subreddit_detail.title) and subreddit_detail.id not in previous_thread_ids and subreddit_detail.created_utc < current_utc_date:  # Check if thread follows regex pattern
                    thread_dict = {
                        "id": subreddit_detail.id,
                        "created_utc": subreddit_detail.created_utc,
                        "title": subreddit_detail.title
                    }
                    thread_list.append(thread_dict)  # Append dictionary to list
        return thread_list  # Return list

# Function to extract thread comments based on thread id input.
def extract_thread_comments(bot_name, user_description, thread_id_dict):
    with praw.Reddit(bot_name, user_agent=f"{user_description}") as reddit_account:
        comment_list = []  # Initialise an empty list to store comments
        time.sleep(5)
        for thread_id in thread_id_dict:
            thread_submission = reddit_account.submission(thread_id["id"])  # Identify thread based on id
            thread_submission.comments.replace_more(limit=None)
            for comment in thread_submission.comments.list():
                if comment.author is not None:  # Account for posts that have been removed (i.e., no author name)
                    pre_process_comment = preprocess_string(comment.body.lower())  # Pre-process text for downstream processes
                    comment_details = {
                        "comment_id": comment.submission.id,  # Extract comment id
                        "parent_id": comment.parent_id,  # Parent id - main thread or specific comment
                        "created_utc": comment.created_utc,  # Time comment was created
                        "author_fullname": comment.author.name,  # Author name
                        "body": comment.body.lower(),  # Comment text -- set to lower case
                        "body_preprocess": " ".join(pre_process_comment),  # Add pre-process text
                        "likes": comment.likes  # Number of likes
                    }
                    comment_list.append(comment_details)
        return comment_list
