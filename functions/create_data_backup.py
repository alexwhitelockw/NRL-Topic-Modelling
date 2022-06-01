from datetime import datetime
import sqlite3

# Function takes current table status and appends them to a backup
# table.
def backup_extract_tables(db_fp):
    
    backup_timestamp = datetime.now().strftime("%Y-%m-%d")
    db_con = sqlite3.connect(db_fp)
    
    print("Backing up NRL Threads")
    # Generate string before passing to sqlite3 execute
    thread_backup_sql = f"""  
            INSERT INTO NRLMatchThreadsReddit_Hist
            SELECT *,
                   '{backup_timestamp}'   AS backup_timestamp
            FROM NRLMatchThreadsReddit
        """
    db_con.execute(thread_backup_sql)
    db_con.commit()

    print("Backing up NRL Comments")
    # Generate string before passing to sqlite3 execute
    comment_backup_sql = f"""
            INSERT INTO NRLMatchCommentsReddit_Hist
            SELECT *,
                   '{backup_timestamp}'   AS backup_timestamp
            FROM NRLMatchCommentsReddit
        """
    db_con.execute(comment_backup_sql)
    db_con.commit()

    db_con.close()

    print("Backup complete")

