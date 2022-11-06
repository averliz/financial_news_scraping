import os
import numpy as np
import datetime as dt
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
pd.options.display.max_colwidth = 500
pd.options.display.max_columns = 50
from psaw import PushshiftAPI

api = PushshiftAPI()

# this function gets all submissions from 'wallstreetbets' with the title "What Are Your Moves Tomorrow", saves the data in a csv file and
# returns a dataframe containing the results
# starting date and ending date should be passed as datetime objects
# e.g. starting_date = dt.datetime(2021, 1, 1)
# e.g. ending_date = dt.datetime(2022, 9, 30)
def get_post_ids(starting_date=dt.datetime(2021, 1, 1), ending_date=dt.datetime(2022, 9, 30)):
    if os.path.exists('post_links.csv'):
        submissions = pd.read_csv('post_links.csv')
    else:
        # converting the starting and ending dates to epochs
        start_epoch=int(starting_date.timestamp())
        end_epoch=int(ending_date.timestamp())

        # getting all reddit submissions (posts) between the two specified dates
        api_request_generator = api.search_submissions(subreddit='wallstreetbets', after=start_epoch, before=end_epoch, title="What Are Your Moves Tomorrow")

        submissions = pd.DataFrame([submission.d_ for submission in api_request_generator])
        # we only want these 3 attributes from the data
        submissions = submissions[["title", "url", "id"]]
        submissions = submissions[submissions["title"].str.contains("What Are Your Moves Tomorrow, ") == True]
        submissions["date"] = submissions.title.str[30:]
        submissions.to_csv("post_links.csv", index=False)

    return submissions

# getting all the comments for each reddit submission
def get_post_comments(date_id_dict, tickers):
    master_counter = 0 # counting progress
    comment_data = pd.DataFrame(columns=['Date', 'Id', 'Comment'])
    for date, id in date_id_dict.items():
        gen = api.search_comments(subreddit='wallstreetbets', link_id=id)
        max_response_cache = 100 # 100 relevant comments (out of 10k+) per date
        cache = []
        i = 0
        for c in gen:
            comment = c.d_["body"]
            
            if any(ticker in comment for ticker in tickers): # only add the comment if any relevant tickers are present
                cache.append(1) # used to keep track of the added comments

                row = {'Date': date, 'Id': id, 'Comment': c.d_["body"]}
                row_df = pd.DataFrame([row])
                comment_data = pd.concat([comment_data, row_df], axis=0, ignore_index=True)
                i += 1

            if len(cache) >= max_response_cache:
                break
        
        
        master_counter += 1
        if master_counter % 2 == 0:
            print("2 date done - now at", date)
            comment_data.to_csv("comment_data.csv", index=False)
            

submissions = get_post_ids()
date_dict = dict(zip(submissions["date"], submissions["id"]))
# ticker_df = pd.read_csv("tickers.csv")
# ticker_df["Symbol"] = ticker_df["Symbol"].astype(str)
# tickers = list(ticker_df["Symbol"])
tickers = ["TSLA", "MSFT", "GOOG", "AAPL"] # try with just these few - using all the tickers result in some irrelevant results
get_post_comments(date_dict, tickers)