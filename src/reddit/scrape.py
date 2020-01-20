#!/usr/bin/env python
# -*- coding: utf-8 -*-

import praw
POST_MATCH_TEXT = "Discussion | Esports"
LINK_FLAIR_TEMPLATE_ID = "af5b57fc-374a-11e6-abd9-0e7c62f92521"
PRAW_AGENT = praw.Reddit("csgo",
                         user_agent="csgo user agent"
                         )

print(PRAW_AGENT.read_only)

subreddit = PRAW_AGENT.subreddit("GlobalOffensive")
test_submission = PRAW_AGENT.submission(url="https://old.reddit.com/r/GlobalOffensive/comments/cwj33v/ence_vs_avangar_starladder_major_berlin_2019_the/")

match_summaries = subreddit.search('flair:"Discussion | Esports" self:yes', limit=1000)
count = 0
for subi in match_summaries:
    count += 1
    title = subi.title
    print(count, title, subi.author)
