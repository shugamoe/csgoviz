#!/usr/bin/env python
# -*- coding: utf-8 -*-

import praw

PRAW_AGENT = praw.Reddit("csgo",  # Site ID
                         user_agent="csgo user agent"
                         )

print(PRAW_AGENT.read_only)

for submission in PRAW_AGENT.subreddit("GlobalOffensive").hot(limit=10):
    print(submission.flair)
