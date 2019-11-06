#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json

results = requests.get("http://localhost:3000/results")
results = json.loads(results.text)
