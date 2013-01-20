#!/bin/bash
pkill -f rssint
screen -dmLS rssint python $DSS28CORE/rss_interface.py
