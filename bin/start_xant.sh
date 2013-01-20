#!/bin/bash
pkill -f xantint
screen -dmLS xantint python $DSS28CORE/xant_interface.py
