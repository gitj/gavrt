#!/bin/bash
pkill -f ibobutil
pkill -f ibobproc

screen -dmLS ibobutil python $DSS28CORE/iBOBUtilServer.py
screen -dmLS ibobproc python $DSS28CORE/iBOBProcessServer.py
