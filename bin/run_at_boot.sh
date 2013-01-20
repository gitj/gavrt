#!/bin/bash
python $DSS28CORE/identifySerialPorts.py
screen -dmLS forx python $DSS28CORE/forx_server.py
screen -dmLS power python $DSS28CORE/powerdbwriter.py
screen -dmLS ups python $DSS28CORE/ups.py
screen -dmLS tct python $DSS28CORE/tct_mon.py
screen -dmLS valon python $DSS28CORE/valonServer.py
screen -dmLS ibobutil python $DSS28CORE/iBOBUtilServer.py
screen -dmLS ibobproc python $DSS28CORE/iBOBProcessServer.py
screen -dmLS rssint python $DSS28CORE/rss_interface.py
screen -dmLS dataint python $DSS28CORE/datainterface.py
screen -dmLS xantint python $DSS28CORE/xant_interface.py
