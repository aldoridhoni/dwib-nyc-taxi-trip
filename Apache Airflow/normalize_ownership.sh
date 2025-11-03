#!/bin/bash

chown -R 1000:1000 ./
chown -v -R 1000:1000 ./{logs,dags,plugins,config,data}
chown -v -R 1000:1000 ../Data\ Build\ Tool
