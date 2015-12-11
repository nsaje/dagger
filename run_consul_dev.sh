#!/bin/bash

rm -rf /tmp/consul; consul agent -server -data-dir=/tmp/consul -log-level="DEBUG" -bootstrap -ui-dir ~/Downloads/dist
