#!/bin/bash
twistd --nodaemon web --port tcp:8443 --wsgi=publisher.app
