#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'NHS'

from cassandra import cluster


class DBCassandra:

    def __init__(self,host,port):
        self.cluster = cluster.Cluster(host,port)

    def connect(self,keyspace):
        self.session = self.cluster.connect(keyspace)

    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()