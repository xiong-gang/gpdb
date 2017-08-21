#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import os
import signal
import uuid
from random import random
import multiprocessing as mp
import pygresql.pg
from generate_test import gen_paras


class Query(object):

	QUERY_DDL = [
		"create table tab1 as select i as c1, round(random()*1000) as c2, round(random()*10000) as c3 from generate_series(1,100000)i",
		"create table tab2 as select * from tab1",
		"create table tabf1 as select random() as c1 from generate_series(1,100000)",
		"grant all on tab1 to public",
		"grant all on tab2 to public",
		"grant all on tabf1 to public"
	]

	QUERY_LEVEL = dict([
		(0, "select * from tab1"),	
		(1, "select sqrt(sqrt(c1)) from tabf1"), 
		(2, "select tab1.c3, count(*) as cnt from tab1, tab2 where tab1.c2=tab2.c2 group by tab1.c3")
	])

	def __init__(self, dbname, resgroup, concurrency, count, time=None):
		self.dbname = dbname
		self.resgroup = resgroup
		self.concurrency = concurrency
		# validate count and time
		if count > 0 and time is not None:
			raise
		self.count = count if count > 0 else float('inf')
		self.time=time
		self.role_infix = str(uuid.uuid1()).replace("-", "")

	def createdb(self):
		conn = pygresql.pg.connect(dbname="postgres")
		conn.query("drop database {dbname}".format(dbname=self.dbname))
		conn.query("create database {dbname}".format(dbname=self.dbname))
		conn.close()
	
	def connectdb(self, dbname=None):
		if dbname is None:
			self.conn = pygresql.pg.connect(dbname=self.dbname)
		else:
			self.conn = pygresql.pg.connect(dbname=dbname)

	def closeconn(self):
		self.conn.close()

	def prepare(self):
		self.createdb()
		self.connectdb()
		for ddl in self.QUERY_DDL:
			self.conn.query(ddl)
		self.closeconn()

	def set_role(self):
		role = "role_{infix}_{pid}".format(pid=os.getpid(), infix=self.role_infix)
		self.conn.query("create role {role} resource group {resgroup}".format(role=role,
										      resgroup=self.resgroup))
		self.conn.query("set role {role}".format(role=role))
		
	def choose_query(self, para):
		f = random()
		intervals = [para[0]] + [sum(para[:i+1]) for i in range(1, len(para))]
		for i, interval in enumerate(intervals):
			if f < interval:
				return self.QUERY_LEVEL.get(i)
			
	def session(self, para):
		self.connectdb()
		self.set_role()
		i = 0
		while i < self.count:
			sql = self.choose_query(para)
			self.conn.query(sql)
			i += 1
		self.closeconn()

	def drop_roles(self, pids):
		self.connectdb("postgres")
		for pid in pids:
			role_name = "role_{infix}_{pid}".format(infix=self.role_infix,
								pid=pid)
			self.conn.query("drop role %s" % role_name)
		self.closeconn()
	
	def run(self):
		self.prepare()
		paras = gen_paras(len(self.QUERY_LEVEL), self.concurrency)
		procs = [mp.Process(target=self.session, args=(para,))
			 for para in paras]
		pids = [proc.pid for proc in procs]
		[proc.start() for proc in procs]
		[proc.join(self.time) for proc in procs]
		if self.time is not None:
			for proc in procs:
				if proc.is_alive():
					os.kill(proc.pid, signal.SIGINT)
					os.wait(proc.pid)
		self.drop_roles(pids)

