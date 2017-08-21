#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import random


def _gen_paras(para_num):
	if para_num == 1:
		return [[i] for i in range(10)]
	r = _gen_paras(para_num-1)
	return [[i] + paras
		for i in range(10) for paras in r]

def gen_paras(para_num, concurrency):
	r = filter(lambda t: sum(t) == 10, _gen_paras(para_num))
	return [[float(i)/10 for i in t] for t in random.sample(r, concurrency)]
	
