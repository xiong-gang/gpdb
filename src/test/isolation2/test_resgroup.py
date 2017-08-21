#!/usr/bin/env python
#-*- coding: utf-8 -*-

from resgroup_test import Query


if __name__ =="__main__":
    q = Query("resgroup_test",
              "default_group",
              20,
              1000)
    q.run()
