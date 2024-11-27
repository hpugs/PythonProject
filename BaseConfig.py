#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import os

class BizException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

# 获取所有系统变量
env_variables = os.environ
env_variables["GD_KEY"]=None

