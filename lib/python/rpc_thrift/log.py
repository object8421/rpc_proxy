# -*- coding: utf-8 -*-
from __future__ import absolute_import
import time
import traceback
from rpc_thrift.services.ttypes import RpcException

def calculate_class_fun_execute_time(info_logger):
    """
    计算类成员函数的执行时间, 使用方法:
    @calculate_class_fun_execute_time(logger)
    def xxxx
    """
    def wrapper(func):
        def _calculate_time(*args, **kwargs):
            try:
                t = time.time()
                result = func(*args, **kwargs)
                t = time.time() - t
                args_str = ", ".join(map(str, args[1:]))
                if info_logger:
                    info_logger.info('%s(%s), elapsed: %.4fs' % (func.__name__, args_str, t))
                return result
            except Exception as e:
                args_str = ", ".join(map(str, args[1:]))
                raise RpcException(0, '%s(%s), Exception: %s, Trace: %s' % (func.__name__, args_str, str(e), traceback.format_exc()))

        return _calculate_time
    return wrapper

def calculate_fun_execute_time(info_logger):
    """
    计算类成员函数的执行时间, 使用方法:
    @calculate_class_fun_execute_time(logger)
    def xxxx
    """
    def wrapper(func):
        def _calculate_time(*args, **kwargs):
            try:
                t = time.time()
                result = func(*args, **kwargs)
                t = time.time() - t
                args_str = ", ".join(map(str, args))
                if info_logger:
                    info_logger.info('%s(%s), elapsed: %.4fs' % (func.__name__, args_str, t))
                return result
            except Exception as e:
                args_str = ", ".join(map(str, args))
                raise RpcException(0, '%s(%s), Exception: %s, Trace: %s' % (func.__name__, args_str, str(e), traceback.format_exc()))

        return _calculate_time
    return wrapper