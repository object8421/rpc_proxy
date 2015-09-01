# -*- coding: utf-8 -*-
from __future__ import absolute_import
from thrift.protocol.TBinaryProtocol import TBinaryProtocol


class TUtf8BinaryProtocol(TBinaryProtocol):
    def writeString(self, str):
        """
            只要控制好了writeString, 在整个thrift系统中，所有的字符串都是utf-8格式的
        """
        if isinstance(str, unicode):
            str = str.encode("utf-8")

        # TBinaryProtocol 为 old style class
        TBinaryProtocol.writeString(self, str)