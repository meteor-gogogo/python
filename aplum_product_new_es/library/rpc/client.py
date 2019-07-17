# coding:utf-8
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from library.aplum_thrift.qserver.ctr_service.CtrService import Client
from library.aplum_thrift.qserver.product_info.ttypes import product_info
from utils import output

class RpcClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        try:
            self.transport = TSocket.TSocket(self.host, self.port)
            # 选择传输层
            self.transport = TTransport.TBufferedTransport(self.transport)
            # 选择传输协议
            protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            # 创建客户端
            self.client = Client(protocol)
            # 开启传输
            self.transport.open()
        except Thrift.TException as tx:
            output.output_info(tx.message)
        '''
        #test
        # 发起请求
        obj = product_info(pid='798',\
                           discount_rate='2.40',\
                           bid='102',\
                           cid='43',\
                           source_of_supply='secondhand',\
                           degree='good', \
                           is_promotion='1', \
                           original_price='2000',\
                           sale_price='798', \
                           discount_price='478', \
                           is_blackcard_member='0')

        print(client.getCtrInfo(obj))
        '''

    def __del__(self):
        # 关闭传输
        self.transport.close()

