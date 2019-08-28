import struct, io, socket, sys

'''
@Author:lihongge
@Create:19/07/13
@description:ip地址转化地址工具类
'''


class IpCity():
    __INDEX_BLOCK_LENGTH = 12
    __TOTAL_HEADER_LENGTH = 8192

    __f = None
    __headerSip = []
    __headerPtr = []
    __headerLen = 0
    __indexSPtr = 0
    __indexLPtr = 0
    __indexCount = 0
    __dbBinStr = ''

    def __init__(self):
        dbfile = '/home/aplum/work_lh/code/data_ctr_to_redis/scripts/IpCity.db'
        self.initDatabase(dbfile)

    def binarySearch(self, ip):
        if not ip.isdigit(): ip = self.ip2long(ip)

        if self.__indexCount == 0:
            self.__f.seek(0)
            superBlock = self.__f.read(8)
            self.__indexSPtr = self.getLong(superBlock, 0)
            self.__indexLPtr = self.getLong(superBlock, 4)
            self.__indexCount = int((self.__indexLPtr - self.__indexSPtr) / self.__INDEX_BLOCK_LENGTH) + 1

        l, h, dataPtr = (0, self.__indexCount, 0)
        while l <= h:
            m = int((l + h) >> 1)
            p = m * self.__INDEX_BLOCK_LENGTH

            self.__f.seek(self.__indexSPtr + p)
            buffer = self.__f.read(self.__INDEX_BLOCK_LENGTH)
            sip = self.getLong(buffer, 0)
            if ip < sip:
                h = m - 1
            else:
                eip = self.getLong(buffer, 4)
                if ip > eip:
                    l = m + 1
                else:
                    dataPtr = self.getLong(buffer, 8)
                    break

        if dataPtr == 0: raise Exception("Data pointer not found")

        dataCity = self.returnData(dataPtr)
        resultStr = dataCity["region"].decode('utf-8')
        result = resultStr.split('|')[0] + resultStr.split('|')[2] + resultStr.split('|')[3]
        return result

    def initDatabase(self, dbfile):
        try:
            self.__f = io.open(dbfile, "rb")
        except IOError as e:
            print("[Error]: %s" % e)
            sys.exit()

    def returnData(self, dataPtr):
        dataLen = (dataPtr >> 24) & 0xFF
        dataPtr = dataPtr & 0x00FFFFFF

        self.__f.seek(dataPtr)
        data = self.__f.read(dataLen)

        return {
            "city_id": self.getLong(data, 0),
            "region": data[4:]
        }

    def ip2long(self, ip):
        _ip = socket.inet_aton(ip)
        return struct.unpack("!L", _ip)[0]

    def isip(self, ip):
        p = ip.split(".")

        if len(p) != 4: return False
        for pp in p:
            if not pp.isdigit(): return False
            if len(pp) > 3: return False
            if int(pp) > 255: return False
        return True

    def getLong(self, b, offset):
        if len(b[offset:offset + 4]) == 4:
            return struct.unpack('I', b[offset:offset + 4])[0]
        return 0

    def close(self):
        if self.__f != None:
            self.__f.close()

        self.__dbBinStr = None
        self.__headerPtr = None
        self.__headerSip = None

