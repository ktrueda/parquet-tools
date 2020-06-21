import struct
import logging

from parquet_tools.gen_py.parquet.ttypes import FileMetaData
from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport

logger = logging.getLogger("parquet")


def get_filemetadata(path: str) -> FileMetaData:
    '''get file metadata using filepath'''
    with open(path, 'rb') as fo:
        return _read_footer(fo)


def _read_footer(file_obj) -> FileMetaData:
    footer_size = _get_footer_size(file_obj)
    file_obj.seek(-(8 + footer_size), 2)

    bytes_data = file_obj.read(footer_size)
    transportIn = TTransport.TMemoryBuffer(bytes_data)
    protocolIn = TCompactProtocol.TCompactProtocol(transportIn)
    fmd = FileMetaData()
    fmd.read(protocolIn)
    return fmd


def _get_footer_size(file_obj) -> int:
    file_obj.seek(-8, 2)
    tup = struct.unpack(b"<i", file_obj.read(4))
    return tup[0]
