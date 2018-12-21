import sys
import glob
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hashlib import sha256

def main():
    # Make socket
    transport = TSocket.TSocket('128.226.180.166', 9090)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = FileStore.Client(protocol)

    # Connect!
    transport.open()
    #key = sha256("Mohit:mohit.txt").hexdigest()
    #print(client.findPred(key))
    #exit()
    metac = RFileMetadata()  

    metac.filename = "mohit.txt"
    metac.version = 10
    metac.owner = "Rohit"    
    obj = RFile(metac,"This new text")
    client.writeFile(obj)

    owner = "Rohit"
    filename = "mohit.txt"

    rfile = client.readFile(filename, owner)
    print(rfile.content, rfile.meta.version, rfile.meta.contentHash)

    metac.filename = "mohit.txt"
    metac.version = 5
    metac.owner = "Rohit"    
    obj1 = RFile(metac,"Real Madrid the best club")
    #client.writeFile(obj1)    
     
    #rfile1 = client.readFile(filename, owner)
    #print(rfile1.content, rfile1.meta.version, rfile1.meta.contentHash)

    # Close!
    transport.close()

if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
