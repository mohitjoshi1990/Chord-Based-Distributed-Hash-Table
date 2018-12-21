import glob
import sys
import socket
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])
from chord import FileStore
from chord.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import socket
from hashlib import sha256
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

localhost = socket.gethostbyname(socket.gethostname())
print(sys.argv[1])
print(localhost)
myNode = NodeID(sha256(localhost +":"+sys.argv[1]).hexdigest(), localhost,int(sys.argv[1]))



class FileStoreHandler:
    serverdict = {}
    rfileownDict = {}
    fingerTableList = []

    def setFingertable(self, node_list):
        
        logger.debug("Begin initializing finger table of the server node")
        if(not node_list): 
            raise SystemException("node list is empty, could not set up the finger table")
            return
        self.fingerTableList = node_list
        logger.debug("Ending initializing finger table of the server node")    
        return


    def liesInKey(self, key, startnodeDigest, endnodeDigest):
        logger.debug("finding key in range starts")
        if((startnodeDigest < endnodeDigest) and (key>startnodeDigest and key <= endnodeDigest)):
            return True        
        if((startnodeDigest > endnodeDigest) and (not(key>=endnodeDigest and key < startnodeDigest))):
            return True
        logger.debug("finding key in range ends")
        return False


    def findPred(self, key):
        logger.debug("Begin finding node predecessor")
        if(not self.fingerTableList):
            logger.debug("Finger table does not exist for current node")
            raise SystemException("Finger table does not exist for current node")
            return
        if(self.liesInKey(key, myNode.id, self.fingerTableList[0].id)):
            logger.debug("Returned finding node predecessor")
            return myNode
        else:
	    highestNodeforCurrentNode = self.fingerTableList[-1]           
            for currentNode in reversed(self.fingerTableList):
                if(self.liesInKey(currentNode.id, myNode.id, key)):                            
                    # Make socket
                    transport = TSocket.TSocket(currentNode.ip, int(currentNode.port))
                    # Buffering is critical. Raw sockets are very slow
                    transport = TTransport.TBufferedTransport(transport)
                    # Wrap in a protocol
                    protocol = TBinaryProtocol.TBinaryProtocol(transport)
                    # Create a client to use the protocol encoder
                    client = FileStore.Client(protocol)
                    # Connect!
                    transport.open()
                    newnode = client.findPred(key)
		    transport.close()
		    return newnode
	    return highestNodeforCurrentNode	
        return myNode


    def getNodeSucc(self):        
        logger.debug("Fetch successor node starts")
        if(not self.fingerTableList):
            logger.debug("Finger table does not exist for current node")
            raise SystemException("Finger table does not exist for current node")
	    return
        else:
            node = self.fingerTableList[0]
            logger.debug("Ending finding successor node")        
            return node 
        return   
    
    
    def findSucc(self, key):
        logger.debug("Begin finding node succesor")
        if(not self.fingerTableList):
            logger.debug("Finger table does not exist for current node")
            raise SystemException("Finger table does not exist for current node")
	    return
        node = self.findPred(key)
	if node == myNode:
            newnode = self.getNodeSucc()
            logger.debug("End finding node succesor")  
            return newnode
	else:         
            # Make socket
            transport = TSocket.TSocket(node.ip, node.port)
            # Buffering is critical. Raw sockets are very slow
            transport = TTransport.TBufferedTransport(transport)
            # Wrap in a protocol
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            # Create a client to use the protocol encoder
            client = FileStore.Client(protocol)
            # Connect!
            transport.open() 
            newnode = client.getNodeSucc()
	    transport.close()
            logger.debug("End finding node succesor")  
            return newnode


    def writeFile(self, rFile):
	if(not self.fingerTableList):
            logger.debug("Finger table does not exist for current node")
            raise SystemException("Finger table does not exist for current node")
	    return
        key = sha256(rFile.meta.owner+":"+rFile.meta.filename).hexdigest()
        returnnode = self.findSucc(key)
	logger.debug(returnnode)   
        if myNode != returnnode:
            logger.debug("Server does not own the file, cannot write the file")
            raise SystemException("Server does not own the file, cannot write the file")
            return
	else:
            logger.debug("File write on server is started :"+ rFile.meta.filename)        
            metac = RFileMetadata()
            rFileobj = RFile(metac,"")
            if rFile.meta.filename in self.serverdict.keys():
                self.rfileownDict = self.serverdict[rFile.meta.filename]
                if rFile.meta.owner in self.rfileownDict.keys():
                    rfile1 = self.rfileownDict[rFile.meta.owner]
                    rFileobj.meta.version = rfile1.meta.version + 1
                    rFileobj.meta.contentHash = sha256(rFile.content).hexdigest()
                    rFileobj.content = rFile.content
                    rFileobj.meta.filename = rfile1.meta.filename
                    self.rfileownDict[rFile.meta.owner] = rFileobj
                else:                    
                    logger.debug("Owner cannot write file as there is file with same name of another owner")
                    raise SystemException("Owner cannot write file as there is file with same name of another owner")
		    return
      
            else:
	        logger.debug("New File creation begins")   
                rFileobj = rFile
                rFileobj.meta.version = 0
                rFileobj.meta.contentHash = sha256(rFile.content).hexdigest()
                rFileobj.meta.filename = rFile.meta.filename
                self.rfileownDict = {}
                self.rfileownDict[rFile.meta.owner] = rFileobj

            self.serverdict[rFile.meta.filename] = self.rfileownDict        
           
            logger.debug("File write on server is complete :"+ rFile.meta.filename)
        return


    def readFile(self, filename, owner):
        if(not self.fingerTableList):
            logger.debug("Finger table does not exist for current node")
            raise SystemException("Finger table does not exist for current node")
	    return
	logger.debug("Reading file from server starts :"+ filename)
        key = sha256(owner+":"+filename).hexdigest()
        returnnode = self.findSucc(key)
        if myNode != returnnode:
            logger.debug("Server does not own the file, cannot read the file")
            raise SystemException("Server does not own the file, cannot read the file")
            return
        
        if filename in self.serverdict.keys():

            self.rfileownDict = self.serverdict[filename]
            if owner in self.rfileownDict.keys():
                rFile = self.rfileownDict[owner]
		return rFile
            else:                    
                logger.debug("Does not have the permission to access the file")
                raise SystemException("Does not have the permission to access the file")
		return            
        else:            
            logger.debug("Server does not have the file")
            raise SystemException("Server does not have the file")
	    return   
            
        logger.debug("File read complete :"+ filename)
        return


if __name__ == '__main__':
    handler = FileStoreHandler()
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    # You could do one of these for a multithreaded server
    # server = TServer.TThreadedServer(
    #     processor, transport, tfactory, pfactory)
    # server = TServer.TThreadPoolServer(
    #     processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')