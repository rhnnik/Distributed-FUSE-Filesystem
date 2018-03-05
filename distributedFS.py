#!/usr/bin/env python
#Distributed File system
import logging

import sys, SimpleXMLRPCServer, getopt, time, threading, unittest
from datetime import datetime, timedelta
from math import ceil
from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

import xmlrpclib
import pickle
from xmlrpclib import Binary

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

bsize = 512

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):
    """Implements a hierarchical file system by using FUSE virtual filesystem.
       The file structure and data are stored in local memory in variable.
       Data is lost when the filesystem is unmounted"""

    def __init__(self):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        
	#Meta ports 	
	self.rpc = xmlrpclib.ServerProxy("http://localhost:"+str(port)+'/')
	
	#Data ports
	self.dserver = len(port1)
	self.rpc1= list()
	for i in range(0,len(port1)):
		self.rpc1.append(xmlrpclib.ServerProxy("http://localhost:"+str(port1[i])+'/'))
	
	#Initialise '/'	
 	initial = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                st_mtime=now, st_atime=now, st_nlink=2, files=list())
	meta = pickle.dumps(initial)
	self.rpc.put(Binary('/'),Binary(meta))

    def hashing(self,path):

	var = 0
	for i in str(path):
		var = var + ord(i)
	return var%self.dserver

    def traverse(self, path, tdata = False):

        p = self.data if tdata else 0
        if tdata:
            for i in path.split('/') :
               p = p[i] if len(i) > 0 else p
        return p 

    def traverseparent(self, path, tdata = False):

        p = self.data if tdata else 0
        target = path[path.rfind('/')+1:]
        path = path[:path.rfind('/')]
	
        if tdata:
            for i in path.split('/') :
                p = p[i] if len(i) > 0 else p
        return p, target,path


    def chmod(self, path, mode):
        #Receive marshalled data from server with the key 'path'
	receive= self.rpc.get(Binary(path))
	#Unmarshal the received data	
	dat = pickle.loads(receive.data) 		
        dat['st_mode'] &= 0o770000
        dat['st_mode'] |= mode
	meta = pickle.dumps(dat)
	self.rpc.put(Binary(path),Binary(meta))
        return 0

    def chown(self, path, uid, gid):
        receive= self.rpc.get(Binary(path))
	dat = pickle.loads(receive.data) 		
        dat['st_uid'] = uid
        dat['st_gid'] = gid
	meta = pickle.dumps(dat)
	self.rpc.put(Binary(path),Binary(meta))

    def getattr(self, path, fh = None):
        self.rpc.print_content()
	receive= self.rpc.get(Binary(path))
       	if receive ==False:
            raise FuseOSError(ENOENT)
	else:       
	    dat = pickle.loads(receive.data) 		
	    return {attr:dat[attr] for attr in dat.keys() if attr != 'files'}

    def mkdir(self, path, mode):
        p, tar, pathx = self.traverseparent(path)
	if len(pathx)==0:
		pathx ='/'
        receive= self.rpc.get(Binary(pathx))
	dat = pickle.loads(receive.data) 	
	dat['files'].append(tar)
	dat['st_nlink'] += 1
	strmeta =pickle.dumps(dat)	
	self.rpc.put(Binary(pathx),Binary(strmeta))
	a = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),files=list())
	strmeta = pickle.dumps(a)
	self.rpc.put(Binary(path),Binary(strmeta))


    def getxattr(self, path, name, position=0):
        receive= self.rpc.get(Binary(path))
        if receive!=False:
	    dat = pickle.loads(receive.data) 
            attrs = dat.get('attrs', {})
	try:        
	    return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        receive= self.rpc.get(Binary(path))
	dat = pickle.loads(receive.data) 
        attrs = dat.get('attrs', {})
        return attrs.keys()

       
    def open(self, path, flags):
        self.fd += 1
        return self.fd


    def readdir(self, path, fh):
        receive= self.rpc.get(Binary(path))
	dat = pickle.loads(receive.data) 	
        return ['.', '..'] + [x for x in dat['files'] ]

    def readlink(self, path):
	x = self.hashing(path+'0')		 
	receive = self.rpc.get(Binary(path))
	c = pickle.loads(receive.data) 
	size = c['st_size']
	noblock = int(ceil(float(size)/bsize))
	dreceive= self.rpc1[x].get(Binary(path+'0'))	
	if dreceive != False:	
		dat =pickle.loads(dreceive.data)
		for i in range(1,noblock):
			dreceive=self.rpc1[(x+i)%self.dserver].get(Binary(path+str(i)))			
			b=pickle.loads(dreceive.data)
			dat=dat+b	
        return dat

    def removexattr(self, path, name):
        attrs = p.get('attrs', {})
	receive= self.rpc.get(Binary(path))
        if receive!=False:
	    dat = pickle.loads(receive.data) 
            attrs = dat.get('attrs', {})
	try:       
	     del attrs[name]
        except KeyError:
            pass       # Should return ENOATTR
	
        
    def rmdir(self, path):
        p, tar,pathx = self.traverseparent(path)
	if len(path)==0:
		path='/'	
	receive =self.rpc.get(Binary(path))
	dt = pickle.loads(receive.data)
        if len(dt['files']) > 0:
            raise FuseOSError(ENOTEMPTY)
	self.rpc.remov(Binary(path))
	if len(pathx)==0:
		pathx='/'
	receive =self.rpc.get(Binary(pathx))
	dt = pickle.loads(receive.data)      
	dt['files'].remove(str(tar))
        dt['st_nlink'] -= 1
	strmeta = pickle.dumps(dt)
	self.rpc.put(Binary(pathx),Binary(strmeta))

    def setxattr(self, path, name, value, options, position=0):
	receive= self.rpc.get(Binary(path))
        if receive!=False:
	    dat = pickle.loads(receive.data) 
            attrs = dat.setdefault('attrs', {})
            attrs[name] = value
        

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        p, tar,pathx = self.traverseparent(target)
       	a = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))
	strmeta = pickle.dumps(a)
	self.rpc.put(Binary(target),Binary(strmeta))
	if len(pathx)==0:
		pathx='/'	
	receive =self.rpc.get(Binary(pathx))
	dt =pickle.loads(receive)
	dt['files'].append(tar)
	strmeta = pickle.dumps(dt)
	self.rpc.put(Binary(pathx),Binary(strmeta))

	x = self.hashing(target+'0')		 
	receive = self.rpc.get(Binary(target))
	c = pickle.loads(receive.data) 
	size = c['st_size']
	noblock = int(ceil(float(size)/bsize))
	dreceive= self.rpc1[x].get(Binary(target+'0'))	
	if dreceive != False:	
		dt =pickle.loads(dreceive.data)
		for i in range(1,noblock):
			dreceive=self.rpc1[(x+i)%self.dserver].get(Binary(target+str(i)))			
			b=pickle.loads(dreceive.data)
			dt=dt+b
	else:
		dt={}
	dt=''.join(dt)						
	dt = source
	t=len(dt) 
	a=list()
	for pos in range(0,t,bsize):
        	a.append(dt[pos:pos+bsize])      
	dt = a
	for i in range(0,len(dt)):
		strdata=pickle.dumps(dt[i])
		dreceive=self.rpc1[(x+i)%self.dserver].put(Binary(target+str(i)),Binary(strdata))			
	c['st_size']= length
	strmeta = pickle.dumps(c)
	self.rpc.put(Binary(target),Binary(strmeta))


    def unlink(self, path):
        p, tar,pathx = self.traverseparent(path)
	self.rpc.remov(Binary(path))
	if len(pathx)==0:
		pathx='/'
	receive =self.rpc.get(Binary(pathx))
	dat = pickle.loads(receive.data)        
	dat['files'].remove(str(tar))
	strmeta = pickle.dumps(dat)
	self.rpc.put(Binary(pathx),Binary(strmeta))
        

    def utimens(self, path, times = None):
        now = time()
        atime, mtime = times if times else (now, now)
        receive= self.rpc.get(Binary(path))
	dat = pickle.loads(receive.data) 	
	dat['st_atime'] = atime
	dat['st_mtime'] = mtime
	strmeta = pickle.dumps(dat)
	self.rpc.put(Binary(path),Binary(strmeta))

    def read(self, path, size, offset, fh):
	x = self.hashing(path+'0')		 
	dreceive= self.rpc1[x].get(Binary(path+'0'))	
	receive = self.rpc.get(Binary(path))
	c = pickle.loads(receive.data) 
	size = c['st_size']
	noblock = int(ceil(float(size)/bsize))
	if dreceive != False:	
		dat =pickle.loads(dreceive.data)
		for i in range(1,noblock):
			dreceive=self.rpc1[(x+i)%self.dserver].get(Binary(path+str(i)))			
			b=pickle.loads(dreceive.data)
			dat=dat+b	
	else:
		dat={}

	dat=''.join(dat)						
	dat1=dat[offset:offset+size]				        	
	return dat1


    def write(self, path, data, offset, fh):
        
	x = self.hashing(path+'0')		 
	receive = self.rpc.get(Binary(path))
	c = pickle.loads(receive.data) 
	size = c['st_size']
	noblock = int(ceil(float(size)/bsize))
	dreceive= self.rpc1[x].get(Binary(path+'0'))	
	if dreceive != False:	
		dt =pickle.loads(dreceive.data)
		for i in range(1,noblock):
			dreceive=self.rpc1[(x+i)%self.dserver].get(Binary(path+str(i)))			
			b=pickle.loads(dreceive.data)
			dt=dt+b
	else:
		dt={}
	a= list()								
	dt=''.join(dt)						
	dt= dt[:offset] + data + dt[offset+len(data):]
	t=len(dt) 
	for pos in range(0,t,bsize):
        	a.append(dt[pos:pos+bsize])
	dt = a
	for i in range(0,len(dt)):
		strdata=pickle.dumps(dt[i])
		dreceive=self.rpc1[(x+i)%self.dserver].put(Binary(path+str(i)),Binary(strdata))			
	c['st_size']=t
	strmeta = pickle.dumps(c)
	self.rpc.put(Binary(path),Binary(strmeta))
	return len(data)


    def create(self, path, mode):
        p, tar, pathx = self.traverseparent(path)
      	if len(pathx)==0:
		pathx ='/'
        receive= self.rpc.get(Binary(pathx))
	dat = pickle.loads(receive.data) 	
	dat['files'].append(tar)
	strmeta =pickle.dumps(dat)	
	self.rpc.put(Binary(pathx),Binary(strmeta))
 	a = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                     st_size=0, st_ctime=time(), st_mtime=time(),
                     st_atime=time())	
	strmeta = pickle.dumps(a)
	self.rpc.put(Binary(path),Binary(strmeta))
        self.fd += 1
        return self.fd


    def rename(self, old, new):
	po, tar1, pathx1= self.traverseparent(old)
        pn, tar2, pathx2= self.traverseparent(new)
	receive = self.rpc.get(Binary(old))
	dto = pickle.loads(receive.data)
	size = dto['st_size']
	noblock = int(ceil(float(size)/bsize))
	
	if len(pathx1)==0:
		pathx1='/'
	if len(pathx2)==0:
		pathx2='/'
	
	receive1 =self.rpc.get(Binary(pathx1))
	dt1= pickle.loads(receive1.data)
	dt1['files'].remove(str(tar1))
        if dto['st_mode'] & 0o770000 == S_IFDIR:
		dt1['st_nlink'] -= 1
	strmeta = pickle.dumps(dt1)
	self.rpc.put(Binary(pathx1),Binary(strmeta))

	receive2 =self.rpc.get(Binary(pathx2))
	dt2= pickle.loads(receive2.data)
	dt2['files'].append(tar2)
        if dto['st_mode'] & 0o770000 == S_IFDIR:
		dt2['st_nlink'] += 1
	strmeta1 = pickle.dumps(dt2)
	strmeta3 = pickle.dumps(dto)
	self.rpc.put(Binary(pathx2),Binary(strmeta1))
	self.rpc.remov(Binary(old))
	self.rpc.put(Binary(new),Binary(strmeta3))

	x = self.hashing(old+'0')		 
	x1 = self.hashing(new+'0')		 
	dreceive0= self.rpc1[x].get(Binary(old+'0'))	
	if dreceive0 != False:	
		for i in range(0,noblock):
			dreceive0=self.rpc1[(x+i)%self.dserver].get(Binary(old+str(i)))			
			b=pickle.loads(dreceive0.data)
			strdata=pickle.dumps(b)
			self.rpc1[(x1+i)%self.dserver].put(Binary(new+str(i)),Binary(strdata))			
			self.rpc1[(x+i)%self.dserver].remov(Binary(old+str(i)))



    def truncate(self, path, length, fh = None):
	x = self.hashing(path+'0')		 
	receive = self.rpc.get(Binary(path))
	c = pickle.loads(receive.data) 
	size = c['st_size']
	noblock = int(ceil(float(size)/bsize))
	dreceive= self.rpc1[x].get(Binary(path+'0'))	
	if dreceive != False:	
		dt =pickle.loads(dreceive.data)
		for i in range(1,noblock):
			dreceive=self.rpc1[(x+i)%self.dserver].get(Binary(path+str(i)))			
			b=pickle.loads(dreceive.data)
			dt=dt+b
	else:
		dt={}
	dt=''.join(dt)						
	dt=dt[:length]
	t=len(dt) 
	a=list()
	for pos in range(0,t,bsize):
        	a.append(dt[pos:pos+bsize])      
	dt = a
	for i in range(0,len(dt)):
		strdata=pickle.dumps(dt[i])
		dreceive=self.rpc1[(x+i)%self.dserver].put(Binary(path+str(i)),Binary(strdata))			
	c['st_size']= length
	strmeta = pickle.dumps(c)
	self.rpc.put(Binary(path),Binary(strmeta))


if __name__ == '__main__':
    if len(argv) <= 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)
    port = argv[2]
    port1 = list()
    for x in range(3,len(argv)):
	port1.append(argv[x])		
    logging.basicConfig(level=logging.DEBUG)	
    fuse = FUSE(Memory(), argv[1], foreground=True, debug=True)
