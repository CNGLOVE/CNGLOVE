#!/usr/bin/python
# -*- coding: UTF-8 -*-
import datetime
import traceback
import json  
import pkuseg
import time
import multiprocessing as mp
import os
seg = pkuseg.pkuseg() 
cores=40
def process_wrapper(chunkStart,chunkSize,num):
  #print(num)
  wrt_text=''
  #fw=open('pcd_text.words',mode='ab+')
  with open('news2016zh_train.json','r') as f:
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    f.seek(chunkStart)
    lines = f.readlines(1024*1024)
    for line in lines:
      
      if not line:
        continue
      text_json=json.loads(line)#把json串变成python的数据类型：字典
      temp_data=text_json['content']
      if not temp_data:
        continue
      text_seg = seg.cut(temp_data)
      wrt_text=wrt_text+(' '.join(text_seg)+'\n')
  return wrt_text,num
    #fw.write(wrt_text)
    #print('done')
    #fw.close()



def chunkify(fname,size=1024*1024):
  fileEnd = os.path.getsize(fname)
  i=0;
  with open(fname,'r') as f:
    chunkEnd = f.tell()
    while True:
      if i%100000==0:
        print(i)
      i+=1;
      chunkStart = chunkEnd
      lines=f.readlines(size)
      #print(len(lines))
      #print(lines[0][0:30])
      for line in lines:
        chunkEnd+=len(line.encode('utf-8'))
      #chunkEnd = f.tell()
      yield chunkStart, chunkEnd - chunkStart
      if chunkEnd >= fileEnd:
        break

pool = mp.Pool(30)
jobs = []
num=0

for chunkStart, chunkSize in chunkify('news2016zh_train.json'):
  jobs.append(pool.apply_async(process_wrapper,args=(chunkStart,chunkSize,num)))
  num=num+1
fw=open('pcd_text.words',mode='a+')
for job in jobs:
	c,num=job.get()
	if c:
		fw.writelines(c)
		print('No. '+str(num)+'done.')
fw.close()

pool.close()
pool.join()
print('finished')