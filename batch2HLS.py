#!/usr/bin/env python
#-*- coding:utf-8 -*-
# 
from qiniu import Auth, PersistentFop, BucketManager, urlsafe_base64_encode#, build_op, op_save, urlsafe_base64_encode
#from awscli import path
import os
import time
#import re

# TODO: Move the info into one specific configure file
access_key = ''
secret_key = ''
bucket_name = ''
saved_bucket_name = ''
pipeline = ''

q = Auth(access_key, secret_key)
bucket = BucketManager(q)



modellist = [0, 1, 2] # [240P, 240P + 480P, 240P + 480P + 720P]

# Case 1: By using default values, loop to list all video files in one bucket;
def list_allfiles(bucket_name, bucket, prefix="", limit=200):    
    rlist=[]
    marker = None
    eof = False
    while eof is False:
        ret, eof, info = bucket.list(bucket_name, prefix=prefix, marker=marker, limit=limit)
        marker = ret.get('marker', None)
        for item in ret['items']:
            rlist.append(item["key"])
            #print(item["key"])
    if eof is not True:
        print ("ERROR: Unexpected stop while transcoding!")
    return rlist

def transcoder(q, bucket, key_path, key_name, trns_model):
    #sufix = os.path.splitext(key_name)[1][1:]
    #assert(sufix == 'rmvb' or ...)
    key_name = os.path.splitext(key_name)[0]
    
    fops = ''
    if trns_model == 0 :
        fops = 'avthumb/m3u8/segtime/10/ab/32k/ar/44100/acodec/libfaac/r/15/vb/200k/vcodec/libx264/s/424x240/autoscale/1/stripmeta/0/noDomain/1'
        saveas_key = urlsafe_base64_encode(saved_bucket_name + ':' + key_path + key_name + '_240p.m3u8')      
        fops = fops+'|saveas/'+saveas_key
    elif trns_model == 1 :
        fops = 'avthumb/m3u8/segtime/10/ab/64k/ar/44100/acodec/libfaac/r/20/vb/600k/vcodec/libx264/s/848x480/autoscale/1/stripmeta/0/noDomain/1'
        saveas_key = urlsafe_base64_encode(saved_bucket_name + ':' + key_path + key_name + '_480p.m3u8')      
        fops = fops+'|saveas/'+saveas_key
    elif trns_model == 2 :
        fops = 'avthumb/m3u8/segtime/10/ab/96k/ar/44100/acodec/libfaac/r/25/vb/1200k/vcodec/libx264/s/1280x720/autoscale/1/stripmeta/0/noDomain/1'
        saveas_key = urlsafe_base64_encode(saved_bucket_name + ':' + key_path + key_name + '_720p.m3u8')     
        fops = fops+'|saveas/'+saveas_key
    else :
        print('ERROR: unexpected transcoding model!')
    #print(key_path + key_name + ' Transcoding to ' + saveas_key)
    """    
    pfop = PersistentFop(q, bucket, pipeline)
    ops = []
    ops.append(fops)
    ret, info = pfop.execute(key, ops, 1)
    #print(info)
    assert ret['persistentId'] is not None
    """
        
if __name__=="__main__":
    logfile = "trace_trns.log"
    fllog = open(logfile,"a")
    finishedkeysfile = "finishedkeylist.txt"
    fl = open(finishedkeysfile, "a+")

    if not fl or not fllog:
        print ("can't open the file %s or %s for writing log" % logfile, finishedkeysfile)
        exit() 
    
    finishedkeys = []       
    fl.seek(0)
    for line in fl.readlines() : 
        finishedkeys.append(line.replace("\n","")) 
    print(finishedkeys)
    #fl.close()
    
    trns_model = 2
    keys = list_allfiles(bucket_name, bucket)
    
    # Remove the finished tasks in finish_list.txt 
    default = "default_test"
    prefix = default
    path = default
    for key in keys :
        if key in finishedkeys :
            pass
        else :
            path = os.path.split(key)[0]
            name = os.path.split(key)[1]
            #TODO: assert the type of media like assert(name == 'rmvb' ...)
        
            if prefix != path :
                if prefix != default :
                    print("Success to create the tasks for transcoding one TV series and exit.")
                    fl.close()
                    fllog.close()
                    exit()
                prefix = path
                # re-select the transcoding mode for new TV series           
                # trns_model = 2 # three resolutions by default
            fl.write(key + '\n')
            
            fllog.write(time.strftime('%Y-%m-%d-%H:%M:%S',time.localtime(time.time())) + '---start the task of transcoding the key: ' + key + '\n')
            for model in modellist :
                if model <= trns_model :
                    transcoder(q, bucket, path, name, model)    
        
