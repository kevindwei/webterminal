# -*- coding: utf-8 -*-
import paramiko
import socket
from channels.generic.websockets import WebsocketConsumer
try:
    import simplejson as json
except ImportError:
    import json
from webterminal.interactive import interactive_shell,get_redis_instance,SshTerminalThread,InterActiveShellThread
import sys
from django.utils.encoding import smart_unicode
from django.core.exceptions import ObjectDoesNotExist
from webterminal.models import ServerInfor,ServerGroup,CommandsSequence,SshLog
from webterminal.sudoterminal import ShellHandlerThread
import ast 
import time
from django.contrib.auth.models import User 
from django.utils.timezone import now
import os
from channels import Group

class webterminal(WebsocketConsumer):
    
    ssh = paramiko.SSHClient() 
    http_user = True
    http_user_and_session = True
    channel_session = True
    channel_session_user = True   

    
    def connect(self, message):
        self.message.reply_channel.send({"accept": True})     
        #permission auth
        self.message.reply_channel.send({"text":json.dumps(['channel_name',self.message.reply_channel.name])},immediately=True)
        
    def disconnect(self, message):
        #close threading
        self.closessh()
        
        self.message.reply_channel.send({"accept":False})
        
        audit_log=SshLog.objects.get(user=User.objects.get(username=self.message.user),channel=self.message.reply_channel.name)
        audit_log.is_finished = True
        audit_log.end_time = now()
        audit_log.save()
        self.close()
    
    def queue(self):
        queue = get_redis_instance()
        channel = queue.pubsub()
        return queue
    
    def closessh(self):
        #close threading
        self.queue().publish(self.message.reply_channel.name, json.dumps(['close']))
        
    def receive(self,text=None, bytes=None, **kwargs):
        try:
            if text:
                data = json.loads(text)
                begin_time = time.time()
                if data[0] == 'ip':
                    ip = data[1]
                    width = data[2]
                    height = data[3]
                    self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())#目的是接受不在本地Known_host文件下的主机
                    try:
                        data = ServerInfor.objects.get(ip=ip)#数据库没有这个ip就会报错
                        port = data.credential.port#端口
                        method = data.credential.method#认证方式
                        username = data.credential.username#服务器用户名
                        audit_log = SshLog.objects.create(user=User.objects.get(username=self.message.user),server=data,channel=self.message.reply_channel.name,width=width,height=height)
                        audit_log.save()#保存访问记录
                        if method == 'password':
                            password = data.credential.password
                        else:
                            key = data.credential.key
                    except ObjectDoesNotExist:
                        self.message.reply_channel.send({"text":json.dumps(['stdout','\033[1;3;31mConnect to server! Server ip doesn\'t exist!\033[0m'])},immediately=True)
                        self.message.reply_channel.send({"accept":False})                        
                    try:#连接服务器
                        if method == 'password':
                            self.ssh.connect(ip, port=port, username=username, password=password, timeout=3)
                        else:
                            self.ssh.connect(ip, port=port, username=username, key_filename=key, timeout=3)
                    except socket.timeout:#超时
                        self.message.reply_channel.send({"text":json.dumps(['stdout','\033[1;3;31mConnect to server time out\033[0m'])},immediately=True)
                        self.message.reply_channel.send({"accept":False})
                        return
                    except Exception:#无法连接服务器
                        self.message.reply_channel.send({"text":json.dumps(['stdout','\033[1;3;31mCan not connect to server\033[0m'])},immediately=True)
                        self.message.reply_channel.send({"accept":False})
                        return
                    
                    chan = self.ssh.invoke_shell(width=width, height=height,)#在SSH server端创建一个交互式的shell，且可以按自己的需求配置伪终端，可以在invoke_shell()函数中添加参数配置。
                    
                    #open a new threading to handle ssh to avoid global variable bug
                    sshterminal=SshTerminalThread(self.message,chan)
                    sshterminal.setDaemon = True#主线程退出不需要等子线程
                    sshterminal.start()     #运行shell
                    
                    directory_date_time = now()
                    log_name = os.path.join('{0}-{1}-{2}'.format(directory_date_time.year,directory_date_time.month,directory_date_time.day),'{0}.json'.format(audit_log.log))
                    
                    #interactive_shell(chan,self.message.reply_channel.name,log_name=log_name,width=width,height=height)
                    interactivessh = InterActiveShellThread(chan,self.message.reply_channel.name,log_name=log_name,width=width,height=height)
                    interactivessh.setDaemon = True
                    interactivessh.start()
                    
                elif data[0] in ['stdin','stdout']:
                    self.queue().publish(self.message.reply_channel.name, json.loads(text)[1])
                elif data[0] == u'set_size':
                    self.queue().publish(self.message.reply_channel.name, text)
                else:
                    self.message.reply_channel.send({"text":json.dumps(['stdout','\033[1;3;31mUnknow command found!\033[0m'])},immediately=True)
            elif bytes:
                self.queue().publish(self.message.reply_channel.name, json.loads(bytes)[1]) #发布
        except socket.error:
            audit_log=SshLog.objects.get(user=User.objects.get(username=self.message.user),channel=self.message.reply_channel.name)
            audit_log.is_finished = True
            audit_log.end_time = now()
            audit_log.save()
            self.closessh()
            self.close()
        except Exception,e:
            import traceback
            print traceback.print_exc()
            self.closessh()
            self.close()


class CommandExecute(WebsocketConsumer):
    http_user = True
    http_user_and_session = True
    channel_session = True
    channel_session_user = True   
    
    def connect(self, message):
        self.message.reply_channel.send({"accept": True})     
        #permission auth

    def disconnect(self, message):
        self.message.reply_channel.send({"accept":False})
        self.close()
    
    def receive(self,text=None, bytes=None, **kwargs):
        try:
            if text:
                data = json.loads(text)
                if isinstance(data,list):
                    return
                if data.has_key('parameter'):
                    parameter = data['parameter']
                    taskname = parameter.get('taskname',None)
                    groupname = parameter.get('groupname',None)
                    ip = parameter.get('ip',None)
                    if taskname and ip and groupname:
                        server_list = [ ip ]
                    elif taskname and not ip and not groupname:
                        server_list = []
                        [server_list.extend([ server.ip for server in ServerGroup.objects.get(name=group.name).servers.all() ]) for group in CommandsSequence.objects.get(name = taskname).group.all() ]
                    elif taskname and groupname and not ip:
                        server_list = [ server.ip for server in ServerGroup.objects.get(name=groupname).servers.all() ]
                    commands = json.loads(CommandsSequence.objects.get(name = taskname).commands)
                    if isinstance(commands,(basestring,str,unicode)):
                        commands = ast.literal_eval(commands)
                    
                    #Run commands 
                    commandshell = ShellHandlerThread(message=self.message,commands=commands,server_list=server_list)
                    commandshell.setDaemon = True
                    commandshell.start()
                        
                else:
                    #illegal
                    self.message.reply_channel.send({"text":json.dumps(['stdout','\033[1;3;31mIllegal parameter passed to the server!\033[0m'])},immediately=True)
                    self.close()
            if bytes:
                data = json.loads(bytes)
        except Exception,e:
            import traceback
            print traceback.print_exc()
            self.message.reply_channel.send({"text":json.dumps(['stdout','\033[1;3;31mSome error happend, Please report it to the administrator! Error info:%s \033[0m' %(smart_unicode(e)) ] )},immediately=True)
            
class SshTerminalMonitor(WebsocketConsumer):
    
    http_user = True
    http_user_and_session = True
    channel_session = True
    channel_session_user = True  
    
    
    def connect(self, message,channel):
        self.message.reply_channel.send({"accept": True})     
        #permission auth
        Group(channel).add(self.message.reply_channel.name)

    def disconnect(self, message,channel):
        Group(channel).discard(self.message.reply_channel.name)
        self.message.reply_channel.send({"accept":False})
        self.close()
    
    def receive(self,text=None, bytes=None, **kwargs):
        pass