from datetime import datetime
import os

class Logger:
    
    def __init__(self, conf_path, remote_host, is_server):
        self.__log_path = os.path.join(conf_path, 'logs.txt')
        self.__remote_host = remote_host
        self.__is_server = is_server

    def __stamp(self):
        host = 'SERVER' if self.__is_server else 'CLIENT'
        time = datetime.now()
        return f'[{time} {host} {self.__remote_host}]'

    def log(self, message):
        message = self.__stamp() + ' - ' + message + '\n'
        with open(self.__log_path, 'a+') as file:
            file.write(message)
