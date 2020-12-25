from datetime import datetime
import os

class Logger:
    
    def __init__(self, log_level, conf_path, remote_host, is_server, logging_limit):
        self.__log_level = log_level
        self.__log_path = os.path.join(conf_path, 'logs.txt')
        self.__remote_host = remote_host
        self.__is_server = is_server
        self.__logging_limit = logging_limit

    def __stamp(self):
        host = 'SERVER' if self.__is_server else 'CLIENT'
        time = datetime.now()
        return f'[{time} {host} {self.__remote_host}]'

    def log(self, message, level):
        if level <= self.__log_level:
            self.__log(message)

    def __log(self, message):
        message = self.__stamp() + ' - ' + message + '\n'
        with open(self.__log_path, 'a+') as file:
            file.write(message)
        if self.__logging_limit != -1:
            self.__check_log_size()
        
    def __check_log_size(self):
        file_size = os.path.getsize(self.__log_path)
        if file_size > self.__logging_limit:
            distance = file_size - self.__logging_limit - 1
            with open(self.__log_path, 'rb+') as log_file:
                log_file.seek(distance)
                newline_distance = log_file.read().find(b'\n')
                if newline_distance != -1:
                    distance += newline_distance
                log_file.seek(distance)
                log_file.write(log_file.read())
                    