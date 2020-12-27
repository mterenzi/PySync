from datetime import datetime
import os

class Logger:
    """
    Handles logging for client and server.
    """
    
    def __init__(self, log_level, conf_path, remote_host, logging_limit, thread=None):
        """
        Configures logger with nesscary information for logging.

        Args:
            log_level (int): Log information level.
            conf_path (str): Path to configuration folder to save log
            remote_host (str): Hostname
            logging_limit (int): Maximum file size allowed for logging
            thread (str, optional): Name of server thread for stamp. Defaults to
            None.
        """
        self.__log_level = log_level
        self.__log_path = os.path.join(conf_path, 'logs.txt')
        self.__remote_host = remote_host
        self.__thread = thread
        self.__logging_limit = logging_limit

    def __stamp(self):
        """
        Generates log stamp for debugging.

        Returns:
            str: Log stamp
        """
        host = 'SERVER' if self.__thread is not None else 'CLIENT'
        time = datetime.now()
        stamp = f'[{time} {host} {self.__remote_host}]'
        if self.__thread is not None:
            stamp += f' {self.__thread}'
        return stamp

    def log(self, message, level):
        """
        Logs a message if the level if equal to or greater than the logging
        level.

        Args:
            message (str): Message to log
            level (int): log priority level
        """
        if level <= self.__log_level:
            self.__log(message)

    def __log(self, message):
        """
        Logs message to file.

        Args:
            message (str): Log
        """
        message = self.__stamp() + ' - ' + message + '\n'
        with open(self.__log_path, 'a+') as file:
            file.write(message)
        if self.__logging_limit != -1:
            self.__check_log_size()
        
    def __check_log_size(self):
        """
        Ensures log does not exceed log file size limit.
        """
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
