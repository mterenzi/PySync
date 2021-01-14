import json
import socket
import ssl
import os
import shutil
import re
import stat
from tempfile import gettempdir
import zlib
import gzip
import uuid
from datetime import datetime
from logger import Logger
from exceptions import *
from file_structure import File_Structure
import time


class Client:
    """
    Client instance. Handles client sync operations.
    Call run() to begin connection and sync.
    """
    
    def __init__(self, struct, conf):
        """
        Creates client instance based on given configuration.
        Performs sync off given file structure.

        Args:
            struct (dict): File structure dictionary from File_Structure class.
            conf (dict): Configuration dictionary for client to operate.
        """
        self.__struct = struct
        self.__conf = conf
        self.__hostname = self.__conf.get('hostname')
        self.__port = self.__conf.get('port')
        self.__root = self.__conf.get('root')
        self.__cert = self.__conf.get('cert', None)
        self.__configure()
        self.__logger = Logger(self.__conf['logging'], self.__conf_path, self.__hostname, self.__conf['logging_limit'])
        self.__dir_mods = []
    
    def __configure(self):
        """
        Configures base variables for Client class before sync.
        """
        home_dir = os.path.expanduser('~')
        stripped_root = os.path.basename(os.path.normpath(self.__root))
        conf_path = os.path.join(home_dir, '.conf')
        conf_path = os.path.join(conf_path, 'pysync')
        self.__conf_path = os.path.join(conf_path, stripped_root)
        os.makedirs(self.__conf_path, exist_ok=True)
        self.__conf['MAC'] = ':'.join(re.findall('..', '%012x' % uuid.getnode())),
        if self.__conf['backup']:
            if self.__conf['backup_path'] == 'DEFAULT':
                self.__conf['backup_path'] = os.path.join(self.__conf_path, 'backups')
            os.makedirs(self.__conf['backup_path'], exist_ok=True)

    def run(self):
        """
        Starts the sync process for the client.
        """
        start_time = datetime.now()
        try:
            self.__conn = self.__connect()
            self.__sync_config()
            self.__process()
        except socket.timeout:
            self.__logger.log('Connection timeout with Server.', 1)
        except json.JSONDecodeError:
            self.__logger.log('Critical json decoding error', 1)
        except MissSpeakException:
            self.__logger.log('Critical miscommunication. Closing connection.', 1)
        finally:
            try:
                self.__conn.shutdown(socket.SHUT_RDWR)
                self.__conn.close()
            except:
                pass
            self.__logger.log('Connection with Server closed.', 2)
            if self.__conf['backup_limit'] is not None:
                self.__purge_backups()
            self.__timeshift_dirs()
            time_elapsed = datetime.now() - start_time
            self.__logger.log(f'Time elapsed {time_elapsed}.', 2)

    def __connect(self):
        """
        Connects to remote server.

        Returns:
            Socket: Socket connected to remote server.
        """
        self.__logger.log('Connecting to Server...', 2)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.__conf['encryption']:
            context = ssl.create_default_context()
            context.load_verify_locations(self.__cert)
            sock = context.wrap_socket(sock, server_hostname=self.__hostname)
        sock.connect((self.__hostname, self.__port))
        print(f'Connected to Server ({self.__hostname}, {self.__port})')
        self.__logger.log('Connected to Server.', 2)
        return sock

    def __sync_config(self):
        """
        Syncs configuration with Server to most agreeable preferences
        """
        self.__logger.log('Syncing configuration...', 2)
        conf_stream = json.dumps(self.__clean_config()).encode()
        self.__conn.sendall(conf_stream)
        data = self.__conn.recv(1024)
        self.__conf.update(json.loads(data))
        self.__conn.sendall(data)
        self.__logger.log('Sync configured.', 2)

    def __clean_config(self):
        """
        Cleans configuration of unnecessary or private data before
        being sent to remote server.

        Returns:
            dict: Cleaned configuration dictionary
        """
        return {
            'purge': self.__conf['purge'],
            'compression': self.__conf['compression'],
            'compression_min': self.__conf['compression_min'],
            'ram': self.__conf['ram'],
            'MAC': self.__conf['MAC'],
        }

    def __process(self):
        """
        Processes commands from remote server.
        """
        data = b'OPEN'
        while data != b'BYE':
            data = self.__conn.recv(1024)
            try:
                if data == b'REQUEST STRUCT':
                    self.__send_struct()
                elif data[0:7] == b'REQUEST':
                    self.__send_file(data[8:].decode('UTF-8'))
                elif data[0:5] == b'MKDIR':
                    self.__get_directory(data[6:])
                elif data[0:6] == b'MKFILE':
                    self.__get_file(data[7:])
                elif data[0:6] == b'DELETE':
                    self.__delete_down(data[7:])
                elif data[0:14] == b'CONFIRM DELETE':
                    self.__confirm_delete(data[15:])
                elif data != b'BYE':
                    self.__conn.sendall(b'RETRY')
            except SkipResponseException:
                pass

    def __send_struct(self):
        """
        Sends file structure to the server for comparrision.

        Raises:
            MissSpeakException: Raised if file structure acknowledgment fails.
        """
        self.__logger.log('Sending struct...', 2)
        struct_bytes = self.__struct.dump_structure().encode('UTF-8')
        if self.__conf['compression'] and len(struct_bytes) >= self.__conf['compression_min']:
            struct_bytes = zlib.compress(struct_bytes, level=self.__conf['compression'])
        struct_info = f'STRUCT {len(struct_bytes)}'
        b_struct_info = struct_info.encode('UTF-8')
        self.__conn.sendall(b_struct_info)
        data = self.__conn.recv(1024)
        msg = data.decode('UTF-8')
        if msg == f'OK {struct_info}':
            self.__conn.sendall(struct_bytes)
            self.__logger.log('Struct sent.', 2)
        else:
            self.__logger.log('Send structure error', 1)
            raise MissSpeakException('STRUCT MissMatch')

    def __send_file(self, path):
        """
        Sends file to remote server.

        Args:
            path (str): Local path of file to send.

        Raises:
            MissSpeakException: Raises if remote server does not correctly
            acknowledge amount of bytes to be sent.
        """
        abs_path = path.replace('.', self.__root, 1)
        info = self.__struct.get_structure()[abs_path]
        info['path'] = path
        byte_total = os.path.getsize(abs_path)
        compressed = False
        if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
            try:
                byte_total, abs_path = self.__compress(abs_path)
                compressed = True
            except PermissionError:
                self.__conn.sendall(b'!!SKIP!!SKIP!!')
                raise SkipResponseException()
        info['bytes'] = byte_total
        info_stream = json.dumps(info).encode()
        self.__conn.sendall(info_stream)

        data = self.__conn.recv(1024)
        if data == f'OK {byte_total}'.encode():
            if byte_total > 0:
                bytes_read = 0
                self.__logger.log(f'Sending file {path} {byte_total}...', 4)
                try:
                    with open(abs_path, 'rb') as f:
                        byte_chunk = min(self.__conf['ram'], byte_total)
                        while bytes_read < byte_total:
                            if byte_chunk != -1:
                                file_bytes = f.read(byte_chunk)
                            else:
                                file_bytes = f.read()
                            self.__conn.sendall(file_bytes)
                            bytes_read += byte_chunk
                except PermissionError:
                    self.__conn.sendall(b'!!SKIP!!SKIP!!')
                    self.__logger.log('Permssion error encountered reading file'
                                        , 1)
                    raise SkipResponseException('REQUEST File')
                if compressed:
                    try:
                        os.remove(abs_path)
                    except PermissionError:
                        self.__logger.log('Permssion error encountered deleting'
                                            + ' compressed file', 1)
        else:
            self.__logger.log('File send error', 1)
            raise MissSpeakException('REQUEST File')

    def __get_directory(self, msg):
        """
        Creates directory specified by remote server.

        Args:
            msg (bytes): Directory creation command message.
        """
        msg = msg.decode('UTF-8')
        msg_parts = msg.split(' ')
        last_mod = int(msg_parts[-1])
        dir_path = ' '.join(msg_parts[:-1])
        abs_path = dir_path.replace('.', self.__root, 1)
        try:
            os.makedirs(abs_path, exist_ok=True)
            os.utime(abs_path, (last_mod, last_mod))
        except PermissionError:
            self.__logger.log('Permission error encountered creating directory '
                            + abs_path, 1)
        self.__dir_mods.append((abs_path, (last_mod, last_mod)))
        ack = 'OK MKDIR ' + msg
        self.__conn.sendall(ack.encode())
        self.__logger.log(f'Recieved directory {dir_path}', 4)

    def __get_file(self, msg):
        """
        Downloads or creates file specified by remote server.

        Args:
            msg (bytes): File creation command message.
        """
        info = json.loads(msg)
        path = info['path']
        abs_path = path.replace('.', self.__root, 1)
        ack = f"OK MKFILE {info['path']} {info['bytes']}"
        self.__conn.sendall(ack.encode())

        byte_total = int(info['bytes'])
        self.__logger.log(f'Receiving file {path} {byte_total}...', 4)
        if byte_total > 0:
            try:
                if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
                    self.__recv_compressed_file(abs_path, byte_total)
                else:
                    self.__recv_file(abs_path, byte_total)
            except PermissionError:
                self.__conn.sendall(b'!!SKIP!!SKIP!!')
                self.__logger.log('Permssion error encountered receiving file', 
                                    1)
                raise SkipResponseException('Get file')
        else:
            try:
                open(abs_path, 'wb+').close()
            except PermissionError:
                raise SkipResponseException('Get file')
        last_mod = int(info['last_mod'])
        os.utime(abs_path, (last_mod, last_mod))
        self.__conn.sendall(b'OK')

    def __recv_file(self, abs_path, byte_total):
        """
        Handles file download from remote server.

        Args:
            abs_path (str): Absolute path to place downloaded file.
            byte_total (int): Total number of bytes to download.
        """
        with open(abs_path, 'wb+') as file:
            byte_count = 0
            byte_chunk = min(self.__conf['ram'], byte_total)
            skip_cache = b''
            while byte_count < byte_total:
                if byte_chunk != -1:
                    data = self.__conn.recv(byte_chunk)
                else:
                    data = self.__conn.recv()
                if b'!!SKIP!!SKIP!!' in skip_cache+data:
                    raise SkipResponseException('Recv file')
                file.write(data)
                byte_count += len(data)
                skip_cache = data[-14:]
    
    def __recv_compressed_file(self, abs_path, byte_total):
        """
        Handles compressed file download from remote server.

        Args:
            abs_path (str): Absolute path to place downloaded file.
            byte_total (int): Total number of bytes to download.
        """
        file_name = f'{datetime.now().microsecond}_' + os.path.basename(os.path.normpath(abs_path)) + '.gz'
        z_path = os.path.join(gettempdir(), file_name)
        with open(z_path, 'wb+') as zip_file:
            byte_count = 0
            byte_chunk = min(self.__conf['ram'], byte_total)
            skip_cache = b''
            while byte_count < byte_total:
                if byte_chunk != -1:
                    data = self.__conn.recv(byte_chunk)
                else:
                    data = self.__conn.recv()
                if b'!!SKIP!!SKIP!!' in skip_cache+data:
                    raise SkipResponseException('Recv file')
                zip_file.write(data)
                byte_count += len(data)
                skip_cache = data[-14:]
        with gzip.open(z_path, 'rb+') as zip_file:
            with open(abs_path, 'wb+') as file:
                shutil.copyfileobj(zip_file, file)

    def __delete_down(self, msg):
        """
        Handles deletion command from remote server.

        Args:
            msg (bytes): Deletion command from remote server.
        """
        path = msg.decode('UTF-8')
        abs_path = path.replace('.', self.__root, 1)
        try:
            self.__logger.log(f'Deleting {path}...', 3)
            if not self.__conf['backup']:
                if os.path.isdir(path):
                    shutil.rmtree(abs_path)
                else:
                    os.remove(abs_path)
            else:
                backup_path = path.replace('.', self.__conf['backup_path'], 1)
                shutil.move(abs_path, backup_path)
        except FileNotFoundError:
            pass
        except PermissionError:
            pass
        self.__conn.sendall(b'OK')

    def __confirm_delete(self, path):
        """
        Confirms deletion of file for remote server.

        Args:
            path (str): Path of file to confirm for deletion.
        """
        path = path.decode('UTF-8')
        abs_path = path.replace('.', self.__root, 1)
        if not os.path.exists(abs_path):
            ack = f'OK {path}'.encode()
            self.__conn.sendall(ack)
            self.__logger.log(f'Delete {path} confirmed.', 3)
        else:
            deny = f'NO {path}'.encode()
            self.__conn.sendall(deny)
            self.__logger.log(f'Delete {path} denied.', 3)

    def __purge_backups(self):
        """
        Removes old backups.
        """
        self.__logger.log('Cleaning backups...', 2)
        day_limit = self.__conf['backup_limit']
        now_time = datetime.now()
        for _, dirs, files in os.walk(self.__conf['backup_path'], topdown=True):
            for _dir in dirs:
                status = os.stat(_dir)
                last_mod = datetime.fromtimestamp(status[stat.ST_MTIME])
                delta = now_time - last_mod
                if delta.days >= day_limit:
                    try:
                        shutil.rmtree(_dir)
                    except IOError:
                        continue
            for file in files:
                status = os.stat(file)
                last_mod = datetime.fromtimestamp(status[stat.ST_MTIME])
                delta = now_time - last_mod
                if delta.days >= day_limit:
                    try:
                        os.remove(file)
                    except IOError:
                        continue
        self.__logger.log('Backups cleaned...', 2)

    def __compress(self, path):
        """
        Compresses file for upload.

        Args:
            path (str): Path to file for compression

        Returns:
            tuple(int, str): Tuple containing size of compressed file and its
            path.
        """
        self.__logger.log(f'Compressing {path}...', 4)
        file_name = f'{datetime.now().microsecond}_' + os.path.basename(os.path.normpath(path)) + '.gz'
        with open(path, 'rb') as f_in:
            z_path = os.path.join(os.path.join(gettempdir(), 'pysync'), file_name)
            with gzip.open(z_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return os.path.getsize(z_path), z_path
    
    def __timeshift_dirs(self):
        """
        Sets directories to proper last modification time after files are
        created and removed within.
        """
        for abs_path, mod in self.__dir_mods:
            os.utime(abs_path, mod)


def client_start(conf):
    """
    Schedules Client sync with remote server.

    Args:
        conf (dict): Configuration dictionary for Client.
    """
    socket.setdefaulttimeout(conf['timeout'])
    structure = File_Structure(conf['root'], conf['gitignore'], conf['purge_limit'])
    tries = 0
    while True:
        structure.update_structure()
        structure.save_structure()
        try:
            client = Client(structure, conf.copy())
            client.run()
            tries = 0
        except ConnectionRefusedError:
            seconds = 30*tries
            print(f'Connection refused. Sleeping for {seconds} seconds')
            time.sleep(min(seconds, 900))
            tries += 1
            continue
        except ConnectionResetError:
            pass

        if conf['sleep_time'] == -1:
            break
        time.sleep(conf['sleep_time'])