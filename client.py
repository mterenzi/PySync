import json
import socket
import ssl
import os
import shutil
from exceptions import *
from file_structure import File_Structure

# REQUEST STRUCT
# REQUEST PATH BYTES
# MKDIR PATH
# MKFILE PATH
# DELETE PATH

def send_struct(conn, struct):
    struct_bytes = struct.dump_structure().encode('UTF-8')
    struct_info = f'STRUCT {len(struct_bytes)}'
    b_struct_info = struct_info.encode('UTF-8') 
    conn.sendall(b_struct_info)
    data = conn.recv(1024)
    msg = data.decode('UTF-8')
    if msg == f'OK {struct_info}':
        conn.sendall(struct_bytes)
    else:
        raise MissSpeakException('STRUCT MissMatch')


def send_files(conn, path, struct):
    root = struct['root']
    abs_path = path.replace('.', root, 1)
    info = struct[abs_path]
    info['path'] = path
    byte_total = os.path.getsize(abs_path)
    info['bytes'] = byte_total
    info_stream = json.dumps(info).encode()
    conn.sendall(info_stream)

    data = conn.recv(1024)
    if data == f'OK {byte_total}'.encode():
        if byte_total > 0:
            _bytes = 2048
            bytes_read = 0
            with open(abs_path, 'rb') as f:
                while bytes_read < byte_total:
                    file_bytes = f.read(_bytes)
                    conn.sendall(file_bytes)
                    bytes_read += _bytes
    else:
        raise MissSpeakException('REQUEST File')


def get_directory(conn, msg, root):
    msg = msg.decode('UTF-8')
    msg_parts = msg.split(' ')
    last_mod = int(msg_parts[-1])
    dir_path = ' '.join(msg_parts[:-1]).replace('.', root, 1)
    os.makedirs(dir_path, exist_ok=True)
    os.utime(dir_path, (last_mod, last_mod))
    ack = 'OK MKDIR ' + msg
    conn.sendall(ack.encode())


def get_file(conn, msg, root):
    info = json.loads(msg)
    file_path = info['path'].replace('.', root, 1)
    with open(file_path, 'wb+') as file:
        ack = f"OK MKFILE {info['path']} {info['bytes']}"
        conn.sendall(ack.encode())

        total_bytes = int(info['bytes'])
        if total_bytes > 0:
            byte_count = 0
            while byte_count < total_bytes:
                data = conn.recv(2048)
                file.write(data)
                byte_count += len(data)
    last_mod = int(info['last_mod'])
    os.utime(file_path, (last_mod, last_mod))
    conn.sendall(b'OK')


def delete_down(conn, msg, root):
    path = msg.decode('UTF-8')
    abs_path = path.replace('.', root, 1)
    try:
        if os.path.isdir(path):
            shutil.rmtree(abs_path)
        else:
            os.remove(abs_path)
    except FileNotFoundError:
        pass
    except PermissionError:
        pass
    conn.sendall(b'OK')



def main(hostname, port, path):
    structure = File_Structure(path, gitignore=True)

    context = ssl.create_default_context()  # TODO Add cert stuff 
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # with context.wrap_socket(sock, server_hostname=hostname) as ssock:
        if True:
            ssock = sock
            ssock.connect((hostname, port))

            data = ssock.recv(1024)
            if data == b'REQUEST STRUCT':
                send_struct(ssock, structure)
            while data != b'BYE':
                data = ssock.recv(1024)
                if data[0:7] == b'REQUEST':
                    send_files(ssock, data[8:].decode('UTF-8'), structure.get_structure())
                elif data[0:5] == b'MKDIR':
                    get_directory(ssock, data[6:], structure.get_root())
                elif data[0:6] == b'MKFILE':
                    get_file(ssock, data[7:], structure.get_root())
                elif data[0:6] == b'DELETE':
                    delete_down(ssock, data[7:], structure.get_root())
            structure.update_structure()
            structure.save_structure()

            
if __name__ == "__main__":
    hostname = 'localhost'
    port = 1818
    path = '.\\folder2'
    main(hostname, port, path)