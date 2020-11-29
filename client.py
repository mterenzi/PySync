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


def send_files(conn, path, root):
    file_path = path.replace('.', root, 1).replace('|', ' ')
    with open(file_path, 'rb') as f:
        file_bytes = f.read()
    info = f"{path.replace(' ', '|')} {len(file_bytes)}"
    conn.sendall(info.encode())
    if len(file_bytes) > 0:
        data = conn.recv(1024)
        if data == ('OK '+info).encode():
            conn.sendall(file_bytes)
        else:
            raise MissSpeakException('REQUEST MissMatch')


def get_directory(conn, msg, root):
    dir_path = msg[1].replace('.', root, 1).replace('|', ' ')
    last_mod = int(msg[2])
    os.makedirs(dir_path, exist_ok=True)
    os.utime(dir_path, (last_mod, last_mod))
    ack = 'OK ' + ' '.join(msg)
    conn.sendall(ack.encode())


def get_file(conn, msg, root):
    path = msg[1].replace('|', ' ')
    byte_total = int(msg[2])
    last_mod = int(msg[3])
    ack = 'OK ' + ' '.join(msg)
    conn.send(ack.encode())
    file_path = path.replace('.', root, 1)
    with open(file_path, 'wb+') as f:
        if byte_total > 0:
            byte_count = 0
            while byte_count < byte_total:
                data = conn.recv(2048)
                f.write(data)
                byte_count += len(data)
    os.utime(file_path, (last_mod, last_mod))
    conn.send(b'OK')


def delete_down(conn, msg, root):
    path = msg[1].replace('.', root, 1).replace('|', ' ')
    if os.path.isdir(path):
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            pass
    else:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
    ack = 'OK ' + ' '.join(msg)
    conn.sendall(ack.encode())



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
                msg = data.decode('UTF-8')
                msg = msg.split(' ')
                if msg[0] == 'REQUEST':
                    send_files(ssock, msg[1], structure.get_root())
                elif msg[0] == 'MKDIR':
                    get_directory(ssock, msg, structure.get_root())
                elif msg[0] == 'MKFILE':
                    get_file(ssock, msg, structure.get_root())
                elif msg[0] == 'DELETE':
                    delete_down(ssock, msg, structure.get_root())
            structure.save_structure()

            
if __name__ == "__main__":
    hostname = 'localhost'
    port = 1818
    path = '.\\folder2'
    main(hostname, port, path)