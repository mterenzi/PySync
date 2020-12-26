import socket
from server_thread import ServerThread
from file_structure import File_Structure
import ssl
import time
from threading import Thread


def update_struct(structure):
    global server_struct
    while True:
        structure.update_structure()
        structure.save_structure()
        server_struct = structure.get_structure().copy()
        time.sleep(5)


def server_start(conf):
    global server_struct
    
    socket.setdefaulttimeout(conf['timeout'])
    structure = File_Structure(
        conf['root'], conf['gitignore'], conf['purge_limit'])
    server_struct = structure.get_structure()
    if conf['encryption']:
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=conf['cert'], keyfile=conf['key'])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((conf['hostname'], conf['port']))
        sock.listen(5)
        threads = []
        struct_updater = Thread(target=update_struct, args=[structure],
                                daemon=True)
        struct_updater.start()
        try:
            while True:
                conn, addr = sock.accept()
                print(f'Connected to {addr}')
                if conf['encryption']:
                    conn = context.wrap_socket(conn, server_side=True)
                thread = ServerThread(conn, addr, server_struct.copy(), conf.copy())
                thread.start()
                threads.append(thread)

                _threads = threads.copy()
                for thread in threads:
                    thread.join(0)
                    if not thread.is_alive():
                        _threads.remove(thread)
                threads = _threads
        except KeyboardInterrupt:
            struct_updater.join(0)
            for thread in threads:
                thread.join(0)
