import socket
from server_thread import ServerThread
from file_structure import File_Structure
import ssl
import time
from threading import Thread
import threading


def update_struct(structure):
    """
    Updates file structure representation repeatedly.

    Args:
        structure (File_Structure): Representation of local file structure.
    """
    global server_struct
    while True:
        structure.update_structure()
        structure.save_structure()
        server_struct = structure.get_structure().copy()
        time.sleep(5)


def clean_locks():
    """
    Cleans thread lock list repeatedly for memory consumption reasons.
    """
    global thread_locks
    while True:
        keys = list(thread_locks.keys())
        for key in keys:
            if thread_locks[key][0] <= 0:
                del thread_locks[key]
        time.sleep(2)


def server_start(conf):
    """
    Starts server and spawns off server threads for new client connections.

    Args:
        conf (dict): Configuration dictionary.
    """
    global server_struct
    global thread_locks
    thread_locks = {}
    
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
        
        struct_updater = Thread(target=update_struct, args=[structure],
                                daemon=True, name='Structure_Updater')
        struct_updater.start()
        lock_cleaner = Thread(target=clean_locks, daemon=True, name='Lock_Cleaner')
        lock_cleaner.start()
        
        threads = []
        try:
            while True:
                try:
                    conn, addr = sock.accept()
                except socket.timeout:
                    continue
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
            lock_cleaner.join(0)
            for thread in threads:
                thread.join(0)
