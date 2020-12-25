import argparse
import os
import json
from server import server_start
from client import client_start


def parse_args():
    description = 'Synchronizes files and directories between a client and a server.'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--config', type=str, default=None,
                        help='Path to a configuration file. A valid configuration file will enable silent mode.')
    parser.add_argument('--root', type=str, default=None,
                        help='Path of directory to Synchronize')
    parser.add_argument('--server', type=bool, default=None,
                        help='This is a server instance.')
    parser.add_argument('--client', type=bool, default=None,
                        help='This is a client instance.')
    parser.add_argument('--hostname', type=str, default=None,
                        help='Hostname to connect to or broadcast as.')
    parser.add_argument('--port', type=int, default=None,
                        help='Port to connect to or bind to.')
    parser.add_argument('--encryption', type=bool,
                        default=None, help='Use TLS or not.')
    parser.add_argument('--cert', type=str, default=None,
                        help='Certificate file path for TLS handshake.')
    parser.add_argument('--key', type=str, default=None,
                        help='Key file path for TLS handshake.')
    parser.add_argument('--purge', type=bool, default=None,
                        help='Deletions are included in Syncs.')
    parser.add_argument('--purge_limit', type=int, default=None,
                        help='How long, in days, deleted items are still monitored before being forgotten.')
    parser.add_argument('--backup', type=bool, default=None,
                        help='Deletions are stored in backup location.')
    parser.add_argument('--backup_path', type=str, default=None,
                        help='Path to place backup files within.\nDefaults to ~/conf/pysync/{root}/backups')
    parser.add_argument('--backup_limit', type=int, default=None,
                        help='Length of time files are held in backup location. (days)')
    parser.add_argument('--ram', type=int, default=None,
                        help='Maximum amount of RAM to use for Syncs. (Bytes)\n-1 for unlimited.')
    parser.add_argument('--compression', type=bool, default=None,
                        help='Compress large files before Synchronization.')
    parser.add_argument('--compression_min', type=int, default=None,
                        help='Minimum file size before compression is applied. (Bytes)')
    logging_help = 'Information will be kept in log file.\n0 - Nothing logged\
        \n1 - Only Errors are logged\n2 - Errors and Summary activity are logged\n3 - \
        Errors, Summary Activity, and Deletions are logged.\n4 - Nearly all activity is logged.'
    parser.add_argument('--logging', type=int, default=None, help=logging_help)
    parser.add_argument('--logging_limit', type=int, default=None,
                        help='Maximum size limit of log file. (Bytes)\n-1 for unlimited.')
    parser.add_argument('--gitignore', type=bool, default=None,
                        help='Read and exclude items from children gitignores in the sync.')
    return parser.parse_args()


def print_intro():
    print('Welcome to PySync')
    print('Version 0.1')
    print('Created by Maximilian Terenzi')
    print()


def check_for_config(conf, confs_path):
    if yes_no('Is there a configuration file you would like to load?'):
        options = os.listdir(confs_path)
        if len(options) > 0:
            options.append('Specify a Path')
            option = ask_options(
                'Pick a configuration file', options, title=False)
            if option == 'Specify a Path':
                conf['config'] = ask_path(
                    'Enter the path for the configuration file')
            else:
                conf['config'] = os.path.join(confs_path, option)
        else:
            conf['config'] = ask_path(
                'Enter the path for the configuration file')
        conf = get_config_file(conf)
    conf.pop('config')
    return conf


def configure(conf):
    unit_prompt = '\nUnits:\nUnit\t-\tExample\nGB\t-\t10GB\nMB\t-\t10MB\nKB\t-\t10KB\nB\t-\t10'
    units = [
        ('GB', lambda x: int(x * 1e9)),
        ('MB', lambda x: int(x * 1e6)),
        ('KB', lambda x: int(x * 1e3)),
        ('B', lambda x: int(x)),
    ]
    if conf.get('root', None) is None:
        conf['root'] = simple_response(
            'What is the path of the directory you wish to sync?')
    conf['root'] = os.path.abspath(conf['root'])

    conf = configure_handshake(conf)
    conf = configure_deletes(conf)
    conf = configure_limits(conf, unit_prompt, units)
    conf = configure_logging(conf, unit_prompt, units)
    conf = configure_misc(conf)
    return conf


def configure_handshake(conf):
    print()
    if conf.get('server', None) is None and conf.get('client', None) is None:
        host = ask_options('Is this the Server or Client?', ['Server', 'Client'])
        if host == 'Server':
            conf['server'] = True
            conf['client'] = False
        else:
            conf['client'] = True
            conf['server'] = False
    if conf.get('hostname', None) is None:
        if conf['server']:
            conf['hostname'] = simple_response('What is your hostname?')
        else:
            conf['hostname'] = simple_response(
                'What is the hostname that you are connecting to?')
    if conf.get('port', None) is None:
        if conf['server']:
            conf['port'] = numeric_response('What port do you want to use?')
        else:
            conf['port'] = numeric_response(
                'What port on the host are you connecting to?')
    if conf.get('encryption', None) is None:
        conf['encryption'] = yes_no('Would you like to use TLS encryption?')
    if conf['encryption'] and conf.get('cert', None) is None:
        conf['cert'] = ask_path('Enter the path for the certificate file')
    if conf['encryption'] and conf['server'] and conf.get('key', None) is None:
        conf['key'] = ask_path('Enter the path for the key file')
    return conf


def configure_deletes(conf):
    print()
    if conf.get('purge', None) is None:
        conf['purge'] = yes_no(
            'Would you like the sync to be able to delete files between devices?')
    if conf['purge'] and conf.get('purge_limit') is None:
        conf['purge_limit'] = numeric_response(
            'How long, in days, should deleted items still be monitored before being forgotten?')
    if conf['purge'] and conf.get('backup', None) is None:
        conf['backup'] = yes_no('Would you like to backup deleted files?')
    if conf['backup'] and conf.get('backup_path', None) is None:
        if yes_no('Would you like to set a specific backup path?'):
            prompt = 'Provide a path for the backups'
            conf['backup_path'] = simple_response(prompt)
        else:
            conf['backup_path'] = 'DEFAULT'
    if conf['backup'] and conf.get('backup_limit', None) is None:
        prompt = 'How long, in days, would you like to keep backed up files? (-1 to never delete)'
        conf['backup_limit'] = numeric_response(prompt)
    return conf


def configure_limits(conf, unit_prompt, units):
    print()
    if conf.get('ram', None) is None:
        if conf['server']:
            prompt = 'How much RAM would you like the Sync to use per thread?'
        else:
            prompt = 'How much RAM would you like the Sync to use?'
        prompt += unit_prompt + '\nEnter -1 for unlimited.'
        conf['ram'] = numeric_response(prompt, units)
    if conf.get('compression', None) is None:
        conf['compression'] = yes_no(
            'Would you like to compress large files before transmission?')
    if conf['compression'] and conf.get('compression_min', None) is None:
        prompt = 'What is the minimum file sized that can be compressed?' + unit_prompt
        conf['compression_min'] = numeric_response(prompt, units)
    return conf


def configure_logging(conf, unit_prompt, units):
    print()
    if conf.get('logging', None) is None:
        prompt = 'Would you like to log information?'
        options = ['Nothing Logged', 'Errors Only', 'Errors and Summary Activity',
                   'Errors, Summary Activity, and Deletions', 'Nearly all Activity']
        conf['logging'] = options.index(ask_options(prompt, options))
    if conf['logging'] > 0 and conf.get('logging_limit', None) is None:
        prompt = 'What is the maximum file size of the log file?' + \
            unit_prompt + '\nEnter -1 for unlimited.'
        conf['logging_limit'] = numeric_response(prompt, units)
    return conf


def configure_misc(conf):
    if conf.get('gitignore', None) is None:
        conf['gitignore'] = yes_no(
            'Would you like items from children gitignores to be excluded from the sync?')
    if conf['client'] and conf.get('sleep_time', None) is None:
        prompt = 'How long, in seconds, would you like the client to sleep before re-syncing? Enter -1 for single use.'
        conf['sleep_time'] = numeric_response(prompt)
    return conf


def ask_options(prompt, options, confirm=True, title=True):
    print(prompt + ':')
    for idx, option in enumerate(options):
        if title:
            option = option.title()
        print(f'{idx+1} - {option}')
    hint = f'Pick an option (1-{len(options)}): '
    option = input(hint)
    try:
        option = int(option)
        try:
            if option < 1:
                raise IndexError
            option = options[option-1]
            if confirm:
                print(f'User selected: {option}')
            return option
        except IndexError:
            print(f'Invalid option. Must be between 1 and {len(options)}')
            return ask_options(prompt, options, confirm)
    except ValueError:
        print('Invalid option. Must be integer.')
        return ask_options(prompt, options, confirm)


def simple_response(prompt):
    response = input(prompt + ': ')
    if response != '':
        return response
    else:
        print('Please enter a valid response')
        return simple_response(prompt)


def yes_no(prompt):
    response = input(prompt + ' (y/n): ')
    if response.lower() == 'y':
        return True
    elif response.lower() == 'n':
        return False
    else:
        print('Please enter \'y\' or \'n\' as a valid response.')
        return yes_no(prompt)


def numeric_response(prompt, units=[], num_type=int):
    response = input(prompt + ': ')
    if response == '':
        print('Please enter a response.')
        return numeric_response(prompt, units)
    if len(units) > 0:
        units.sort(key=len, reverse=True)
        for unit, callback in units:
            _slice = len(unit) * -1
            if response[_slice:].upper() == unit.upper():
                try:
                    response = num_type(response[:_slice])
                    return callback(response)
                except ValueError:
                    print('Number must be an integer.')
                    return numeric_response(prompt, units)
    try:
        return num_type(response)
    except ValueError:
        print('Number must be an integer or a unit was incorrectly entered.')
        return numeric_response(prompt, units)


def ask_path(prompt):
    response = simple_response(prompt)
    if os.path.exists(response):
        return response
    else:
        print('That path does not exist. Try again.')
        return ask_path(prompt)


def confirm_conf(conf):
    print()
    print('Your configuration:')
    for key, value in conf.items():
        print(f'{key.title()}\t-\t{value}')
    if not yes_no('Is this correct?'):
        key = ask_options('Which would you like to change?', list(conf.keys()))
        conf[key] = None
        return conf, False
    return conf, True


def save_config(conf, path):
    print()
    if yes_no('Would you like to save your configuration?'):
        name = simple_response(
            'What would you like to name your configuration?')
        if yes_no('Would you like to specify where to save your configuration?'):
            path = simple_response(
                'Please enter the path you would like your configuration saved to')
        file_path = os.path.join(path, name)
        if file_path.find('.json') == -1:
            file_path += '.json'
        with open(file_path, 'w+') as f:
            json.dump(conf, f, indent=4)


def get_config_file(conf):
    with open(conf['config'], 'r') as f:
        saved_conf = json.load(f)
    for key, value in conf.items():
        if value is not None:
            saved_conf[key] = value
    return saved_conf


def main(_conf=None):
    if _conf is None:
        conf = vars(parse_args())
    else:
        conf = _conf

    home_dir = os.path.expanduser('~')
    home_conf_path = os.path.join(home_dir, '.conf')
    pysync_path = os.path.join(home_conf_path, 'pysync')
    confs_path = os.path.join(pysync_path, 'configs')
    os.makedirs(confs_path, exist_ok=True)

    if conf.get('config', None) is None:
        print_intro()
        conf = check_for_config(conf, confs_path)
        print()
        while True:
            conf = configure(conf)
            conf, done = confirm_conf(conf)
            if done:
                break
        save_config(conf, confs_path)
    else:
        if not os.path.exists(conf['config']):
            test_path = os.path.join(confs_path, conf['config'])
            if not os.path.exists(test_path):
                test_path += '.json'
                if not os.path.exists(test_path):
                    print('The configuration file specified does not exist!')
                    return
            conf['config'] = test_path
        conf = get_config_file(conf)
        _conf = configure(conf.copy())
        if conf != _conf:
            save_config(_conf, confs_path)
        conf = _conf
    if conf['server']:
        return server_start(conf)
    elif conf['client']:
        return client_start(conf)


if __name__ == "__main__":
    main()
