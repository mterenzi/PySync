import argparse
import os
import json
from server import server_start
from client import client_start


def parse_args():
    """
    Parses command line arguments

    Returns:
        Namespace: Namespace of arguments.
    """
    description = 'Synchronizes files and directories between a client and a server.'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--config', type=str, default=None,
                        help='Path to a configuration file. A valid configuration file will enable silent mode.')
    parser.add_argument('--root', type=str, default=None,
                        help='Path of directory to Synchronize')
    parser.add_argument('--host', type=str, default=None,
                        help='Server or Client')
    parser.add_argument('--hostname', type=str, default=None,
                        help='Hostname to connect to or broadcast as.')
    parser.add_argument('--port', type=int, default=None,
                        help='Port to connect to or bind to.')
    parser.add_argument('--timeout', type=int, default=None,
                        help='How long the connect will hang before timeing out.')
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
    parser.add_argument('--compression', type=int, default=None,
                        help='Compression level to use on large files. Follows'\
                        + ' the zlib compression levels. 0 is no compression'\
                        + ' and 9 is most compression.')
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
    """
    Welcome message
    """
    print('Welcome to PySync')
    print('Version 0.1')
    print('Created by Maximilian Terenzi')
    print()


def check_for_config(conf, confs_path):
    """
    Checks if configuration file should be loaded.

    Args:
        conf (dict): Configuration dictionary
        confs_path (str): Default path configuration files

    Returns:
        dict: Configuration file
    """
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
        conf, _ = confirm_conf(conf)
    conf.pop('config')
    return conf


def configure(conf):
    """
    Checks configuration for missing values and prompts for values.

    Args:
        conf (dict): Configuration dictionary

    Returns:
        dict: Configuration dictionary
    """
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
    """
    Checks configuration for missing handshake values and prompts for values.

    Args:
        conf (dict): Configuration dictionary

    Returns:
        dict: Configuration dictionary
    """
    print()
    if conf.get('host', None) is None or conf['host'] not in ['Server', 'Client']:
        conf['host'] = ask_options('Is this the Server or Client?',
                                    ['Server', 'Client']).title()
    if conf.get('hostname', None) is None:
        if conf['host'] == 'Server':
            conf['hostname'] = simple_response('What is your hostname?')
        else:
            conf['hostname'] = simple_response(
                'What is the hostname that you are connecting to?')
    if conf.get('port', None) is None:
        if conf['host'] == 'Server':
            conf['port'] = numeric_response('What port do you want to use?')
        else:
            conf['port'] = numeric_response(
                'What port on the host are you connecting to?')
    if conf.get('timeout', None) is None:
        conf['timeout'] = numeric_response('How long, in seconds, can a connection hang before timing out?',
                                           default=30)
    if conf.get('encryption', None) is None:
        conf['encryption'] = yes_no(
            'Would you like to use TLS encryption?', default=False)
    if conf['encryption'] and conf.get('cert', None) is None:
        conf['cert'] = ask_path('Enter the path for the certificate file')
    if conf['encryption'] and conf['host'] == 'Server' \
        and conf.get('key', None) is None:
        conf['key'] = ask_path('Enter the path for the key file')
    return conf


def configure_deletes(conf):
    """
    Checks configuration for missing deletion related values and prompts
    for values.

    Args:
        conf (dict): Configuration dictionary

    Returns:
        dict: Configuration dictionary
    """
    print()
    if conf.get('purge', None) is None:
        conf['purge'] = yes_no(
            'Would you like the sync to be able to delete files between devices?', default=False)
    if conf['purge'] and conf.get('purge_limit') is None:
        conf['purge_limit'] = numeric_response(
            'How long, in days, should deleted items still be monitored before being forgotten?', default=7)
    if conf['purge'] and conf.get('backup', None) is None:
        conf['backup'] = yes_no(
            'Would you like to backup deleted files?', default=False)
    if conf['backup'] and conf.get('backup_path', None) is None:
        prompt = 'Provide a path for the backups'
        conf['backup_path'] = simple_response(prompt, default='DEFAULT')
    if conf['backup'] and conf.get('backup_limit', None) is None:
        prompt = 'How long, in days, would you like to keep backed up files? (-1 to never delete)'
        conf['backup_limit'] = numeric_response(prompt, default=7)
    return conf


def configure_limits(conf, unit_prompt, units):
    """
    Checks configuration for missing performance limitations related values and
    prompts for values.

    Args:
        conf (dict): Configuration dictionary

    Returns:
        dict: Configuration dictionary
    """
    print()
    if conf.get('ram', None) is None:
        if conf['host'] == 'Server':
            prompt = 'How much RAM would you like the Sync to use per thread?'
        else:
            prompt = 'How much RAM would you like the Sync to use?'
        prompt += unit_prompt + '\nEnter -1 for unlimited.'
        conf['ram'] = numeric_response(prompt, units, default='1MB')
    if conf.get('compression', None) is None:
        conf['compression'] = ask_range(
            prompt='How much would you like to compress large files?', 
            min=0, max=9, tips=['No Compression', 'Max Compression'], default=0)
    if conf['compression'] and conf.get('compression_min', None) is None:
        prompt = 'What is the minimum file sized that can be compressed?' + unit_prompt
        conf['compression_min'] = numeric_response(prompt, units, default=70)
    return conf


def configure_logging(conf, unit_prompt, units):
    """
    Checks configuration for missing logging related values and prompts
    for values.

    Args:
        conf (dict): Configuration dictionary

    Returns:
        dict: Configuration dictionary
    """
    print()
    if conf.get('logging', None) is None:
        prompt = 'Would you like to log information?'
        options = ['Nothing Logged', 'Errors Only', 'Errors and Summary Activity',
                    'Errors, Summary Activity, and Deletions', 'Nearly all Activity']
        conf['logging'] = options.index(ask_options(
            prompt, options, default='Nothing Logged'))
    if conf['logging'] > 0 and conf.get('logging_limit', None) is None:
        prompt = 'What is the maximum file size of the log file?' + \
            unit_prompt + '\nEnter -1 for unlimited.'
        conf['logging_limit'] = numeric_response(prompt, units, default='10MB')
    return conf


def configure_misc(conf):
    """
    Checks configuration for missing miscellaneous values and prompts
    for values.

    Args:
        conf (dict): Configuration dictionary

    Returns:
        dict: Configuration dictionary
    """
    if conf.get('gitignore', None) is None:
        conf['gitignore'] = yes_no(
            'Would you like items from children gitignores to be excluded from the sync?', default=False)
    if conf['host'] == 'Client' and conf.get('sleep_time', None) is None:
        prompt = 'How long, in seconds, would you like the client to sleep before re-syncing? Enter -1 for single use.'
        conf['sleep_time'] = numeric_response(prompt, default=-1)
    return conf


def ask_options(prompt, options, confirm=True, title=True, default=None, hints=None):
    """
    Presents options for response from user. Response is checked and returned.

    Args:
        prompt (str): Question prompt.
        options (list): List of options to display.
        confirm (bool, optional): Echos user selections. Defaults to True.
        title (bool, optional): Capitalize first letter of each option. Defaults to True.
        default (object, optional): Default input value presented. Must be
        contained in the options parameter. Defaults to None.
        hints (list): List of hints to be displayed alongside choices.

    Raises:
        IndexError: Default value is not in options parameter.

    Returns:
        object: Returns user selection from options.
    """
    print(prompt + ':')
    for idx, option in enumerate(options):
        if title:
            option = str(option).title()
        if hints is None:
            print(f'{idx+1} - {option}')
        else:
            try:
                print(f'{idx+1} - {option}: {hints[idx]}')
            except IndexError:
                print(f'{idx+1} - {option}')
    if default is None:
        hint = f'Pick an option (1-{len(options)}): '
    else:
        hint = f'Pick an option (1-{len(options)}) [{options.index(default)+1}]: '
    option = input(hint)
    if option == '' and default is not None:
        return default
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
            return ask_options(prompt, options, confirm, title, default)
    except ValueError:
        print('Invalid option. Must be integer.')
        return ask_options(prompt, options, confirm, title, default)


def simple_response(prompt, default=None):
    """
    Presents prompt and returns response.

    Args:
        prompt (str): Question to present.
        default (obj, optional): Value to present as default. Defaults to None.

    Returns:
        obj: Response string or default object.
    """
    if default is None:
        response = input(prompt + ': ')
    else:
        response = input(prompt + f' [{default}]' + ': ')
    if response != '':
        return response
    elif response == '' and default is not None:
        return default
    else:
        print('Please enter a valid response')
        return simple_response(prompt, default)


def yes_no(prompt, default=None):
    """
    Presents yes or no question and returns response.

    Args:
        prompt (str): Question to be presented.
        default (bool, optional): Default value to be presented. Defaults to None.

    Raises:
        KeyError: Default value was not boolean.

    Returns:
        obj: User input string or default value.
    """
    if default is None:
        response = input(prompt + ' (y/n): ')
    elif default:
        response = input(prompt + ' ([y]/n): ')
    elif not default:
        response = input(prompt + ' (y/[n]): ')
    else:
        raise KeyError('Default must be True or False')
    if response.lower() == 'y':
        return True
    elif response.lower() == 'n':
        return False
    elif response == '' and default is not None:
        return default
    else:
        print('Please enter \'y\' or \'n\' as a valid response.')
        return yes_no(prompt, default)


def numeric_response(prompt, units=[], num_type=int, default=None):
    """
    Presents question that requires a numeric response.

    Args:
        prompt (str): Question to present.
        units (list, optional): Units to evaluate answer with. List of tuples.
        (symbol, func) Defaults to [].
        num_type (type, optional): Variable type to cast response to. Defaults to int.
        default (obj, optional): Default value to present. Defaults to None.

    Returns:
        obj: User or default response casted to num_type parameter.
    """
    if default is None:
        response = input(prompt + ': ')
    else:
        response = input(prompt + f' [{default}]' + ': ')
    try:
        if response == '' and default is not None:
            return standardize_response(default, units, num_type)
        elif response == '':
            print('Please enter a response.')
            return numeric_response(prompt, units, num_type, default)
        return standardize_response(response, units, num_type)
    except ValueError:
        print('Number must be an integer or a unit was incorrectly entered.')
        return numeric_response(prompt, units, num_type, default)


def standardize_response(response, units, num_type):
    """
    Standardize response containing units to base unit.

    Args:
        response (str): User inputted response.
        units (list[Tuple(str, func)]): List of tuples containing unit symbol
        and conversion function.
        num_type (type): Type to cast response into.

    Returns:
        type: Response value casted to num_type parameter.
    """
    if len(units) > 0:
        response = str(response)
        units.sort(key=len, reverse=True)
        for unit, callback in units:
            _slice = len(unit) * -1
            if response[_slice:].upper() == unit.upper():
                response = num_type(response[:_slice])
                return callback(response)
        return num_type(response)
    else:
        return num_type(response)


def ask_path(prompt, default=None):
    """
    Presents question which requires a response that is a valid path.

    Args:
        prompt (str): Question to present.
        default (str, optional): Default path to presented. Defaults to None.

    Returns:
        str: User response or default path.
    """
    response = simple_response(prompt, default)
    if os.path.exists(response):
        return response
    else:
        print('That path does not exist. Try again.')
        return ask_path(prompt, default)


def ask_range(prompt, min, max, tips=[], default=None):
    """
    Prompts user with a range of values to choose from. Formats differently
    depending on tips.

    Args:
        prompt (str): Question to present.
        min (int): Minimum integer value.
        max (int): Maximum integer value.
        tips (list, optional): List of tips to display.. Defaults to [].
        default (int, optional): Default option to display. Defaults to None.

    Raises:
        KeyError: Raises if default is not in range.

    Returns:
        int: Integer response
    """
    print(prompt + ':')
    keys = [i for i in range(min, max+1)]
    if default is not None and default not in keys:
        raise KeyError('Default value not in range.')
    if len(keys) == len(tips):
        for key, tips in zip(keys, tips):
            print(key +'\t-\t'+ tips)
    elif len(tips) == 2 and len(keys) > 2:
        print(f'Range:\n{min} ({tips[0]}) - {max} ({tips[1]})')
    else:
        print(f'Range: {min} - {max}')
    if default is None:
        hint = f'Pick an option ({min}-{max}): '
    else:
        hint = f'Pick an option ({min}-{max}) [{default}]: '
    option = input(hint)
    try:
        if option == '' and default is not None:
            return default
        elif option == '' or int(option) not in keys:
            print(f'Invalid option. Must be between {min} and {max}')
            return ask_range(prompt, min, max, tips, default)
        else:
            return int(option)
    except ValueError:
        print(f'Response must be and integer between {min} and {max}')
        return ask_range(prompt, min, max, tips, default)


def confirm_conf(conf):
    """
    Asks user if configuration dictionary is correct.

    Args:
        conf (dict): Configuration dictionary.

    Returns:
        Tuple(dict, bool): Tuple containing configuration dictionary and 
        whether confirmed.
    """
    print()
    print('Your configuration:')
    for key, value in conf.items():
        print(f'{key.title()}: {value}')
    if not yes_no('Is this correct?'):
        key = ask_options('Which would you like to change?', list(conf.keys()), hints=list(conf.values()))
        conf[key] = None
        return conf, False
    return conf, True


def save_config(conf, default):
    """
    Saves configuration dictionary to file.

    Args:
        conf (dict): Configuration dictionary
        default (str): Default save location.
    """
    print()
    if yes_no('Would you like to save your configuration?'):
        name = simple_response(
            'What would you like to name your configuration?')
        path = ask_path(
            'Please enter the path you would like your configuration saved to',
            default=default)
        file_path = os.path.join(path, name)
        if file_path.find('.json') == -1:
            file_path += '.json'
        with open(file_path, 'w+') as f:
            json.dump(conf, f, indent=4)


def get_config_file(conf):
    """
    Get configuration dictionary from file specified.

    Args:
        conf (str): Configuration dictionary.

    Returns:
        dict: Configuration dictionary.
    """
    with open(conf['config'], 'r') as f:
        saved_conf = json.load(f)
    for key, value in conf.items():
        if value is not None:
            saved_conf[key] = value
    return saved_conf


def main(conf=None):
    """
    Main function

    Args:
        conf (dict, optional): Configuration dictionary. If left to default
        command line arguments will be parsed. Defaults to None.
    """
    if conf is None:
        conf = vars(parse_args())

    home_dir = os.path.expanduser('~')
    home_conf_path = os.path.join(home_dir, '.conf')
    pysync_path = os.path.join(home_conf_path, 'pysync')
    confs_path = os.path.join(pysync_path, 'configs')
    os.makedirs(confs_path, exist_ok=True)

    if conf.get('config', None) is None:
        print_intro()
        conf = check_for_config(conf, confs_path)
        print()
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
    while True:
        _conf = configure(conf.copy())
        if _conf != conf:
            conf, done = confirm_conf(_conf)
            if done:
                save_config(conf, confs_path)
                break
        else:
            conf = _conf
            break
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')
    if conf['host'] == 'Server':
        server_start(conf)
    else:
        client_start(conf)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('\n')
        print('Keyboard Interrupt')
        print('Exiting...')
