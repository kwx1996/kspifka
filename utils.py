import imp
import importlib
import logging
from builtins import object
from importlib import _bootstrap

import backoff
import requests

log = logging.getLogger(__name__)

logger = logging.getLogger()


def exception_handler(details):
    logger.info(''.format(**details))


def load_object(path):
    try:
        dot = path.rindex('.')
    except ValueError:
        raise ValueError("Error loading object '%s': not a full path" % path)

    module, name = path[:dot], path[dot + 1:]
    mod = import_module(module)

    try:
        obj = getattr(mod, name)
    except AttributeError:
        raise NameError("Module '%s' doesn't define any object named '%s'" % (module, name))

    return obj


def import_module(name, package=None):
    level = 0
    if name.startswith('.'):
        if not package:
            msg = ("the 'package' argument is required to perform a relative "
                   "import for {!r}")
            raise TypeError(msg.format(name))
        for character in name:
            if character != '.':
                break
            level += 1
    return _bootstrap._gcd_import(name[level:], package, level)


class CallLaterOnce:

    def __init__(self, func, *a, **kw):
        self._func = func
        self._a = a
        self._kw = kw
        self._call = None

    def schedule(self, delay=0):
        from twisted.internet import reactor
        if self._call is None:
            self._call = reactor.callLater(delay, self)

    def cancel(self):
        if self._call:
            self._call.cancel()

    def __call__(self):
        self._call = None
        return self._func(*self._a, **self._kw)


class SettingsWrapper(object):

    my_settings = {}
    ignore = [
        '__builtins__',
        '__file__',
        '__package__',
        '__doc__',
        '__name__',
        '__spec__',
        '__loader__',
        '__cached__',
    ]

    def _init__(self):
        pass

    def load(self, local='localsettings.py', default='settings.py'):
        '''
        Load the settings dict

        @param local: The local settings filename to use
        @param default: The default settings module to read
        @return: A dict of the loaded settings
        '''
        self._load_defaults(default)
        self._load_custom(local)

        return self.settings()

    def load_from_string(self, settings_string='', module_name='customsettings'):
        '''
        Loads settings from a settings_string. Expects an escaped string like
        the following:
            "NAME=\'stuff\'\nTYPE=[\'item\']\n"

        @param settings_string: The string with your settings
        @return: A dict of loaded settings
        '''
        try:
            mod = imp.new_module(module_name)
            exec(settings_string, mod.__dict__)
        except TypeError:
            log.warning("Could not import settings")
        self.my_settings = {}
        try:
            self.my_settings = self._convert_to_dict(mod)
        except ImportError:
            log.warning("Settings unable to be loaded")

        return self.settings()

    def settings(self):
        '''
        Returns the current settings dictionary
        '''
        return self.my_settings

    def _load_defaults(self, default='settings.py'):
        '''
        Load the default settings
        '''
        if default[-3:] == '.py':
            default = default[:-3]

        self.my_settings = {}
        try:
            settings = importlib.import_module(default)
            self.my_settings = self._convert_to_dict(settings)
        except ImportError:
            log.warning("No default settings found")

    def _load_custom(self, settings_name='localsettings.py'):
        '''
        Load the user defined settings, overriding the defaults

        '''
        if settings_name[-3:] == '.py':
            settings_name = settings_name[:-3]

        new_settings = {}
        try:
            settings = importlib.import_module(settings_name)
            new_settings = self._convert_to_dict(settings)
        except ImportError:
            log.info("No override settings found")

        for key in new_settings:
            if key in self.my_settings:
                item = new_settings[key]
                if isinstance(item, dict) and \
                        isinstance(self.my_settings[key], dict):
                    for key2 in item:
                        self.my_settings[key][key2] = item[key2]
                else:
                    self.my_settings[key] = item
            else:
                self.my_settings[key] = new_settings[key]

    def _convert_to_dict(self, setting):
        '''
        Converts a settings file into a dictionary, ignoring python defaults

        @param setting: A loaded setting module
        '''
        the_dict = {}
        set = dir(setting)
        for key in set:
            if key in self.ignore:
                continue
            value = getattr(setting, key)
            the_dict[key] = value

        return the_dict
