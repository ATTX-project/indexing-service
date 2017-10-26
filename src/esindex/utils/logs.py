import logging
import logging.config


config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'theFormatter': {
            'format': "[%(asctime)s] [%(process)d] [%(funcName)s] [%(levelname)s] %(message)s",
            'datefmt': '%Y-%m-%d %H:%M:%S %z',
            'class': 'logging.Formatter'
        },
        'threadFormatter': {
            'format': "[%(asctime)s] [%(process)d] (%(threadName)s) [%(funcName)s] [%(levelname)s] %(message)s",
            'datefmt': '%Y-%m-%d %H:%M:%S %z',
            'class': 'logging.Formatter'
        },
        'access': {
            'format': '%(message)s',
            'class': 'logging.Formatter'
        }
    },
    'handlers': {
        'fileHandler': {
            'level': 'INFO',
            'formatter': 'theFormatter',
            'class': 'logging.FileHandler',
            'filename': 'logs/service.log',
            'mode': 'a+'
        },
        'consoleHandler': {
            'level': 'INFO',
            'formatter': 'theFormatter',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout'
        },
        'threadHandler': {
            'level': 'INFO',
            'formatter': 'threadFormatter',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout'
        }
    },
    'loggers': {
        '': {
            'handlers': ['consoleHandler'],
            'level': 'INFO',
            'propagate': True
        },
        'appLogger': {
            'handlers': ['consoleHandler', 'fileHandler'],
            'level': 'INFO',
            'propagate': True
        },
        'threadLogger': {
            'handlers': ['consoleHandler', 'threadHandler'],
            'level': 'INFO',
            'propagate': True
        }
    }
}

logging.config.dictConfig(config)
app_logger = logging.getLogger('appLogger')
thread_logger = logging.getLogger('threadLogger')
