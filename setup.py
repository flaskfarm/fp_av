from plugin import *

setting = {
    'filepath' : __file__,
    'use_db': True,
    'use_default_setting': True,
    'home_module': None,
    'menu': {
        'uri': __package__,
        'name': 'AV 파일처리',
        'list': [
            {
                'uri': 'jav_censored',
                'name': 'Jav Censored',
                'list': [
                    {
                        'uri': 'setting',
                        'name': '설정',
                    },
                    {
                        'uri': 'list',
                        'name': '처리결과',
                    },
                    {
                        'uri': 'files/jav_censored.md',
                        'name': '매뉴얼',
                    },
                ]
            },
            {
                'uri': 'jav_censored_yaml',
                'name': 'Jav Censored Yaml',
                'list': [
                    {
                        'uri': 'setting',
                        'name': '설정',
                    },
                    {
                        'uri': 'files/jav_censored_yaml.md',
                        'name': '매뉴얼',
                    },
                ]
            },
            {
                'uri': 'jav_uncensored',
                'name': 'Jav Uncensored',
                'list': [
                    {
                        'uri': 'setting',
                        'name': '설정',
                    },
                ]
            },
            {
                'uri': 'manual',
                'name': 'ChangeLog',
                'list': [
                    {'uri':'README.md', 'name':'ChangeLog'},
                ]
            },
            {
                'uri': 'log',
                'name': '로그',
            },
        ]
    },
    'default_route': 'normal',
}

P = create_plugin_instance(setting)
logger = P.logger
PLUGIN_ROOT = os.path.dirname(__file__)

try:
    from .mod_jav_censored import ModuleJavCensored
    from .mod_jav_censored_yaml import ModuleJavCensoredYaml
    from .mod_jav_uncensored import ModuleJavUncensored
    P.set_module_list([ModuleJavCensored, ModuleJavCensoredYaml, ModuleJavUncensored])
except Exception as e:
    P.logger.error(f'Exception:{str(e)}')
    P.logger.error(traceback.format_exc())

