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
                        'name': '작업 관리',
                    },
                    {
                        'uri': 'manual/files/jav_censored.md',
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
                    {
                        'uri': 'list',
                        'name': '작업 관리',
                    },
                ]
            },
            {
                'uri': 'western',
                'name': 'Western',
                'list': [
                    {
                        'uri': 'setting',
                        'name': '설정',
                    },
                    {
                        'uri': 'list',
                        'name': '작업 관리',
                    },
                ]
            },
            {
                'uri': 'av_yaml',
                'name': 'YAML 작업',
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
    from .mod_jav_uncensored import ModuleJavUncensored
    from .mod_western import ModuleWestern
    from .mod_av_yaml import ModuleAvYaml
    P.set_module_list([ModuleJavCensored, ModuleJavUncensored, ModuleWestern, ModuleAvYaml])
except Exception as e:
    P.logger.error(f'Exception:{str(e)}')
    P.logger.error(traceback.format_exc())

