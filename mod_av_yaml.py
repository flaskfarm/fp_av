from .setup import *
from .util_jobs import JobPanelMixin, yaml_file_names, resolve_yaml_files
from .task_av import TaskBase


class ModuleAvYaml(JobPanelMixin, PluginModuleBase):
    def __init__(self, P):
        super(ModuleAvYaml, self).__init__(P, 'setting', name='av_yaml', scheduler_desc='AV 파일처리 - YAML 작업')
        self.db_default = {
            'av_yaml_db_version': '1',
            'av_yaml_interval': '60',
            'av_yaml_auto_start': 'False',
            'av_yaml_schedule_files': '[]',
        }

    def process_menu(self, page_name, req):
        arg = P.ModelSetting.to_dict()
        arg['yaml_files'] = yaml_file_names()
        arg['is_include'] = F.scheduler.is_include(self.get_scheduler_name())
        arg['is_running'] = F.scheduler.is_running(self.get_scheduler_name())
        return render_template(f'{P.package_name}_{self.name}_{page_name}.html', arg=arg)

    def scheduler_function(self):
        try:
            paths = resolve_yaml_files(P.ModelSetting.get('av_yaml_schedule_files') or '[]')
        except ValueError as error:
            logger.warning('YAML 예약 실행을 건너뜁니다: %s', error)
            return
        self.start_celery(TaskBase.start, None, 'yaml', paths)
