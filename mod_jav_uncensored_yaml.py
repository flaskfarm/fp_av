from .setup import *
from .task_jav_uncensored import TaskBase

class ModuleJavUncensoredYaml(PluginModuleBase):
    def __init__(self, P):
        super(ModuleJavUncensoredYaml, self).__init__(P, 'setting', name='jav_uncensored_yaml', scheduler_desc="AV 파일처리 - JavUncensored with YAML")
        self.db_default = {
            f"{self.name}_db_version": "1",
            f"{self.name}_auto_start": "False",
            f"{self.name}_interval": "60",
            f'{self.name}_filepath' : f"{path_data}/db/fp_av_uncensored.yaml",
        }

    def process_menu(self, page_name, req):
        arg = P.ModelSetting.to_dict()
        try:
            arg['is_include'] = F.scheduler.is_include(self.get_scheduler_name())
            arg['is_running'] = F.scheduler.is_running(self.get_scheduler_name())
            return render_template(f'{P.package_name}_{self.name}_{page_name}.html', arg=arg)
        except Exception as e:
            logger.error(f'Exception:{str(e)}')
            return render_template('sample.html', title=f"{P.package_name}/{self.name}/{page_name}")

    def scheduler_function(self):
        filepath_key = f"{self.name}_filepath"
        ret = self.start_celery(TaskBase.start, None, "yaml", filepath_key)

    ###################################################################
    
    