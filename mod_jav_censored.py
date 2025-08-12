from .setup import *
from .model_jav_censored import ModelJavCensoredItem
from .task_jav_censored import TaskBase

class ModuleJavCensored(PluginModuleBase):
    def __init__(self, P):
        super(ModuleJavCensored, self).__init__(P, 'setting', name='jav_censored', scheduler_desc="AV 파일처리 - JavCensored")
        self.db_default = {
            f"{self.name}_db_version": "1",
            # auto
            f"{self.name}_auto_start": "False",
            f"{self.name}_interval": "60",
            f'{self.name}_db_delete_day' : '30',
            f'{self.name}_db_auto_delete' : 'False',
            # basic
            f"{self.name}_download_path": "",
            f"{self.name}_temp_path": "",
            f"{self.name}_remove_path": "",
            f"{self.name}_min_size": "300",
            f"{self.name}_max_age": "0",
            f"{self.name}_filename_not_allowed_list": "",
            # filename
            f"{self.name}_include_original_filename": "True",
            f"{self.name}_include_original_filename_option": "0",
            f"{self.name}_filename_test": "",
            # folders
            f"{self.name}_folder_format": "{label}/{code}",
            f"{self.name}_change_filename": "False",
            f"{self.name}_use_meta": "not_using",
            # folders w/o meta
            f"{self.name}_target_path": "",
            # folders w/ meta
            f"{self.name}_meta_dvd_path": "",
            f"{self.name}_meta_dvd_vr_path": "",
            f"{self.name}_meta_dvd_use_dmm_only": "False",
            f"{self.name}_folder_format_actor": "",
            f"{self.name}_meta_dvd_labels_exclude": "",
            f"{self.name}_meta_dvd_labels_include": "",
            f"{self.name}_meta_no_path": "",
            f"{self.name}_meta_no_retry_every": "0",
            f"{self.name}_meta_no_last_retry": "1970-01-01T00:00:00",
            f"{self.name}_meta_no_move": "False",
            f"{self.name}_meta_no_change_filename": "False",
            f"{self.name}_make_yaml": "False",
            f"{self.name}_make_nfo": "False",
            f"{self.name}_make_image": "False",
            # etc
            f"{self.name}_delay_per_file": "0",
            #f"{self.name}_last_list_option": "",
        }
        self.web_list_model = ModelJavCensoredItem

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
        ret = self.start_celery(TaskBase.start, None, "default")

    ###################################################################
    
    