from .setup import *
from .model_jav_uncensored import ModelJavUncensoredItem
from .task_jav_uncensored import TaskBase

# [수정] 클래스 이름을 Uncensored용으로 변경합니다.
class ModuleJavUncensored(PluginModuleBase):
    def __init__(self, P):
        super(ModuleJavUncensored, self).__init__(P, 'setting', name='jav_uncensored', scheduler_desc="AV 파일처리 - JavUncensored")

        self.db_default = {
            f"{self.name}_db_version": "1",
            # auto
            f"{self.name}_auto_start": "False",
            f"{self.name}_interval": "60",
            f'{self.name}_db_delete_day' : '30',
            f'{self.name}_db_auto_delete' : 'False',
            # basic
            f"{self.name}_download_path": "",
            f"{self.name}_min_size": "300",
            f"{self.name}_max_age": "0",
            f"{self.name}_filename_not_allowed_list": "",
            f"{self.name}_filename_cleanup_list": "3xplanet|archive|bo99|boy999|ddr91|dioguitar|dioguitar23|fengniao|fengniao151|giogio99|im520|javonly|javplayer|javplayer200a|javsubs91|konoha57|love69|mosaic|removed|uncensored|nodrm|nomatch-2023|sis001|u3c3|wwg101|xplanet",
            f"{self.name}_delay_per_file": "0",
            # filename
            f"{self.name}_change_filename": "False",
            f"{self.name}_process_part_files": "True",
            f"{self.name}_include_original_filename": "True",
            f"{self.name}_include_original_filename_option": "original",
            f"{self.name}_filename_test": "",
            # folders
            f"{self.name}_folder_format": "{label}/{code}",
            f"{self.name}_temp_path": "",
            f"{self.name}_remove_path": "",
            f"{self.name}_label_path_overrides": "",
            f"{self.name}_use_meta": "using",
            f"{self.name}_meta_path": "",
            f"{self.name}_meta_no_path": "",
            f"{self.name}_meta_no_change_filename": "False",
            f"{self.name}_meta_no_retry_every": "0",
            f"{self.name}_meta_no_last_retry": "1970-01-01T00:00:00",
            f"{self.name}_target_path": "",
            # 부가파일 생성
            f"{self.name}_make_yaml": "False",
            f"{self.name}_make_nfo": "False",
            f"{self.name}_make_image": "False",
        }
        self.web_list_model = ModelJavUncensoredItem

    def process_menu(self, page_name, req):
        arg = P.ModelSetting.to_dict()
        try:
            arg['is_include'] = F.scheduler.is_include(self.get_scheduler_name())
            arg['is_running'] = F.scheduler.is_running(self.get_scheduler_name())
            return render_template(f'{P.package_name}_{self.name}_{page_name}.html', arg=arg)
        except Exception as e:
            logger.error(f'Exception:{str(e)}')
            logger.error(traceback.format_exc())
            return render_template('sample.html', title=f"{P.package_name}/{self.name}/{page_name}")

    def scheduler_function(self):
        ret = self.start_celery(TaskBase.start, None, "default")

    ###################################################################
    
    