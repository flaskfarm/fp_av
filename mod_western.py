from .setup import *
from .model_western import ModelWesternItem
from .task_western import TaskBase

class ModuleWestern(PluginModuleBase):
    def __init__(self, P):
        super(ModuleWestern, self).__init__(P, 'setting', name='western', scheduler_desc="AV 파일처리 - Western")

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
            f"{self.name}_filename_cleanup_list": "",
            f"{self.name}_delay_per_file": "0",
            f"{self.name}_temp_path": "",
            f"{self.name}_remove_path": "",
            f"{self.name}_scan_with_plex_mate": "False",
            f"{self.name}_dry_run": "False",

            # 파일명 끝부분의 찌꺼기(화질, 릴그룹 등) 제거용 정규식
            f"{self.name}_search_cleanup_pattern": r'[ ._\-]xxx[ ._\-]\d+p[ ._\-][a-zA-Z0-9._-]+(?:\[[a-zA-Z0-9._-]+\])?$',
            # 서양 전용 메타데이터 매칭 커트라인 점수

            # filename
            f"{self.name}_rename_from_folder": "False",

            # folders
            f"{self.name}_folder_format": "{studio}", 
            f"{self.name}_use_meta": "using",
            f"{self.name}_meta_path": "",
            f"{self.name}_target_path": "",

            f"{self.name}_meta_min_score": "80",
            
            f"{self.name}_meta_no_move": "False",
            f"{self.name}_meta_no_path": "",
            f"{self.name}_meta_no_scan_include": "False",
            f"{self.name}_meta_no_retry_every": "0",
            
            f"{self.name}_movie_type_use_separate_path": "False",
            f"{self.name}_movie_type_path": "",
            f"{self.name}_movie_type_folder_format": "{studio}/{title} ({year})",

            f"{self.name}_featurette_regex": r"[-_\s\.](bts|behindthescene|special|interview)s?$",

            # 부가파일
            f"{self.name}_make_yaml": "False",
            f"{self.name}_make_nfo": "False",
            f"{self.name}_make_json": "False",
            f"{self.name}_make_image": "False",
            f"{self.name}_make_trailer": "False",
            f"{self.name}_make_overwrite": "False",
            f"{self.name}_include_media_path": "False",

            # 동반자막
            f"{self.name}_companion_enable": "False",
            f"{self.name}_companion_add_ko": "True",
            f"{self.name}_companion_detect_kor": "False",
            f"{self.name}_companion_use_separate_path": "False",
            f"{self.name}_companion_path": "",
            f"{self.name}_companion_meta_fail_path": "",
            # 메타 취득 임시 오버라이드 기본값
            f"{self.name}_is_shared_library": "False",
            f"{self.name}_override_actor_img_order": "",
            f"{self.name}_override_image_mode": "",
            f"{self.name}_strip_images": "False",
        }
        self.web_list_model = ModelWesternItem

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

    def process_command(self, command, arg1, arg2, arg3, req):
        try:
            if command == 'meta_no_path_start':
                path = P.ModelSetting.get(f"{self.name}_meta_no_path").strip()
                if not path:
                    temp_path = P.ModelSetting.get(f"{self.name}_temp_path").strip()
                    if temp_path:
                        path = os.path.join(temp_path, '[NO META]')
                
                if not path:
                    return jsonify({'ret': 'fail', 'msg': '경로가 존재하지 않거나 설정되지 않았습니다.'})

                # 템플릿 변수 분리
                if '{' in path:
                    path = path.split('{')[0].rstrip('/\\')

                if not os.path.exists(path):
                    return jsonify({'ret': 'fail', 'msg': f'처리할 폴더가 존재하지 않습니다.\n({path})'})
                
                self.start_celery(TaskBase.start, None, 'manual_path', path)
                return jsonify({'ret': 'success', 'msg': f'[{os.path.basename(path)}] 폴더에서 재처리를 시작했습니다.'})
                
        except Exception as e:
            logger.error(f'Exception:{str(e)}')
            return {'ret': 'exception', 'msg': str(e)}
