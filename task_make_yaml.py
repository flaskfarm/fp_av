from .setup import *
from pathlib import Path
ModelSetting = P.ModelSetting
from .tool import ToolExpandFileProcess, UtilFunc
from .model_jav_censored import ModelJavCensoredItem
from support import SupportYaml, SupportUtil
import os



class Task:
    metadata_module = None

    config = {}

    def load():
        try:
            Task.config= SupportYaml.read_yaml('/data/db/fp_av_make_yaml.yaml')
            print(Task.config)
        except Exception as e:
            logger.error(f"YAML 파일 처리 중 오류 발생: {str(e)}")

    @F.celery.task
    def start1(*args):
        Task.load()
        logger.info("Jav Censored Task 시작")
        logger.info(d(args))
        Task.meta_module = Task.get_meta_module()

        if os.path.exists(Task.Task.config['finish_folder_path']):
            logger.info(f"처리완료 폴더 파일이 존재: {Task.config['finish_folder_path']}")
            with open(Task.config['finish_folder_path'], "r", encoding="utf-8") as f:
                for line in f:
                    tmps = line.split('|')
                    print(line)
                    if tmps:
                        if  tmps[0] not in Task.config['finish_folder']:
                            Task.config['finish_folder'].append(tmps[0])
                            Task.config['code_list'].append(tmps[1])
                        else:
                            logger.error(f"중복된 폴더: {tmps[0]}")
                            logger.error(f"중복된 폴더: {tmps[0]}")
                            logger.error(f"중복된 폴더: {tmps[0]}")
                            logger.error(f"중복된 폴더: {tmps[0]}")
                            return
                #Task.config['finish_folder'] = set(line.strip() for line in f)
            logger.info(f"처리완료 폴더: {len(Task.config['finish_folder'])}개")
        else:
            Task.config['finish_folder'] = []
        #Task.config['finish_folder'] = set(Task.config['finish_folder'])
        #Task.config['code_list'] = set(Task.config['code_list'])
        logger.info(f"처리완료 폴더: {len(Task.config['finish_folder'])}개")

        for alphabet in os.listdir(Task.config["root"]):
            path_alphabet = os.path.join(Task.config["root"], alphabet)
            for label in os.listdir(path_alphabet):
                path_label = os.path.join(path_alphabet, label)
                for code in os.listdir(path_label):
                    try:
                        path_code = os.path.join(path_label, code)
                        #logger.debug(path_code)
                        if path_code in Task.config['finish_folder']:
                            continue
                        #for filename in ['info.json', 'movie.yaml', 'movie.nfo']:
                        #    tmp = os.path.join(path_code, filename)
                        #    if os.path.exists(tmp):
                        #        os.remove(tmp)
                        data = {'path_code': path_code}    
                        Task.process_code(data)
                    except Exception as e:
                        logger.error(f"Exception:{str(e)}")
                        logger.error(traceback.format_exc())
                    #return

    def process_code(data):
        #logger.error("처리할 코드: %s", data['path_code'])
        #if data['path_code'] in Task.config['finish_folder']:
        #    #logger.error("이미 처리한 폴더: %s", data['path_code'])
        #    return

        data['code'] = os.path.split(data['path_code'])[-1]
        #data['code'] = re.sub(r"[\[\{\(].*?[\]\}\)]", "", data['code']).strip()
        data['code'] = re.sub(r"\[.*?\]", "", data['code']).strip()
        data['code'] = re.sub(r"\(.*?\)", "", data['code']).strip()
        data['code'] = re.sub(r"\{.*?\}", "", data['code']).strip()
        logger.error(f"검색: {data['code']}")
        
        """
        finishcheck = True
        filecount = len(os.listdir(data['path_code']))
        if filecount != len(Task.config['check_file']) + 2:
            finishcheck = False
        else:
            for file in Task.config['check_file']:
                if os.path.exists(os.path.join(data['path_code'], file)) == False:
                    finishcheck = False
                    break
        if finishcheck:
            with open(Task.config['finish_folder_path'], "a", encoding="utf-8") as f:
                f.writelines(f"{data['path_code']}|{data['code']}|{filecount}\n")
                return  
        """
        check = Task.config['check_file']

        # 메타 검색
        #for site in ["dmm", "mgstage", "jav321"]:
        site_list = Task.config.get('site_list', ["dmm", "mgstage"])
        for site in site_list:
            #tmp = search_name
            #if site == "javdb":
            #    tmp = search_name.replace(" ", "-").upper()
            #data = meta_module.search2(tmp, site, manual=False)
            logger.info(f"메타 검색({site}): {data['code']}")
            data['search'] = Task.meta_module.search2(data['code'], site['site'], manual=False)
            if data['search'] == None:
                logger.error(f"검색결과({site}): NONE")
                continue
            if len(data['search']) > 0 and data['search'][0]["score"] >= site['score']:
                data['info'] = Task.meta_module.info(data['search'][0]["code"])
                if data['info'] is not None:
                    if data['info'].get('extras') is None:
                        data['info']['extras'] = []
                    logger.info(f"메타 정보({site}): {data['info']['code']} {len(data['info']['extras'])}")
                    if len(data['info']['extras']) == 0:
                        check = Task.config['check_file'][:-1]
                    Task.make_files(data['info'], data['path_code'])
                    break
        
        
        finishcheck = True
        filecount = len(os.listdir(data['path_code']))
        if filecount != len(check) + 2:
            finishcheck = False
        else:
            for file in check:
                if os.path.exists(os.path.join(data['path_code'], file)) == False:
                    finishcheck = False
                    break
        if finishcheck:
            with open(Task.config['finish_folder_path'], "a", encoding="utf-8") as f:
                f.writelines(f"{data['path_code']}|{data['code']}|{filecount}\n")
                return
        else:
            with open(Task.config['tmp_folder_path'], "a", encoding="utf-8") as f:
                f.writelines(f"{data['path_code']}|{data['code']}|{filecount}\n")
                return
        return


    def make_files(info, folder_path, make_yaml=True, make_nfo=True, make_json=False, make_image=True, make_overwrite=False, include_image_paths_in_file=False, is_code_folder=None):
        if not any([make_yaml, make_nfo, make_json, make_image]):
            return

        # 1. 파일명 결정
        code_name = (info.get('originaltitle') or info.get('sorttitle') or info.get('code', 'movie')).lower()
        if is_code_folder is None:
            current_folder_name = os.path.basename(folder_path).lower()
            is_code_folder = current_folder_name.replace('-', '') == code_name.replace('-', '')
        
        prefix = 'movie' if is_code_folder else code_name
        # logger.debug(f"부가파일 생성 시작: [코드:{code_name}] [폴더명:{os.path.basename(folder_path)}] [코드폴더여부:{is_code_folder}]")

        filepath_yaml = os.path.join(folder_path, f'{prefix}.yaml')
        filepath_nfo = os.path.join(folder_path, f'{prefix}.nfo')
        filepath_json = os.path.join(folder_path, f'{code_name}.json') # JSON은 항상 품번.json
        
        # 이미지 파일명 (Plex 에이전트 표준: 품번 폴더가 아니면 품번-poster.jpg 형태)
        img_prefix = '' if is_code_folder else f'{code_name}-'
        filepath_poster = os.path.join(folder_path, f'{img_prefix}poster.jpg')
        filepath_fanart = os.path.join(folder_path, f'{img_prefix}fanart.jpg')
        filepath_trailer = os.path.join(folder_path, f'{img_prefix}movie-trailer.mp4')
        
        # 2. 공용 데이터 추출 함수 (리스트/단일객체 모두 대응)
        def get_as_list(data, key):
            v = data.get(key, [])
            if v is None: return []
            return v if isinstance(v, list) else [v]

        # 3. 데이터 가공 및 정제
        info_for_files = info.copy()

        # [공용 변수 준비] 
        # 정보 포함 옵션이 켜져 있을 때만 데이터를 추출하고, 꺼져 있으면 빈 값을 유지함
        posters = []
        arts = []
        extras_list = []

        if include_image_paths_in_file:
            # 포스터 및 배경 이미지 추출
            all_thumbs = get_as_list(info_for_files, 'thumb')
            for t in all_thumbs:
                if not isinstance(t, dict): continue
                val = t.get('value')
                if not val: continue
                
                aspect = t.get('aspect')
                if aspect == 'poster':
                    posters.append(val)
                elif aspect == 'landscape':
                    arts.append(val)
            
            # fanart 필드에 있는 주소들도 arts에 합침
            for f in get_as_list(info_for_files, 'fanart'):
                url = f if isinstance(f, str) else f.get('value') if isinstance(f, dict) else None
                if url and url not in arts:
                    arts.append(url)
            
            # 부가 정보 (트레일러 등)
            extras_list = get_as_list(info_for_files, 'extras')
        else:
            # 정보 포함 안 함 설정 시 관련 필드 삭제 (정제)
            info_for_files.pop('thumb', None)
            info_for_files.pop('fanart', None)
            info_for_files.pop('extras', None)

        # 4. 이미지/트레일러 물리 파일 저장 (개선된 로직 + 덮어쓰기 적용)
        if make_image:
            poster_url = next((t['value'] for t in get_as_list(info, 'thumb') if isinstance(t, dict) and t.get('aspect') == 'poster'), None)
            
            landscape_url = next((t['value'] for t in get_as_list(info, 'thumb') if isinstance(t, dict) and t.get('aspect') == 'landscape'), None)
            if not landscape_url:
                landscapes = get_as_list(info, 'fanart')
                if landscapes:
                    landscape_url = landscapes[0] if isinstance(landscapes[0], str) else landscapes[0].get('value')
            
            trailer_url = next((e['content_url'] for e in get_as_list(info, 'extras') if isinstance(e, dict) and e.get('content_type') == 'trailer'), None)

            if poster_url:
                if make_overwrite or not os.path.exists(filepath_poster):
                    Task.file_save(poster_url, filepath_poster)
                else:
                    logger.debug(f"이미 존재함 (건너뜀): {os.path.basename(filepath_poster)}")
            
            if landscape_url:
                if make_overwrite or not os.path.exists(filepath_fanart):
                    Task.file_save(landscape_url, filepath_fanart)
                else:
                    logger.debug(f"이미 존재함 (건너뜀): {os.path.basename(filepath_fanart)}")
            
            if trailer_url:
                if make_overwrite or not os.path.exists(filepath_trailer):
                    Task.file_save(trailer_url, filepath_trailer)
                else:
                    logger.debug(f"이미 존재함 (건너뜀): {os.path.basename(filepath_trailer)}")

        # --- JSON 파일 생성 ---
        if make_json:
            try:
                with open(filepath_json, 'w', encoding='utf-8') as f:
                    json.dump(info_for_files, f, ensure_ascii=False, indent=4)
                logger.debug(f"JSON 생성 완료: {filepath_json}")
            except Exception as e:
                logger.error(f"JSON 생성 중 오류 발생: {e}")

        # --- YAML 파일 생성 ---
        if make_yaml:
            if make_overwrite or not os.path.exists(filepath_yaml):
                yaml_data = {
                    'primary': True,
                    'code': info_for_files.get('code', ''),
                    'title': info_for_files.get('title', ''),
                    'original_title': info_for_files.get('originaltitle', ''),
                    'title_sort': info_for_files.get('sorttitle', ''),
                    'originally_available_at': info_for_files.get('premiered', ''),
                    'year': info_for_files.get('year', 1950),
                    'studio': info_for_files.get('studio', ''),
                    'content_rating': info_for_files.get('mpaa', '청소년 관람불가'),
                    'tagline': info_for_files.get('tagline', ''),
                    'summary': info_for_files.get('plot', ''),
                    'rating': '',
                    'rating_image': info_for_files.get('rating_image', ''),
                    'audience_rating': info_for_files.get('audience_rating', ''),
                    'audience_rating_image': info_for_files.get('audience_rating_image', ''),
                    
                    'genres': info_for_files.get('genre') or [],
                    'collections': info_for_files.get('tag') or [],
                    'countries': info_for_files.get('country') or [],
                    'similar': [],
                    'writers': [],
                    'directors': [],
                    'producers': [],
                    'roles': [],
                    'posters': posters,
                    'art': arts,
                    'themes': [],
                    'reviews': [],
                    'extras': extras_list,
                }
                
                actors = info_for_files.get('actor') if info_for_files.get('actor') else []
                for actor in actors:
                    actor_data = {
                        'name': actor.get('name', ''),
                        'role': actor.get('originalname', ''),
                        'photo': actor.get('thumb', '').replace(F.SystemModelSetting.get('ddns'), '')
                    }
                    yaml_data['roles'].append(actor_data)

                if info_for_files.get('director'):
                    yaml_data['directors'] = info_for_files['director']
                
                try:
                    if info_for_files.get('ratings') is not None and len(info_for_files['ratings']) > 0:
                        if info_for_files['ratings'][0]['max'] == 5:
                            yaml_data['rating'] = float(info_for_files['ratings'][0]['value']) * 2
                        else:
                            yaml_data['rating'] = float(info_for_files['ratings'][0]['value'])
                except Exception as e:
                    pass

                try:
                    SupportYaml.write_yaml(filepath_yaml, yaml_data)
                    logger.debug(f"YAML 생성 완료: {filepath_yaml}")
                except Exception as e:
                    logger.error(f"YAML 생성 중 오류 발생: {e}")

        # --- NFO 파일 생성 ---
        if make_nfo:
            if make_overwrite or not os.path.exists(filepath_nfo):
                nfo_data = info_for_files.copy()
                nfo_data['thumb'] = [{'value': p, 'aspect': 'poster'} for p in posters]
                nfo_data['fanart'] = arts
                nfo_data['extras'] = extras_list
                from support_site import UtilNfo
                try:
                    UtilNfo.make_nfo_movie(nfo_data, output='save', savepath=filepath_nfo)
                    logger.debug(f"NFO 생성 완료: {filepath_nfo}")
                except Exception as e:
                    logger.error(f"NFO 생성 중 오류 발생: {e}")


    def file_save(url, filepath, proxy_url=None):
        filename = os.path.basename(filepath)
        try:
            proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else {}
            with requests.get(url, stream=True, proxies=proxies) as r:
                r.raise_for_status()
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            logger.debug(f"파일 다운로드 성공: {filepath}")
            return True
        except Exception as e:
            logger.error(f"파일 다운로드 실패: {filename} - {str(e)}")
            return False


    def get_meta_module():
        try:
            if Task.metadata_module is None:
                Task.metadata_module = F.PluginManager.get_plugin_instance("metadata").get_module("jav_censored")
            return Task.metadata_module
        except Exception as exception:
            logger.debug("Exception:%s", exception)
            logger.debug(traceback.format_exc())










    @F.celery.task
    def start(*args):
        Task.load()
        logger.info("Jav Censored Task 시작")
        logger.info(d(args))
        Task.meta_module = Task.get_meta_module()




        
        for tmp in reversed(Task.config['folder_list']):
            tmp = tmp.replace("/gdrive/Shareddrives/VIDEO5 - AV/MP/GDS", "/mnt/AV_MP/GDS")
            data = {'path_code': tmp}    
            print(f"Processing11: {tmp}")
            try:
                Task.process_code(data)
            except Exception as exception:
                logger.debug("Exception:%s", exception)
                logger.debug(traceback.format_exc())

