from .setup import *
from pathlib import Path
ModelSetting = P.ModelSetting
from .tool import ToolExpandFileProcess, UtilFunc
from .model_jav_censored import ModelJavCensoredItem
from support import SupportYaml, SupportUtil




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


    def make_files(info, folder_path, make_yaml=True, make_nfo=True, make_image=True):
        if make_yaml == False and make_nfo == False and make_image == False:
            return
        filepath_yaml = os.path.join(folder_path, 'movie.yaml')
        filepath_nfo = os.path.join(folder_path, 'movie.nfo')
        filepath_poster = os.path.join(folder_path, 'poster.jpg')
        filepath_fanart = os.path.join(folder_path, 'fanart.jpg')
        filepath_trailer = os.path.join(folder_path, 'movie-trailer.mp4')
        if os.path.exists(filepath_yaml) and os.path.exists(filepath_nfo) and os.path.exists(filepath_poster) and os.path.exists(filepath_fanart) and os.path.exists(filepath_trailer):
            return

        if make_yaml and os.path.exists(filepath_yaml) == False:
            yaml_data = {
                'primary': True,
                'code': info.get('code', ''),
                'title': info.get('title', ''),
                'original_title': info.get('originaltitle', ''),
                'title_sort': info.get('sorttitle', ''),
                'originally_available_at': info.get('premiered', ''),
                'year': info.get('year', 1950),
                'studio': info.get('studio', ''),
                'content_rating': info.get('mpaa', '청소년 관람불가'),
                'tagline': info.get('tagline', ''),
                'summary': info.get('plot', ''),
                'rating': '',
                'rating_image': info.get('rating_image', ''),
                'audience_rating': info.get('audience_rating', ''),
                'audience_rating_image': info.get('audience_rating_image', ''),
                
                # set_data_list
                'genres': info.get('genre') or [],
                'collections': info.get('tag') or [],
                'countries': info.get('country') or [],
                'similar': [],
                # set_data_person
                'writers': [],
                'directors': [],
                'producers': [],
                'roles': [],
                # set_data_media
                'posters': [],
                'art': [],
                'themes': [],
                # set_data_reviews
                'reviews': [],
                # set_data_extras
                'extras': [] #info.get('extras', []),
            }
            
            actors = info.get('actor') if info.get('actor') else []
            for actor in actors:
                actor_data = {
                    'name': actor.get('name', ''),
                    'role': actor.get('originalname', ''),
                    'photo': actor.get('thumb', '').replace(F.SystemModelSetting.get('ddns'), '')
                }
                yaml_data['roles'].append(actor_data)

            if info.get('director'):
                yaml_data['directors'] = info['director']
            
            try:
                if info['ratings'] is not None and len(info['ratings']) > 0:
                    if info['ratings'][0]['max'] == 5:
                        yaml_data['rating'] = float(info['ratings'][0]['value']) * 2
                    else:
                        yaml_data['rating'] = float(info['ratings'][0]['value'])
            except Exception as e:
                pass
            SupportYaml.write_yaml(filepath_yaml, yaml_data)

        if make_image:
            for thumb in info.get('thumb') or []:
                if os.path.exists(filepath_poster) == False and thumb.get('aspect', '') == 'poster':
                    make_poster = Task.file_save(thumb['value'], filepath_poster)
                elif os.path.exists(filepath_fanart) == False and thumb.get('aspect', '') == 'landscape':
                    make_art = Task.file_save(thumb['value'], filepath_fanart)
            for extra in info.get('extras') or []:
                if os.path.exists(filepath_trailer) == False and extra.get('content_type', '') == 'trailer':
                    make_trailer = Task.file_save(extra['content_url'], filepath_trailer)

        if make_nfo and os.path.exists(filepath_nfo) == False:
            info_for_nfo = info.copy()
            info_for_nfo['thumb'] = info_for_nfo.get('thumb') or []
            info_for_nfo['extras'] = info_for_nfo.get('extras') or []
            info_for_nfo['fanart'] = info_for_nfo.get('fanart') or []
            from support_site import UtilNfo
            UtilNfo.make_nfo_movie(info_for_nfo, output='save', savepath=filepath_nfo)


    def file_save(url, filepath, proxy_url=None):
        try:
            proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else {}
            with requests.get(url, stream=True, proxies=proxies) as r:
                r.raise_for_status()
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        except requests.exceptions.RequestException as e:
            return False
        return True
    

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

