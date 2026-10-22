import json
import os
import uuid

import requests

from .util_runtime import cooperate
from .setup import F, logger
from support import SupportYaml


class Task:
    def make_files(info, folder_path, make_yaml=True, make_nfo=True, make_json=False, make_image=True, make_trailer=False, make_overwrite=False, include_media_path=False, is_code_folder=None, module_name='jav', original_filename=None, existing_folder=None, report=None, storage_result=None):
        
        if not any([make_yaml, make_nfo, make_json, make_image, make_trailer]):
            return True

        metadata_files_ok = True
        report = report if report is not None else {}
        report.setdefault('warnings', [])
        report.setdefault('failures', [])
        if storage_result and (storage_result.get('commit_state') == 'unknown' or storage_result.get('state') == 'blocked'
                or storage_result.get('error_code') in ('storage_lookup_failed','storage_asset_incomplete','storage_conflict','commit_outcome_unknown')):
            report['failures'].append('storage_state_blocked')
            logger.warning('저장소 결과 확인이 필요하여 부가파일 생성을 보류합니다.')
            return False

        # 기본 식별자(Identifier) 결정
        if module_name == 'western' and original_filename:
            # 서양 모듈의 기본 식별자는 원본 영상 파일명
            identifier = original_filename
        else:
            # JAV 모듈의 기본 식별자는 품번
            identifier = (info.get('pure_code') or info.get('originaltitle') or info.get('sorttitle') or info.get('code', 'movie')).lower()

        # 전용 폴더(is_code_folder) 여부
        if is_code_folder is None:
            current_folder_name = os.path.basename(existing_folder or folder_path).lower()
            if module_name == 'western':
                title_for_check = (info.get('title') or identifier).lower()
                is_code_folder = (current_folder_name == identifier.lower()) or (current_folder_name == title_for_check)
            else:
                is_code_folder = current_folder_name.replace('-', '') == identifier.replace('-', '')

        # 공용 폴더 시 기준이 될 최종 비디오 파일 이름
        video_stem = original_filename if original_filename else identifier
        
        prefix = 'movie' if is_code_folder else video_stem
        img_prefix = '' if is_code_folder else f'{video_stem}-'
        trailer_name = 'movie-trailer.mp4' if is_code_folder else f'{video_stem}-trailer.mp4'

        logger.debug(f"부가파일 생성: [모듈:{module_name}] [식별자:{identifier}] [프리픽스:{prefix}] [전용폴더:{is_code_folder}]")

        # YAML, NFO는 결정된 prefix에 따름
        filepath_yaml = os.path.join(folder_path, f'{prefix}.yaml')
        filepath_nfo = os.path.join(folder_path, f'{prefix}.nfo')
        
        # JSON은 항상 고유 식별자 사용
        filepath_json = os.path.join(folder_path, f'{identifier}.json')
        
        # 이미지/트레일러
        filepath_poster = os.path.join(folder_path, f'{img_prefix}poster.jpg')
        filepath_fanart = os.path.join(folder_path, f'{img_prefix}fanart.jpg')
        filepath_trailer = os.path.join(folder_path, trailer_name)

        def read_path(path):
            if os.path.lexists(path) or not existing_folder:
                return path
            return os.path.join(existing_folder, os.path.basename(path))

        def existing(path):
            return os.path.lexists(read_path(path))

        def ready(path):
            path = read_path(path)
            if not os.path.isfile(path) or os.path.getsize(path) <= 0:
                return False
            if str(path).lower().endswith(('.jpg', '.jpeg', '.png')):
                from PIL import Image
                try:
                    with Image.open(path) as image:
                        image.verify()
                except Exception:
                    return False
            return True

        def download(url, path):
            if make_overwrite or not existing(path):
                success = Task.file_save(url, path)
            else:
                success = ready(path)
            if not success or not ready(path):
                report['warnings'].append(os.path.basename(path) + ':download_failed')
                logger.warning('선택 미디어 저장 실패: %s', os.path.basename(path))

        # 공용 데이터 추출 함수 (리스트/단일객체 모두 대응)
        def get_as_list(data, key):
            v = data.get(key, [])
            if v is None: return []
            return v if isinstance(v, list) else [v]

        # 데이터 가공 및 정제
        info_for_files = info.copy()
        info_for_files['extra_info'] = dict(info_for_files.get('extra_info') or {})
        info_for_files['extra_info'].pop('source_video_path', None)

        # [공용 변수 준비] 
        # 정보 포함 옵션이 켜져 있을 때만 데이터를 추출하고, 꺼져 있으면 빈 값을 유지함
        posters = []
        arts = []
        extras_list = []

        # 이미지/트레일러 물리 파일 저장
        if make_image:
            poster_url = next((t['value'] for t in get_as_list(info, 'thumb') if isinstance(t, dict) and t.get('aspect') == 'poster'), None)

            landscape_url = next((t['value'] for t in get_as_list(info, 'thumb') if isinstance(t, dict) and t.get('aspect') == 'landscape'), None)
            if not landscape_url:
                landscapes = get_as_list(info, 'fanart')
                if landscapes:
                    landscape_url = landscapes[0] if isinstance(landscapes[0], str) else landscapes[0].get('value')

            trailer_url = next((e['content_url'] for e in get_as_list(info, 'extras') if isinstance(e, dict) and e.get('content_type') == 'trailer'), None)

            if poster_url:
                download(poster_url, filepath_poster)
            if landscape_url:
                download(landscape_url, filepath_fanart)

        if make_trailer:
            trailer_url = next((e.get('content_url') for e in get_as_list(info, 'extras') if isinstance(e, dict) and e.get('content_type') == 'trailer'), None)
            if trailer_url:
                download(trailer_url, filepath_trailer)

        if include_media_path:
            # 포스터 및 배경 이미지 추출
            all_thumbs = get_as_list(info_for_files, 'thumb')
            
            # 이미지 파일 생성 옵션이 켜져있거나 이미 로컬에 존재하면 제외 처리
            skip_poster_url = ready(filepath_poster)
            skip_fanart_url = ready(filepath_fanart)
            
            # if skip_poster_url or skip_fanart_url:
            #     logger.debug("로컬 이미지가 우선하므로 메타파일(NFO/YAML) 기록에서 일부/전체 이미지 URL을 제외합니다.")

            for t in all_thumbs:
                if not isinstance(t, dict): continue
                val = t.get('value')
                if not val: continue
                
                aspect = t.get('aspect')
                if aspect == 'poster':
                    if not skip_poster_url:
                        posters.append(val)
                elif aspect == 'landscape':
                    if not skip_fanart_url:
                        arts.append(val)
            
            # fanart 필드에 있는 주소들도 arts에 합침
            for f in get_as_list(info_for_files, 'fanart'):
                url = f if isinstance(f, str) else f.get('value') if isinstance(f, dict) else None
                if url and url not in arts:
                    if not skip_fanart_url:
                        arts.append(url)
            
            # 부가 정보 (트레일러 등) 필터링
            raw_extras = get_as_list(info_for_files, 'extras')
            for ext in raw_extras:
                if isinstance(ext, dict) and ext.get('content_type') == 'trailer':
                    # 트레일러 파일 생성 옵션이 켜져 있거나, 이미 로컬 트레일러 파일이 존재한다면 메타 URL에서 제외
                    if ready(filepath_trailer):
                        # logger.debug(f"로컬 트레일러가 우선하므로 메타파일(NFO/YAML) 기록에서 트레일러 URL을 제외합니다.")
                        continue
                extras_list.append(ext)
        else:
            # 정보 포함 안 함 설정 시 관련 필드 삭제 (정제)
            info_for_files.pop('thumb', None)
            info_for_files.pop('fanart', None)
            info_for_files.pop('extras', None)

        # --- JSON 파일 생성 ---
        if make_json:
            if make_overwrite or not existing(filepath_json):
                try:
                    serialized = json.dumps(info_for_files, ensure_ascii=False, indent=4)
                    with open(filepath_json, 'w', encoding='utf-8') as f:
                        f.write(serialized)
                    logger.debug(f"JSON 생성 완료: {filepath_json}")
                except Exception as e:
                    logger.error(f"JSON 생성 중 오류 발생: {e}")
                    metadata_files_ok = False
            else:
                logger.debug(f"이미 존재함 (건너뜀): {os.path.basename(filepath_json)}")

        # --- YAML 파일 생성 ---
        if make_yaml:
            if make_overwrite or not existing(filepath_yaml):
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
                    if not isinstance(actor, dict):
                        continue
                    name_ko_val = str(actor.get('name_ko') or '').strip()
                    name_org_val = str(actor.get('name_org') or '').strip()
                    name_en_val = str(actor.get('name_en') or '').strip()
                    display_name = name_ko_val or name_org_val or name_en_val

                    actor_data = {
                        'name': display_name,
                        'name_ko': name_ko_val,
                        'name_org': name_org_val,
                        'name_en': name_en_val,
                        'role': name_org_val or str(actor.get('role') or '출연').strip(),
                        'photo': str(actor.get('thumb') or '').replace(F.SystemModelSetting.get('ddns'), '')
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
                    metadata_files_ok = False

        # --- NFO 파일 생성 ---
        if make_nfo:
            # 0바이트 파일이 이미 생성되어 있는 경우도 재작성 대상에 포함
            is_nfo_empty = os.path.exists(filepath_nfo) and os.path.getsize(filepath_nfo) == 0

            if make_overwrite or not existing(filepath_nfo) or is_nfo_empty:
                nfo_data = info_for_files.copy()
                nfo_data['thumb'] = [{'value': p, 'aspect': 'poster'} for p in posters]
                nfo_data['fanart'] = arts
                nfo_data['extras'] = extras_list

                # UtilNfo가 요구하는 표준 'name' 키 보장 및 정제
                nfo_actors = []
                for a in info_for_files.get('actor') or []:
                    if not isinstance(a, dict):
                        continue
                    name_ko_val = str(a.get('name_ko') or '').strip()
                    name_org_val = str(a.get('name_org') or '').strip()
                    name_en_val = str(a.get('name_en') or '').strip()
                    name_val = str(a.get('name') or '').strip()

                    # 한국어 -> 원문 -> 영어 -> 기존 name 순으로 표시 이름 결정
                    display_name = name_val or name_ko_val or name_org_val or name_en_val
                    if not display_name:
                        continue

                    nfo_actors.append({
                        'name': display_name,
                        'name_ko': name_ko_val,
                        'name_org': name_org_val,
                        'name_en': name_en_val,
                        'role': str(a.get('role') or name_org_val or '출연').strip(),
                        'thumb': str(a.get('thumb') or '').replace(F.SystemModelSetting.get('ddns'), '')
                    })
                nfo_data['actor'] = nfo_actors

                from support_site import UtilNfo
                try:
                    UtilNfo.make_nfo_movie(nfo_data, output='save', savepath=filepath_nfo)
                    logger.debug(f"NFO 생성 완료: {filepath_nfo}")
                except Exception as e:
                    logger.error(f"NFO 생성 중 오류 발생: {e}")
                    metadata_files_ok = False
                    # 오류 발생 시 생성된 0바이트 파일 정리 (재시도 방해 방지)
                    if os.path.exists(filepath_nfo) and os.path.getsize(filepath_nfo) == 0:
                        try:
                            os.remove(filepath_nfo)
                        except Exception:
                            pass
            else:
                logger.debug(f"이미 존재함 (건너뜀): {os.path.basename(filepath_nfo)}")


        for enabled, filepath in ((make_json, filepath_json), (make_yaml, filepath_yaml), (make_nfo, filepath_nfo)):
            if enabled and not ready(filepath):
                logger.error(f"부가 메타파일 저장 결과를 확인할 수 없습니다: {filepath}")
                metadata_files_ok = False
        report['success'] = metadata_files_ok
        if not metadata_files_ok:
            report['failures'].append('required_metadata_failed')
        return metadata_files_ok


    def file_save(url, filepath, proxy_url=None):
        cooperate()
        filename = os.path.basename(filepath)
        partial = str(filepath) + '.part-' + uuid.uuid4().hex
        try:
            logger.debug(f"파일 다운로드 시도: {filename} <- {url}")
            proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else {}
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': url.split('/', 3)[0] + '//' + url.split('/')[2] 
            }

            with requests.get(url, stream=True, proxies=proxies, headers=headers, verify=False, timeout=180) as r:
                r.raise_for_status()
                with open(partial, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        cooperate()
                        if chunk:
                            f.write(chunk)
            
            if not os.path.isfile(partial) or os.path.getsize(partial) == 0:
                raise ValueError('Empty downloaded media')
            if str(filepath).lower().endswith(('.jpg', '.jpeg', '.png')):
                from PIL import Image
                with Image.open(partial) as image:
                    image.verify()
            os.replace(partial, filepath)
            logger.debug(f"파일 다운로드 성공: {filename}")
            return True
            
        except requests.exceptions.ConnectionError as ce:
            logger.error(f"서버 연결 끊김 (방화벽/봇 차단 의심): {filename} - {str(ce)}")
            return False
        except Exception as e:
            logger.error(f"파일 다운로드 실패: {filename} - {str(e)}")
            return False
        finally:
            try:
                if os.path.exists(partial):
                    os.unlink(partial)
            except OSError:
                pass
