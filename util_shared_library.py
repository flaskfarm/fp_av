"""Media policy applied only to a sidecar serialization copy."""
import copy
from urllib.parse import parse_qs, urlparse


def as_bool(value):
    if isinstance(value, str):
        return value.strip().lower() in ('true', '1', 'yes', 'on')
    return bool(value)


def is_shared_library(config):
    return as_bool(config.get('공유용라이브러리')) or as_bool(config.get('is_shared_library'))


def shared_actor_image_order(config):
    return (config.get('공유용라이브러리_배우이미지우선순위') or
            'google_fileid, site_img_url')


def _is_user_preview(entry, clip):
    if not isinstance(entry, dict):
        return False
    if str(entry.get('title') or '').startswith('[Preview]') or entry.get('video_type') == 'preview':
        return True
    url = str(entry.get('content_url') or '')
    query = parse_qs(urlparse(url).query)
    # Official stored trailers also use mode=preview_*; that mode alone is not provenance.
    file_id = clip.get('google_fileid')
    local_path = clip.get('local_path')
    return bool((file_id and (file_id in query.get('fileid', []) or
                             file_id in query.get('id', []) or f'/d/{file_id}/' in url)) or
                (local_path and local_path in query.get('path', [])))


def _apply_shared_preview_policy(config, result, category):
    """Filter the already-owned output copy, never the metadata response."""
    if (not is_shared_library(config) or
            as_bool(config.get('공유용라이브러리_유저프리뷰클립사용', False))):
        return
    extra = result.get('extra_info') or {}
    clip = extra.pop('preview_clip', None) or {}
    extras = result.get('extras') or []
    kept = [entry for entry in extras if not _is_user_preview(entry, clip)]
    removed = len(kept) != len(extras)
    if removed and not any(isinstance(entry, dict) and entry.get('content_type') == 'trailer' for entry in kept):
        trailer = extra.get('trailer_video')
        if isinstance(trailer, dict):
            from metadata.util_preview import MetaPreviewUtil
            url = MetaPreviewUtil.get_video_stream_url(trailer, result, video_type='trailer', category=category)
            if url:
                kept.append(dict(content_type='trailer', mode='mp4',
                                 title=result.get('title') or '', content_url=url))
        if not any(isinstance(entry, dict) and entry.get('content_type') == 'trailer' for entry in kept):
            original = result.get('original') or {}
            official = [dict(entry, content_type='trailer') for entry in (original.get('extras') or [])
                        if isinstance(entry, dict) and entry.get('content_url') and
                        entry.get('content_type', 'trailer') == 'trailer' and not _is_user_preview(entry, clip)]
            kept.extend(official)
            if not official and isinstance(original.get('trailer'), str) and original['trailer']:
                candidate = dict(content_type='trailer', mode='mp4', title=result.get('title') or '',
                                 content_url=original['trailer'])
                if not _is_user_preview(candidate, clip):
                    kept.append(candidate)
    if removed:
        result['extras'] = kept


def metadata_file_copy(config, metadata, category, media_sources=None):
    """One output copy; retain permitted unproxied download URLs before formatting."""
    from metadata.setup import P, F
    from urllib.parse import urlencode
    result = copy.deepcopy(metadata)
    _apply_shared_preview_policy(config, result, category)
    if media_sources is not None:
        def entries(key):
            value = result.get(key) or []
            return value if isinstance(value, list) else [value]
        for aspect in ('poster', 'landscape'):
            media_sources[aspect] = next((t.get('value') for t in entries('thumb')
                if isinstance(t, dict) and t.get('aspect') == aspect and t.get('value')), None)
        if not media_sources['landscape']:
            media_sources['landscape'] = next((a if isinstance(a, str) else a.get('value')
                for a in entries('fanart') if isinstance(a, (str, dict))), None)
        media_sources['trailer'] = next((e.get('content_url') for e in entries('extras')
            if isinstance(e, dict) and e.get('content_type') == 'trailer' and e.get('content_url')), None)
    result.pop('original', None)
    prefix = 'western' if category == 'WESTERN' else 'jav_uncensored' if category == 'JAV_UNCEN' else 'jav_censored'
    mode = config.get('메타이미지모드') or config.get('image_mode') or ('ff_proxy' if is_shared_library(config) else
        P.ModelSetting.get(prefix + '_image_mode') or P.ModelSetting.get('jav_censored_image_mode'))
    enabled = mode == 'ff_proxy' or P.ModelSetting.get_bool('meta_db_use_ff_proxy')
    module = P.get_module(prefix)
    entry = getattr(module, 'site_map', {}).get(result.get('site')) if module else None
    site = entry.get('instance') if isinstance(entry, dict) else entry
    site_proxy = category != 'WESTERN' and bool(site and (getattr(site, 'config', None) or {}).get('use_proxy') is True)
    video_enabled = enabled or site_proxy or P.ModelSetting.get_bool(prefix + '_use_trailer_proxy')
    extra = result.get('extra_info')
    if isinstance(extra, dict):
        extra.pop('image_transforms', None)
        extra.pop('source_video_path', None)
    ddns = str(F.SystemModelSetting.get('ddns') or '').rstrip('/')
    roots = [str(P.ModelSetting.get(k) or '').rstrip('/') for k in
        ('jav_censored_image_server_url', 'western_image_server_url', 'jav_uncensored_image_server_url')]
    def proxy(url, video=False):
        if not (video_enabled if video else enabled) or not ddns or not isinstance(url, str) or not url.startswith(('http://', 'https://')):
            return url
        parsed = urlparse(url)
        if (parsed.hostname in ('drive.google.com', 'googleusercontent.com', 'lh3.googleusercontent.com') or
                (parsed.hostname or '').endswith('.googleusercontent.com') or '/metadata/normal/' in parsed.path or
                any(root and (url == root or url.startswith(root + '/')) for root in [ddns] + roots)):
            return url
        if video and category == 'WESTERN' and not P.ModelSetting.get_bool('western_use_trailer_proxy'):
            return url
        endpoint = ('jav_video' if video else 'jav_image') + ('_un' if category == 'JAV_UNCEN' else '')
        return ddns + '/metadata/normal/' + endpoint + '?' + urlencode({'site': result.get('site') or '', 'url': url})
    for thumb in result.get('thumb') or []:
        if isinstance(thumb, dict): thumb['value'] = proxy(thumb.get('value'))
    result['fanart'] = [proxy(a) if isinstance(a, str) else dict(a, value=proxy(a.get('value'))) if isinstance(a, dict) else a for a in result.get('fanart') or []]
    for key in ('poster_url', 'landscape_url'):
        if key in result: result[key] = proxy(result[key])
    target_actor_order = shared_actor_image_order(config) if is_shared_library(config) else config.get('actor_img_order')
    actor_order = str(target_actor_order or '').replace(',', ' ').lower().split()
    site_first = is_shared_library(config) and actor_order[:1] == ['site_img_url']
    def actor_site_url(actor):
        extra = actor.get('extra_info') if isinstance(actor.get('extra_info'), dict) else {}
        media = actor.get('media_src') if isinstance(actor.get('media_src'), dict) else {}
        official = media.get('official') if isinstance(media.get('official'), dict) else {}
        for value in (actor.get('site_img_url'), extra.get('site_img_url'), official.get('site_img_url'),
                      media.get('site_img_url'), actor.get('thumb')):
            if not isinstance(value, str):
                continue
            url = value.strip()
            try:
                parsed = urlparse(url)
                if (parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.port == 0 or
                        parsed.username is not None or parsed.password is not None):
                    continue
            except ValueError:
                continue
            if (parsed.hostname in ('drive.google.com', 'googleusercontent.com') or
                    parsed.hostname.endswith('.googleusercontent.com') or '/metadata/normal/' in parsed.path or
                    any(root and (url.lower() == root.lower() or url.lower().startswith(root.lower() + '/'))
                        for root in [ddns] + roots)):
                continue
            return url
        return None
    for actor in result.get('actor') or []:
        if not isinstance(actor, dict):
            continue
        if is_shared_library(config):
            from metadata.mod_meta_db import ModuleMetaDb
            extra = actor.get('extra_info') if isinstance(actor.get('extra_info'), dict) else {}
            sources = [value for value in (actor, actor.get('media_src'), extra, extra.get('media_src'))
                       if isinstance(value, dict)]
            official = [value['official'] for value in sources if isinstance(value.get('official'), dict)] + sources
            custom = [value['custom'] for value in sources if isinstance(value.get('custom'), dict)] + sources
            remote = {
                'official': {key: next((value.get(key) for value in official if value.get(key)), '')
                             for key in ('google_fileid', 'official_cached_google_fileid')},
                'custom': {'custom_user_google_fileid': next((value.get('custom_user_google_fileid')
                           for value in custom if value.get('custom_user_google_fileid')), '')}
            }
            if not remote['official']['official_cached_google_fileid']:
                remote['official']['official_cached_google_fileid'] = next((value.get('user_google_fileid')
                    for value in official if value.get('user_google_fileid')), '')
            remote['official']['site_img_url'] = ''
            for value in official:
                source = actor_site_url({'site_img_url': value.get('site_img_url')})
                if source:
                    remote['official']['site_img_url'] = source
                    break
            domain = 'WESTERN' if category == 'WESTERN' else 'JAV'
            remote_order = ' '.join(key for key in actor_order if key in ('google_fileid', 'site_img_url'))
            selected = ModuleMetaDb.resolve_actor_thumb_url(remote, order_str=remote_order, domain=domain) if remote_order else ''
            if not selected:
                selected = ModuleMetaDb.resolve_actor_thumb_url(remote,
                    order_str='google_fileid, custom_user_google_fileid, official_cached_google_fileid', domain=domain)
            if not selected:
                from metadata.util_metadata import MetaUtil
                file_id = MetaUtil._item_drive_id(actor.get('thumb'))
                if file_id:
                    selected = MetaUtil.format_gdrive_thumbnail_url(file_id)
            actor['thumb'] = selected or ''
            continue
        source = actor_site_url(actor) if site_first else None
        if source:
            actor['thumb'] = source
        else:
            if target_actor_order and not site_first:
                # This URL resolver is pure: no person lookup or storage completion.
                from metadata.mod_meta_db import ModuleMetaDb
                media = actor.get('media_src') or actor.get('extra_info') or actor
                selected = ModuleMetaDb.resolve_actor_thumb_url(media, order_str=target_actor_order,
                    domain='WESTERN' if category == 'WESTERN' else 'JAV')
                if selected:
                    actor['thumb'] = selected
            if actor.get('thumb'):
                actor['thumb'] = proxy(actor['thumb'])
    for entry in result.get('extras') or []:
        if isinstance(entry, dict): entry['content_url'] = proxy(entry.get('content_url'), video=True)
    if as_bool(config.get('메타옵션_이미지제거') or config.get('strip_images')):
        result['thumb'], result['fanart'] = [], []
        result['poster_url'], result['landscape_url'] = '', ''
        if media_sources is not None:
            media_sources['poster'], media_sources['landscape'] = None, None
    return result
