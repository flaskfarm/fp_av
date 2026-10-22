"""Options and return-copy media policy for shared libraries."""
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


def apply_shared_preview_policy(config, metadata, category):
    if (not isinstance(metadata, dict) or not is_shared_library(config) or
            as_bool(config.get('공유용라이브러리_유저프리뷰클립사용', False))):
        return metadata
    result = copy.deepcopy(metadata)
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
    return result
