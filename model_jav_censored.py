from .setup import *
from pathlib import Path

class ModelJavCensoredItem(ModelBase):
    P = P
    __tablename__ = f'{P.package_name}_channel'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = P.package_name

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime)
    reserved = db.Column(db.JSON)

    job_type = db.Column(db.String)
    is_file = db.Column(db.Boolean)
    source_dir = db.Column(db.String)
    source_filename = db.Column(db.String)
    source_path = db.Column(db.String)
    move_type = db.Column(db.String)  # -1, 0:정상, 1:타입불일치, 2:중복삭제
    target_dir = db.Column(db.String)
    target_filename = db.Column(db.String)
    target_path = db.Column(db.String)
    log = db.Column(db.String)

    meta_result = db.Column(db.String)
    poster = db.Column(db.String)

    def __init__(self, job_type, source_dir, source_filename):
        self.created_time = datetime.now()
        self.job_type = job_type
        self.is_file = True
        self.source_dir = source_dir
        self.source_filename = source_filename
        self.move_type = None

    def __repr__(self):
        return repr(self.as_dict())


    def set_move_type(self, move_type):
        self.move_type = move_type
        return self

    def set_target(self, trg):
        trg = Path(trg)
        self.target_dir = str(trg.parent)
        self.target_filename = trg.name
        self.target_path = str(trg)
        return self

    @classmethod
    def make_query(cls, req, order='desc', search='', option1='all'):
        with F.app.app_context():
            query = F.db.session.query(cls)
            
            # 1. 검색어 처리
            if search:
                query = query.filter(or_(
                    cls.source_filename.like('%' + search + '%'),
                    cls.target_filename.like('%' + search + '%')
                ))

            # 2. 옵션 필터 (option1)
            if option1 != 'all':
                if option1 == 'meta_success':
                    # VR 포함 여부에 따라 필터링 (HTML 옵션 텍스트 참조)
                    # 여기서는 '메타 성공' 선택 시 meta_success와 vr을 모두 보여주거나 분리할 수 있음.
                    # 일단 meta_success와 vr을 합쳐서 보여주는 것이 일반적 (성공 케이스)
                    query = query.filter(cls.move_type.in_(['meta_success', 'vr', 'dvd']))
                elif option1 == 'normal':
                    query = query.filter(cls.move_type == 'normal')
                elif option1 == 'meta_fail':
                    query = query.filter(cls.move_type.in_(['meta_fail', 'no_meta']))
                elif option1 == 'companion':
                    query = query.filter(cls.move_type.like('companion%'))
                elif option1 == 'custom_path':
                    query = query.filter(cls.move_type == 'custom_path')
                elif option1 == 'subbed':
                    query = query.filter(cls.move_type == 'subbed')
                elif option1 == 'failed':
                    query = query.filter(cls.move_type.in_(['failed_video', 'etc_file']))

            # 3. 정렬
            if order == 'desc':
                query = query.order_by(desc(cls.id))
            else:
                query = query.order_by(cls.id)

            return query

    @classmethod
    def web_list(cls, req):
        try:
            ret = {}
            page = 1
            page_size = 30
            search = ''
            
            if 'page' in req.form:
                page = int(req.form['page'])
            
            if 'search_word' in req.form:
                search = req.form['search_word'].strip()
            elif 'keyword' in req.form:
                search = req.form['keyword'].strip()
            
            # [중요] HTML의 name="option1"을 받음
            option1 = req.form.get('option1', 'all') 
            order = req.form.get('order', 'desc')

            query = cls.make_query(req, order=order, search=search, option1=option1)
            count = query.count()
            query = query.limit(page_size).offset((page-1)*page_size)
            
            lists = query.all()
            ret['list'] = [item.as_dict() for item in lists]
            ret['paging'] = cls.get_paging_info(count, page, page_size)
            
            try:
                # 저장 키값도 통일 (option1|order|search)
                save_option = f'{option1}|{order}|{search}'
                P.ModelSetting.set(f'{cls.P.package_name}_jav_censored_last_list_option', save_option)
            except Exception as e:
                logger.error(f"옵션 저장 실패: {e}")
                
            return ret
        except Exception as e:
            logger.error(f"Exception:{str(e)}")
            logger.error(traceback.format_exc())
            return {'ret':'error', 'msg':str(e)}

