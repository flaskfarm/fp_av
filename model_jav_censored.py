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

