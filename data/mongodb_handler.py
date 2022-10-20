
# Author  : 이병헌
# Contact : lww7438@gmail.com
# Date    : 2022-10-12(수)



# Required Modules
from pymongo        import MongoClient
from pymongo.cursor import CursorType



# Class Declaration
class MongoDBHandler:
    """
    Mongo DB 연결 설정 및 CRUD 작업을 간편히 수행할 수 있게 하는 다양한 기능을 제공한다.
    """
    
    def __init__(self, connection_url:str):
        """
        config.ini 파일에서 MongoDB 접속정보를 로딩 한다. 
        접속 정보를 이용해 MongoDB 접속과 명령어 처리에 사용할 self._clinet 객체를 생성하고,
        _db, _collection에는 현재 사용하는 database 및 collection 명을 저장한다.
        
        [Parameters]
        connection_url (str) : Mongo DB에 접속하기 위한 URL

        [Returns]
        No Returns 
        """

        # self._client에 MongoClient 객체를 저장 (MongoDB의 모든 Command들은 self._client를 통해 수행됨)
        self._client = MongoClient(connection_url)

    def insert_item(self, data:dict, db_name=None, collection_name=None):
        """
        MongoDB에 하나의 document를 입력하기 위한 메서드이다.
        
        [Parameters]
        data            (dict) : 입력할 document
        db_name         (str)  : 입력할 document가 속하게 될 MongoDB database 이름 (default=None)
        collection_name (str)  : 입력할 document가 속하게 될 MongoDB collection 이름 (default=None)

        [Returns]
        str : 입력 완료된 문서의 ObjectId

        [Exceptions]
        매개변수 db_name과 매개변수 collection_name이 존재하지 않는 경우
        """
 
        # data가 dict 타입이 아닌 경우, 예외를 발생
        if not isinstance(data, dict):
            raise Exception("data type should be dict")

        # db_name이 입력되지 않았거나, collenction_name이 입력되지 않았을 경우, 예외를 발생
        if (db_name is None) or (collection_name is None):
            raise Exception("Need to param db_name, collection_name")

        # MongoDB의 insertOne 명령어는 PyMongo에서 insert_one으로 지원
        # 생성한 Document의 _id를 리턴
        return self._client[db_name][collection_name].insert_one(data).inserted_id

    def insert_items(self, datas:list, db_name:str=None, collection_name:str=None):
        """
        MongoDB에 다수의 document를 입력하기 위한 메소드이다.
        
        [Parameters]
        datas           (list) : 입력할 document들이 저장된 리스트이며, 각 document의 타입은 dict
        db_name         (str)  : 입력할 document들이 속하게 될 MongoDB database 이름 (default=None)
        collection_name (str)  : 입력할 document들이 속하게 될 MongoDB collection 이름 (default=None)

        [Returns]
        list : 입력 완료된 문서의 ObjectId 리스트이며, 각 ObjectId의 타입은 str

        [Exceptions]
        매개변수 db_name과 매개변수 collection_name이 존재하지 않는 경우
        """

        # data가 list 타입이 아닌 경우, 예외를 발생
        if not isinstance(datas, list):
            raise Exception("datas type should be list")

        # db_name이 입력되지 않았거나, collenction_name이 입력되지 않았을 경우, 예외를 발생
        if (db_name is None) or (collection_name is None):
            raise Exception("Need to param db_name, collection_name")

        # MongoDB의 insertMany 명령어는 PyMongo에서 insert_many로 지원
        # 생성한 Document들의 _id를 저장한 리스트를 리턴
        return self._client[db_name][collection_name].insert_many(datas).inserted_ids

    def find_item(self, condition:dict=None, db_name:str=None, collection_name:str=None):
        """
        MongoDB에 하나의 document를 검색하기 위한 메소드이다.
        
        [Parameters]
        condition       (dict) : 검색할 document들에 대한 검색 조건 (default=None)
        db_name         (str)  : 검색할 document들이 속한 MongoDB database 이름 (default=None)
        collection_name (str)  : 검색할 document들이 속한 MongoDB collection 이름 (default=None)

        [Returns]
        dict : 검색 결과에 해당되는 Document

        [Exceptions]
        매개변수 db_name과 매개변수 collection_name이 존재하지 않는 경우
        """

        # condition이 dict 타입이 아닌 경우, empty dictionary로 초기화한 후 검색을 진행 (SELECT *)
        if (condition is None) or (not isinstance(condition, dict)):
            condition = {}

        # db_name이 입력되지 않았거나, collenction_name이 입력되지 않았을 경우, 예외를 발생  
        if (db_name is None) or (collection_name is None):
            raise Exception("Need to param db_name, collection_name")

        # MongoDB의 findOne 명령어는 PyMongo에서 find_one으로 지원
        # 검색 조건에 해당되는 Document를 리턴
        return self._client[db_name][collection_name].find_one(condition, {"_id": False})

    def find_items(self, condition:dict=None, db_name:str=None, collection_name:str=None):
        """
        MongoDB에 다수의 document를 검색하기 위한 메소드이다.
        
        [Parameters]
        condition       (dict) : 검색할 document들에 대한 검색 조건 (default=None)
        db_name         (str)  : 검색할 document들이 속한 MongoDB database 이름 (default=None)
        collection_name (str)  : 검색할 document들이 속한 MongoDB collection 이름 (default=None)

        [Returns]
        Cursor : 검색 결과를 가리키는 Cursor 객체

        [Exceptions]
        매개변수 db_name과 매개변수 collection_name이 존재하지 않는 경우
        """

        # condition이 dict 타입이 아닌 경우, empty dictionary로 초기화한 후 검색을 진행 (SELECT *)
        if condition is None or not isinstance(condition, dict):
            condition = {}

        # db_name이 입력되지 않았거나, collenction_name이 입력되지 않았을 경우, 예외를 발생    
        if db_name is None or collection_name is None:
            raise Exception("Need to param db_name, collection_name")

        # MongoDB의 find 명령어는 PyMongo에서도 find로 지원
        # 검색 조건에 해당되는 Document들에 대한 Cursor를 리턴
        # 대규모 데이터 쿼리를 위해 no_cursor_timeout을 True로 설정하고, cursor_type을 EXHAUST로 설정
        return self._client[db_name][collection_name].find(condition, {"_id": False}, no_cursor_timeout=True, cursor_type=CursorType.EXHAUST)

    def delete_items(self, condition:dict=None, db_name:str=None, collection_name:str=None):
        """
        MongoDB에 다수의 document를 삭제하기 위한 메소드이다.
        
        [Parameters]
        condition       (dict) : 삭제할 document들에 대한 검색 조건 (default=None)
        db_name         (str)  : 삭제할 document들이 속한 MongoDB database 이름 (default=None)
        collection_name (str)  : 삭제할 document들이 속한 MongoDB collection 이름 (default=None)

        [Returns]
        DeleteResult : 삭제 결과를 저장하고 있는 DeleteResult 객체

        [Exceptions]
        매개변수 db_name과 매개변수 collection_name이 존재하지 않는 경우
        """

        # condition이 dict 타입이 아니거나 입력되지 않은 경우, 예외를 발생
        # 삭제 조건이 없을 경우, 삭제가 진행되지 않음
        if (condition is None) or (not isinstance(condition, dict)):
            raise Exception("Need to condition")
        
        # db_name이 입력되지 않았거나, collenction_name이 입력되지 않았을 경우, 예외를 발생
        if (db_name is None) or (collection_name is None):
            raise Exception("Need to param db_name, collection_name")

        # MongoDB의 deleteMany 명령어는 PyMongo에서 delete_many로 지원
        # 삭제 결과를 저장하고 있는 DeleteResult 객체를 리턴
        return self._client[db_name][collection_name].delete_many(condition)

    def update_item(self, condition:dict=None, update_value:dict=None, db_name:str=None, collection_name:str=None):
        """
        MongoDB에 하나의 document를 업데이트하기 위한 메소드이다.
        
        [Parameters]
        condition       (dict) : 업데이트할 document에 대한 검색 조건 (default=None)
        update_value    (dict) : 업데이트할 값 (default=None)
        db_name         (str)  : 업데이트할 document가 속한 MongoDB database 이름 (default=None)
        collection_name (str)  : 업데이트할 document가 속한 MongoDB collection 이름 (default=None)

        [Returns]
        UpdateResult : 업데이트 결과를 저장하고 있는 UpdateResult 객체

        [Exceptions]
        매개변수 db_name과 매개변수 collection_name이 존재하지 않는 경우
        """
 
        # condition이 dict 타입이 아니거나 입력되지 않은 경우, 예외를 발생
        # 업데이트 조건이 없을 경우, 업데이트가 진행되지 않음
        if (condition is None) or (not isinstance(condition, dict)):
            raise Exception("Need to condition")

        # update_value가 입력되지 않은 경우, 예외를 발생
        # 업데이트 값이 없을 경우, 빈 값으로 업데이트하지 않음
        if update_value is None:
            raise Exception("Need to update value")

        # db_name이 입력되지 않았거나, collenction_name이 입력되지 않았을 경우, 예외를 발생
        if (db_name is None) or (collection_name is None):
            raise Exception("Need to param db_name, collection_name")

        # MongoDB의 updateOne 명령어는 PyMongo에서 update_one으로 지원
        # 업데이트 결과를 저장하고 있는 UpdateResult 객체를 리턴
        # upsert 옵션을 True(default)로 지정하여 매칭되는 Document가 없을 경우, Insert를 진행
        return self._client[db_name][collection_name].update_one(filter=condition, update=update_value, upsert=True)

    def update_items(self, condition:dict=None, update_value:dict=None, db_name:str=None, collection_name:str=None):
        """
        MongoDB에 다수의 document를 업데이트하기 위한 메소드이다.
        
        [Parameters]
        condition       (dict) : 업데이트할 document들에 대한 검색 조건 (default=None)
        update_value    (dict) : 업데이트할 값 (default=None)
        db_name         (str)  : 업데이트할 document들이 속한 MongoDB database 이름 (default=None)
        collection_name (str)  : 업데이트할 document들이 속한 MongoDB collection 이름 (default=None)

        [Returns]
        UpdateResult : 업데이트 결과를 저장하고 있는 UpdateResult 객체

        [Exceptions]
        매개변수 db_name과 매개변수 collection_name이 존재하지 않는 경우
        """
 
        # condition이 dict 타입이 아니거나 입력되지 않은 경우, 예외를 발생
        # 업데이트 조건이 없을 경우, 업데이트가 진행되지 않음
        if condition is None or not isinstance(condition, dict):
            raise Exception("Need to condition")

        # update_value가 입력되지 않은 경우, 예외를 발생
        # 업데이트 값이 없을 경우, 빈 값으로 업데이트하지 않음
        if update_value is None:
            raise Exception("Need to update value")

        # db_name이 입력되지 않았거나, collenction_name이 입력되지 않았을 경우, 예외를 발생
        if db_name is None or collection_name is None:
            raise Exception("Need to param db_name, collection_name")
        
        # MongoDB의 updateMany 명령어는 PyMongo에서 update_many로 지원
        # 업데이트 결과를 저장하고 있는 UpdateResult 객체를 리턴
        # upsert 옵션을 True(default)로 지정하여 매칭되는 Document가 없을 경우, Insert를 진행
        return self._client[db_name][collection_name].update_many(filter=condition, update=update_value, upsert=True)

    def aggregate(self, pipeline:list=None, db_name:str=None, collection_name:str=None):
        """
        MongoDB에 aggregate(집계)를 위한 메소드이다.
        
        [Parameters]
        pipeline        (list) :  document들에 대한 집계 조건(dict)들이 저장된 list (default=None)
        db_name         (str)  : 업데이트할 document들이 속한 MongoDB database 이름 (default=None)
        collection_name (str)  : 업데이트할 document들이 속한 MongoDB collection 이름 (default=None)

        [Returns]
        CommandCursor : CommandCursor 객체

        [Exceptions]
        매개변수 db_name과 매개변수 collection_name이 존재하지 않는 경우
        """
   
        # pipeline이 list 타입이 아니거나 입력되지 않은 경우, 예외를 발생
        if pipeline is None or not isinstance(pipeline, list):
            raise Exception("Need to pipeline") 

        # db_name이 입력되지 않았거나, collenction_name이 입력되지 않았을 경우, 예외를 발생
        if db_name is None or collection_name is None:
            raise Exception("Need to param db_name, collection_name")

        # MongoDB의 aggregate 명령어는 PyMongo에서도 aggregate로 지원
        # 집계 결과를 저장하고 있는 CommandCursor 객체를 리턴           
        return self._client[db_name][collection_name].aggregate(pipeline)

    def text_search(self, text:str=None, db_name:str=None, collection_name:str=None):
        """
        MongoDB에 텍스트 검색을 위한 메소드이다.
        
        [Parameters]
        text            (str) : 검색하고자 하는 문자열 (default=None)
        db_name         (str) : 검색할 텍스트가 document들이 속한 MongoDB database 이름 (default=None)
        collection_name (str) : 검색할 텍스트가 있는 document들이 속한 MongoDB collection 이름 (default=None)

        [Returns]
        Cursor : 검색 결과에 대한 Cursor 객체

        [Exceptions]
        - text가 None이거나 str 타입이 아닌 경우
        - 매개변수 db_name과 매개변수 collection_name이 존재하지 않는 경우
        """

        # text가 str 타입이 아니거나 입력되지 않은 경우, 예외를 발생
        if text is None or not isinstance(text, str):
            raise Exception("Need to text") 

        # db_name이 입력되지 않았거나, collenction_name이 입력되지 않았을 경우, 예외를 발생
        if db_name is None or collection_name is None:
            raise Exception("Need to param db_name, collection_name")

        # 텍스트 검색은 MongoDB의 find 명령어를 이용 ($text 연산자와 $search 연산자 이용)
        return self._client[db_name][collection_name].find({"$text": {"$search": text}})
 