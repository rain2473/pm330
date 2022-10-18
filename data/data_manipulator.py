
# Author  : 이병헌
# Contact : lww7438@gmail.com
# Date    : 2022-10-18(화)



# Function Declaration

def set_query_url(service_url:str, params:dict):
    """
    요청 주소와 파라미터들을 입력받아 REST API Server에 전송할 URL을 구성하여 반환한다.

    [Parameters]
    service_url (str)  : 서비스 URL
    params      (dict) : REST로 요청할 매개변수와 값

    [Returns]
    str : REST API Server에 전송할 URL
    """

    # Set URL with Parameters
    request_url = service_url + '?'    
    for k, v in params.items():
        request_url += str(k) + '=' + str(v) + '&'

    return request_url[:-1] # Eliminate last '&' character 

def filter_params(data_list:list, params:list):
    """
    dict들의 list로 구성된 data_list에서 params 매개변수 값들만 추출하여 반환한다.

    [Parameters]
    data_list (list) : 원본 데이터 (list of dict)
    params    (list) : 추출할 매개변수의 이름들이 저장된 리스트
    
    [Returns]
    list : data_list에서 params에 명시된 매개변수만 추출한 리스트 (list of dict)
    """

    filtered_list=[]

    for data in data_list:
        new_dict = dict()

        for k, v in data.items():
            if k in params:
                new_dict[k] = v
            else:
                continue
        
        filtered_list.append(new_dict)

    return filtered_list

def left_join_by_key(ldata:list, rdata:list, key:str):
    """
    dict들의 list로 구성된 두 list(ldata, rdata)를 병합하되,
    같은 매개변수에 대해서 다른 값이 존재할 경우, ldata의 값으로 유지하여 반환한다.

    [Parameters]
    ldata (list) : 좌측 데이터 (list of dict)
    rdata (list) : 우측 데이터 (list of dict)
    key   (str)  : 병합 과정에서 Key 역할을 할 매개변수
    
    [Returns]
    list : 병합된 데이터 (list of dict)
    """

    merged_list = []

    for data in ldata:
        merged_list.append(data)

    for item in merged_list:
        for data in rdata:
            if item[key] == data[key]:
                for k, v in data.items():
                    item[k] = v

    return merged_list
