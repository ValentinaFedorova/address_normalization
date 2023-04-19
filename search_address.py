

from natasha import (
    Segmenter,
    Doc
)
import re
import pandas as pd
import datetime
import numpy as np
import datetime
from razdel import tokenize, sentenize
import psycopg2
#from nltk.stem.snowball import SnowballStemmer


class AddrItem(object):
    def __init__(self, name="Unknown name", code="Unknown major", socr="Unknown major"):
        self.name = name
        self.Index = code
        self.socr = socr


def get_fias_data_by_region(database, host, user, password, port, region_code, table_name):
    global df_cities
    global df_cities_parts
    global df_subjects
    cnxn = psycopg2.connect(database=database,
                            host=host,
                            user=user,
                            password=password,
                            port=port)
    df_cities = pd.read_sql('SELECT NAME,CODE,SOCR FROM ' + table_name + ' where code like \'' + region_code + '%\' order by NAME',cnxn, index_col="code")
    df_obj = df_cities.select_dtypes(['object'])
    df_cities[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    df_cities[df_obj.columns] = df_cities[df_obj.columns].apply(lambda x: x.str.lower())
    df_cities['name'] = df_cities['name'].astype(str)
    df_cities['parts'] = df_cities['name'].apply(parts_of_name) 
    cities_parts_array = []
    for row in df_cities.itertuples():
        row_name_parts = word_base_array(parts_of_name(row.name))
        row_code = row.Index
        for i in range(len(row_name_parts)):
            row_dict = {}
            row_dict['name'] = row_name_parts[i]
            row_dict['code'] = row_code + str(i+1)
            row_dict['socr'] = row.socr
            row_dict['full_city'] = row
            cities_parts_array.append(row_dict)    
    df_cities_parts = pd.DataFrame(cities_parts_array)   
    df_cities_parts = df_cities_parts.set_index('code')     
    df_subjects = df_cities.filter(regex='\d\d00000000000', axis=0)
    cnxn.close()

def get_part_of_speech(word):
    vowels='уеыаоэиюя'
    adjective_suffixes = ["ого","его","ому","ему","ой","ый","ым","им","ом","ем","ей","ой",
                          "ую","юю","ое","ее","ая","яя","ий","ья","ье","ые"]
    suffix_2 = word[-2:]
    suffix_3 = word[-3:]
    if suffix_2 in adjective_suffixes:
        return 'ADJV'
    elif suffix_3 in adjective_suffixes:
        return 'ADJV'
    end=''
    for i in range(len(word)-1,0,-1):
        if word[i] in vowels:
            end+=word[i]
        else:
            break
    if end in ['а','e','и']:
        return 'NOUN'
    return 'UNDEF'
    

def lemmatize_sent(sent, comma_flag):
    sent = sent.lower()
    sent = sent.replace('р-н','район')
    sent = sent.replace('р-он','район')
    sent = sent.replace(' р.п. ',' рп ')
    sent = sent.replace('ё','е')
    sent = sent.replace('пр-кт','проспект')
    sent = sent.replace('город-курорт','город')
    sent = sent.replace('эсто-садок','эстосадок')
    sent = sent.replace('б-р','бульвар')
    sent = re.sub(r'[^а-я,\d]',' ', sent).strip()
    if comma_flag:
        sent_groups = sent.split(',')
    else:
        sent_groups = [sent]
    sent_groups_tokens = []
    for group in sent_groups:
        doc = Doc(group)
        doc.segment(segmenter)
        tokens = []
        for t in doc.tokens:
            tk = t.text
            if tk not in symbols and tk not in stop_words:
                mas = tk.split('-')
                for el in mas:
                    tokens.append(el) 
        if len(tokens)>0:
            sent_groups_tokens.append(tokens)
    return sent_groups_tokens



def word_base(word):
    index = len(word)
    for i in range(len(word)-1,0,-1):
        if word[i] in vowels:
            index = i
        else:
            break
    return word[:index]


def word_base_array(words):
    res = []
    for word in words:
        index = len(word)
        for i in range(len(word)-1,0,-1):
            if word[i] in vowels:
                index = i
            else:
                break
        res.append(word[:index].lower())
    return res

def parts_of_name(word):
    return re.sub(r'-',' ',word.strip()).split()

def check_if_real_estate(address):
    real_estate = re.search(r'участок|участки|уч\.|(лесничество.*(квартал|выдел))|(квартал.*выдел)|з/у', address)
    if real_estate is not None:
        return True
    return False




  
def possible_address_objects(tokens,obj_df):
    found = pd.DataFrame()
    for t in tokens:
        if len(t) > 1 and not t.isdigit():
            found = pd.concat([found,obj_df.query('name.str.contains("'+t+'")',engine = 'python')])

    return found

def search_object_by_code(code,obj_df):
    found = obj_df.query('code.str.startswith("'+code+'")',engine = 'python')
    return found

      
def to_lower_case(values):
    res = []
    for v in values:  
        res.append(v.lower()) 
    return res     


def get_next_token(tokens,pos):
    for i in range(pos+1, len(tokens)):
        if word_base(tokens[i]) in key_words:
            return ''
        if word_base(tokens[i]) not in key_words and len(tokens[i]) > 1 and not tokens[i].isdigit():
            return tokens[i]
    return ''

def get_prev_token(tokens,pos):
    for i in range(min(pos-1, len(tokens)-1), -1, -1):
        if word_base(tokens[i]) in key_words:
            return ''
        if word_base(tokens[i]) not in key_words and len(tokens[i]) > 1 and not tokens[i].isdigit():
            return tokens[i]
    return ''

def check_equals_parts(kladr_items, current_token, croped_current_token, current_token_group, croped_current_token_group, current_index, group_flag):
    checked_kladr_items = []
    checked_result = {}
    for k_row in kladr_items.itertuples():
        row = k_row.full_city
        parts = row.parts
        parts_amount = len(parts)
        if parts_amount>1:
            if current_index < 0:
                startindex = 0
                endindex = len(current_token_group)
            else:
                startindex = max(0, current_index - parts_amount - 1)
                endindex = min(len(current_token_group),current_index + parts_amount + 1)
            parts_cnt = 0
            for s in range(startindex,endindex):
                for j in range(parts_amount):
                    if croped_current_token_group[s] == word_base(parts[j]) and get_part_of_speech(current_token_group[s]) == get_part_of_speech(parts[j]):
                        parts_cnt += 1
            if parts_cnt == parts_amount and row not in checked_kladr_items: 
                checked_kladr_items.append(row)
        else:
            if current_token!="":
                if croped_current_token == word_base(row.parts[0]) and get_part_of_speech(current_token) == get_part_of_speech(row.parts[0]) and row not in checked_kladr_items:
                    checked_kladr_items.append(row)
            else:
                for i in range(len(current_token_group)):
                    if croped_current_token_group[i] == word_base(row.parts[0]) and get_part_of_speech(current_token_group[i]) == get_part_of_speech(row.parts[0]) and row not in checked_kladr_items:
                        checked_kladr_items.append(row)
    if len(checked_kladr_items) > 0:
        if group_flag:
            checked_result[(tuple(current_token_group), current_index)] = checked_kladr_items
        else:
            checked_result[(current_token, current_index)] = checked_kladr_items
    return checked_result

def check_multiple_variants(kladr_possible_items):
    checked = kladr_possible_items
    # проверка на максимально длинное составное название
    if len(checked) > 1:
        max_parts_len = 0  
        checked_by_parts_len = []
        for row in checked:
            if len(row.parts) > max_parts_len:
                max_parts_len = len(row.parts)
        if max_parts_len > 1:
            for row in checked:
                if len(row.parts) == max_parts_len:
                    checked_by_parts_len.append(row)
        if len(checked_by_parts_len) > 0:
            checked = checked_by_parts_len

    # # проверка по названию 
    # if len(checked) > 1:
    #     checked_by_name = []
    #     for key, kladr_items in kladr_possible_items.items():
    #         token_group = list(key)[0]
    #         kw_index = list(key)[1]
    #         for row in checked:
    #             if row.name in token_group:
    #                 checked_by_name.append(row)
    #     if len(checked_by_name) > 0:
    #         checked = checked_by_name

    # проверка на актуальность
    actual = []
    for c in checked:
        try:
            if c.Index[-2:] == '00':
                actual.append(c)
        except:
            print(c)

    if len(actual) > 0:
        checked = actual

    # проверка на самое длинное название 
    if len(checked) > 1:
        max_len = -1
        for c in checked:
            if len(c.name) > max_len:
                max_len = len(c.name)
        if max_len > 0:
            checked = [x for x in checked if len(x.name)==max_len]
        
    return checked


def getitemsBykeyword(regcities, tokens, keywords, group_flag):
    kladr_possible_items_all = {}
    curkeywords=[]
    croped_tokens = word_base_array(tokens)
    checked = []
    max_len = 0
    for i in range(len(tokens)):
        kladr_items = pd.DataFrame()
        
        if tokens[i] in keywords:
            if tokens[i] in small_city_area:
                curkeywords = small_city_area
            elif tokens[i] in villagies_key_words:
                curkeywords = villagies_key_words 
            elif tokens[i] in city_key_word:
                curkeywords = city_key_word
            elif tokens[i] in district_key_words:
                curkeywords = district_key_words
            else:
                continue
            if i<len(tokens)-1:    
                cur_token=get_next_token(tokens,i)
                wbcur_token = word_base(cur_token)
                if wbcur_token!='' and len(wbcur_token) > 1 and not wbcur_token.isdigit():
                    kladr_items = regcities.query( "socr ==  @curkeywords and name == '"+wbcur_token+"'",engine = 'python')
               
            if i==len(tokens)-1 or len(kladr_items)==0:
                cur_token = get_prev_token(tokens,i)
                wbcur_token = word_base(cur_token)
                if wbcur_token!='' and len(wbcur_token) > 1 and not wbcur_token.isdigit():
                    kladr_items = regcities.query( "socr == @curkeywords and name == '"+ wbcur_token +"'",engine = 'python')
            
            if group_flag:
                kladr_possible_items = check_equals_parts(kladr_items, cur_token, wbcur_token, tokens, croped_tokens, i, True) 
            else:
                kladr_possible_items = check_equals_parts(kladr_items, cur_token, wbcur_token, tokens, croped_tokens, i, False)

            kladr_possible_items_all.update(kladr_possible_items)


    
    if not kladr_possible_items_all:
        checked = []
    elif len(list(kladr_possible_items_all.values())[0]) > 1 or len(kladr_possible_items_all)>1:
        if group_flag:
            checked = check_multiple_variants([x for x in kladr_possible_items_all.values()][0])
        else:
            for v in kladr_possible_items_all.values():
                checked += check_multiple_variants(v)
    else:
        checked = [x[0] for x in kladr_possible_items_all.values()]  
    

    return checked

def getitems(regcities, tokens, group_flag):

    kladr_possible_items_all = {}
    keywords = ["ао","аобл","край","обл","респ"]
    checked = []
    for group in tokens:
        group = [x for x in group if len(x) > 2 and x not in address_key_words]
        croped_tokens = word_base_array(group)
    
        words=[]
        for i in range(len(group)):
            kladr_items = pd.DataFrame()
            if group[i] not in key_words and len(group[i]) > 1 and not group[i].isdigit(): 
                cur_token=word_base(group[i])
            else:
                continue
            words.append("name == '"+cur_token+"'")
        
        strwords =" or ".join(words)
        if strwords!="":
            kladr_items = regcities.query("socr != @keywords and ("+strwords+" )",engine = 'python')

            if len(kladr_items) > 0:
                if group_flag:
                    kladr_possible_items = check_equals_parts(kladr_items, "", "", group, croped_tokens, -1, True)
                else:
                    kladr_items = kladr_items.sort_values(by='name')
                    kladr_items_grouped = kladr_items.groupby('name').groups
                    for croped_token, v in kladr_items_grouped.items():
                        possible_items_by_token = kladr_items.filter(items = v.tolist(), axis=0)
                        token = [el for el in possible_items_by_token.iloc[0].full_city.parts if croped_token in el][0]
                        kladr_possible_items = check_equals_parts(possible_items_by_token, token, croped_token, group, croped_tokens, -1, False)
                        kladr_possible_items_all.update(kladr_possible_items)
                kladr_possible_items_all.update(kladr_possible_items)

    if not kladr_possible_items_all:
        checked = []
    elif len(list(kladr_possible_items_all.values())[0]) > 1 or len(kladr_possible_items_all)>1:
        if group_flag:
            checked = check_multiple_variants(list(kladr_possible_items_all.values())[0])
        else:
            for v in kladr_possible_items_all.values():
                checked += check_multiple_variants(v)
    else:
        checked = [x for x in kladr_possible_items_all.values()][0]

    return checked

def get_cities_by_region(found_area):
    areaquery=""
    cnt=0
    for row in found_area:
        cnt+=1
        area_code = row.Index[:2]
        if areaquery!='':
            areaquery+=' or '
        # areaquery+="CODE.str.startswith('"+area_code+"')"
        areaquery=area_code+"\d{11}"
    if areaquery!='':
        regcities = df_cities_parts.filter(regex=areaquery, axis=0)
    else:
        regcities = df_cities_parts  
    return regcities

def search_region(tokens):
    excludes=[]
    areas = pd.DataFrame()
    areas_array = []
    croped_tokens = word_base_array(tokens)

    for i in range(len(tokens)):
        if tokens[i] == 'обл' or tokens[i] == 'область' or tokens[i] == 'край':
            excludes.append(i)
            if i > 0:
                if tokens[i-1] not in address_key_words:
                    reg = possible_address_objects([tokens[i-1]],df_subjects)
                    if len(reg)>0:
                        areas = pd.concat([areas,reg])
                        excludes.append(i-1)
            if i + 1 < len(tokens):
                if tokens[i+1] not in address_key_words:
                    reg = possible_address_objects([tokens[i+1]],df_subjects)
                    if len(reg)>0:
                        areas = pd.concat([areas,reg])
                        excludes.append(i+1)
        elif tokens[i] == 'хмао':
            regobj = AddrItem('ханты-мансийский автономный округ - югра','8600000000000', 'ао')
            areas_array.append(regobj)
        elif tokens[i] == 'рх':
            regobj = AddrItem('хакасия','1900000000000', 'респ')
            areas_array.append(regobj) 
        elif tokens[i] == 'рмэ':
            regobj = AddrItem('марий эл','1200000000000', 'респ')
            areas_array.append(regobj)
        elif tokens[i] == 'янао':
            regobj = AddrItem('ямало-ненецкий','8900000000000', 'ао')
            areas_array.append(regobj)
        elif tokens[i] == 'респ' or tokens[i] == 'республика':
            #check
            excludes.append(i)
            startindex = max(0,i-2)
            endindex = min(len(tokens),i+3)
            variances = []
            for j in range(startindex,endindex):
                if i != j:
                    parts = parts_of_name(tokens[j])
                    for p in parts:
                        if p not in address_key_words and len(p) > 1 and not p.isdigit():
                            variances.append(p)
            res = word_base_array(variances)
            reg = possible_address_objects(res,df_subjects)

            areas_array = check_equals_parts(reg, '', '', tokens, croped_tokens, -1, True)

        if len(areas)> 0 or len(areas_array)>0:
            break
                
    for row in areas.itertuples():
        areas_array.append(row)

    if len(areas_array) > 1:
        areas_array = check_multiple_variants(areas_array)
     

    return areas_array


def search_district(tokens, regcities):
    cities_socr_rn=getitemsBykeyword(regcities,tokens, ['р-н', 'район','р'], True)
    return cities_socr_rn


def search_city(tokens, regcities, found_district, group_flag):   

    excludes=['ул', 'улица', 'проспект', 'бульвар', 'пер', 'переулок', 'район', 'дом', 'д']
    possible_cities = []
    cities_socr_g = []
    cities_socr_snt = []
    cities_socr_vlg = []
    villagies = []
    snts=[]
    cities = []
    unprocessed_tokens = tokens.copy()
    if group_flag:
        for group in tokens:
            if list(set(group) & set(excludes)):
                continue
            cities+=getitemsBykeyword(regcities,group, ['г', 'город','гор']+small_city_area + villagies_key_words, True)
            if len(cities) > 0:
                unprocessed_tokens.remove(group)
    else:
        cities+=getitemsBykeyword(regcities,tokens, ['г', 'город','гор']+small_city_area + villagies_key_words, False)
    for city_el in cities:
        if city_el.socr == 'г' and city_el not in cities_socr_g:
            cities_socr_g.append(city_el)
        elif city_el.socr in small_city_area and city_el not in cities_socr_snt:
            cities_socr_snt.append(city_el)
        elif city_el.socr in villagies_key_words and city_el not in cities_socr_vlg:
            cities_socr_vlg.append(city_el)
    if group_flag:
        unprocessed_tokens = [x for x in unprocessed_tokens if len(set(x)&set(excludes))==0]    
    else:
        unprocessed_tokens = [unprocessed_tokens]
    if len(unprocessed_tokens) > 0:  
        if len(cities_socr_vlg) == 0 or len(cities_socr_snt) == 0:
            if group_flag:
                possible_cities = getitems(regcities, unprocessed_tokens, True)
            else:
                possible_cities = getitems(regcities, unprocessed_tokens, False)
            for possible_obj in possible_cities:
                if possible_obj.socr in villagies_key_words and possible_obj not in cities_socr_vlg:
                    cities_socr_vlg.append(possible_obj)
                elif possible_obj.socr in small_city_area and possible_obj not in cities_socr_snt:
                    cities_socr_snt.append(possible_obj)
                elif possible_obj.socr in city_key_word and possible_obj not in cities_socr_g:
                    cities_socr_g.append(possible_obj)
            possible_cities = []
        
        # Если нашлось СНТ, то осуществляем проверку на наличие данного СНТ в городе
        if len(cities_socr_snt) > 0:
            for city in cities_socr_g:
                city_code=city.Index[:8]
                cur_snt = list(filter(lambda x: x.Index[:8] == city_code, cities_socr_snt))
                snts=snts+cur_snt
            if len(snts)==0:
                snts=cities_socr_snt

    # Проверка наличия деревни в найденном районе 
    for rn in found_district:
        rn_code=rn.Index[:5]
        cur_vlgs = list(filter(lambda x: x.Index[:5] == rn_code, cities_socr_vlg))
        villagies=villagies+cur_vlgs
    if len(villagies)==0:
        villagies=cities_socr_vlg   
   
    if len(villagies) == 0 and len(snts) == 0 and len(cities_socr_g) == 0 and len(unprocessed_tokens) > 0:
        if group_flag:
            possible_cities = getitems(regcities, unprocessed_tokens, True)
        else:
            possible_cities = getitems(regcities, unprocessed_tokens, False)
    
    cities_wo_keys = []    
    if len(possible_cities) > 0 and len(found_district) > 0:
        for rn in found_district:
            rn_code=rn.Index[:5]    
            cur_cities = list(filter(lambda x: x.Index[:5] == rn_code, possible_cities))
            cities_wo_keys=cities_wo_keys+cur_cities
        

    if len(possible_cities) > 0 and len(found_district) == 0:
        for p in possible_cities:
            if p.socr == 'р-н' and p not in found_district:
                found_district.append(p)
        cities_wo_keys = possible_cities

    return found_district,cities_socr_g,snts,villagies,cities_wo_keys
                

       
def normalize_address(address, current_region_code):

    result = {}
    t1 = datetime.datetime.now()
    region = ''
    region_code = ''
    district = []
    district_code = []
    city = []
    city_code = []
    locality = []
    locality_code = []
    snt = []
    snt_code = []
    
    str_address = re.sub(r'\d{2}:\d{2}:\d{5,9}:\d{3,4}','',address)
    str_address = re.sub(r'\d{6}','',str_address)
    
    tokens = lemmatize_sent(str_address, True) 
    if len(tokens) == 0:
        return None
    areas = []
    districts = []
    cities = []
    small_cities = []
    villagies = []
    possible_cities = []
    tokens_wo_excludes = tokens.copy()
    for group in tokens:
        group_areas = search_region(group)
        if len(group_areas) > 0:
            areas += group_areas
            tokens_wo_excludes.remove(group)

    if len(areas) != 1:
        areas += [row for row in search_object_by_code(current_region_code, df_subjects).itertuples()]
        tokens_wo_excludes = tokens.copy()
    
    cities_by_region = get_cities_by_region(areas)
    tokens = tokens_wo_excludes.copy()
    for group in tokens:
        group_districts = search_district(group, cities_by_region)
        if len(group_districts) > 0:
            districts += group_districts
            tokens_wo_excludes.remove(group)


    districts,cities,small_cities,villagies,possible_cities = search_city(tokens_wo_excludes, cities_by_region, districts, True)

    if len(cities) == 0 and len(small_cities) == 0 and len(villagies) == 0 and len(possible_cities) == 0:
        all_tokens = lemmatize_sent(str_address, False)
        all_tokens = [x for x in all_tokens[0] if x != areas[0].name and x != areas[0].socr]
        districts,cities,small_cities,villagies,possible_cities = search_city(all_tokens, cities_by_region, districts, False)

    



    if len(areas) == 1:  
        region = areas[0].name
        region_code = areas[0].Index
        
    
    for item in districts:
        district.append(item.name)
        district_code.append(item.Index)


    for item in cities:
        city.append(item.name)
        city_code.append(item.Index)
        if len(cities)==1 and region_code=='':
            area_code = item.Index[:2]
            areas_df = df_subjects.query('code.str.startswith("'+area_code+'")',engine = 'python')
            for row in areas_df.itertuples():
                region = row.name
                region_code = row.Index
    for item in small_cities:
        snt.append(item.name)
        snt_code.append(item.Index)
        if len(small_cities)==1 and region_code=='':
            area_code = item.Index[:2]
            areas_df = df_subjects.query('code.str.startswith("'+area_code+'")',engine = 'python')
            for row in areas_df.itertuples():
                region = row.name
                region_code = row.Index
    for item in villagies:
        locality.append(item.name)
        locality_code.append(item.Index)
        if len(villagies)==1 and region_code=='':
            area_code = item.Index[:2]
            areas_df = df_subjects.query('code.str.startswith("'+area_code+'")',engine = 'python')
            for row in areas_df.itertuples():
                region = row.name
                region_code = row.Index

    for item in possible_cities:
        if item.socr in city_key_word and item.name not in city:
            city.append(item.name)
            city_code.append(item.Index)
        elif item.socr in villagies_key_words and item.name not in locality:
            locality.append(item.name)
            locality_code.append(item.Index)
        elif item.socr in small_city_area and item.name not in snt:
            snt.append(item.name)
            snt_code.append(item.Index)

        if len(possible_cities)==1 and region_code=='':
            area_code = item.Index[:2]
            areas_df = df_subjects.query('code.str.startswith("'+area_code+'")',engine = 'python')
            for row in areas_df.itertuples():
                region = row.name
                region_code = row.Index
    
    strcity =", ".join(city)
    str_city_code =", ".join(city_code)
    
    strdistrict =", ".join(district)
    str_district_code =", ".join(district_code)

    strlocality =", ".join(locality)
    str_locality_code =", ".join(locality_code)

    strsnt =", ".join(snt)
    str_snt_code =", ".join(snt_code)

    t2 = datetime.datetime.now()
    dt = t2 - t1
    print(dt.total_seconds())
    if region != '' or strdistrict != '' or strcity != '':
        result['region'] = region
        result['region_code'] = region_code
        result['strdistrict'] = strdistrict
        result['str_district_code'] = str_district_code
        result['strcity'] = strcity
        result['str_city_code'] = str_city_code
        result['strlocality'] = strlocality
        result['str_locality_code'] = str_locality_code
        result['strsnt'] = strsnt
        result['str_snt_code'] = str_snt_code
        return result
    return None



vowels = ['а','у','о','ы','и','й','э','я','ю','ё','е']
symbols = ":().,!;'?-\"@#$%^&*+ "
key_words = ["г","обл","адрес","росреестр","ул","д","кв","республик","рх","рп","р","н",
             "корп","федерац","область","округ","город","улиц","квартир","п","пер","мкр",
             "муниципальн","район","дер","республика","улица"]

stop_words = ["российская","федерация","муниципальный","рб","жилое","помещение","проектный",
              "номер","этаже","подъезде","общей","проектной","площадью","учётом","холодных",
              "помещений","квартира","однокомнатная","расположенная","этажного","жилого",
              "находящаяся","находящегося","кадастровый","земельного","участка","управление",
              "рф","адресу"]

address_key_words = ['обл','область','край','респ','республика','город','г','гор','поселок',
                         'посёлок','пгт','днт','снт','рп','дп','кп','нп',"ул","улиц","улица","проспект",
                         "пр-кт","мкр","кв","квартира","к","дер","дом"]

subjects = ["ао","аобл","край","обл","респ"]
small_city_area = ["дп","кп","рп","нп","снт","днт","тер. днт","тер. снт","тер. тсн","тер", "жст"]
villagies_key_words = ["пгт","с/мо","с/п","д","п","с","с/о","дер", "аул", "пос", "поселок", "посёлок", "деревня" , "село"]
city_key_word = ["г","гор","город"]
district_key_words = ["р","район","р-н"]

#Для Наташи
segmenter = Segmenter()

df_cities = pd.DataFrame()
df_cities_parts = pd.DataFrame()
df_subjects = pd.DataFrame()


region_code = '50'
# Грузим данные о населенных пунктах
get_fias_data_by_region(database="kladr", host="localhost", user="postgres", password="ok", port="5432", region_code=region_code, table_name="public.kladr")
                


reg_objects_table = 'moscow.zone_50_14_address'

result_table = 'moscow.zone_50_14_processed'

reg_conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="ok",
                        port="5432")

# df_objects_sochi = pd.read_sql('SELECT address, id FROM public.rosreestr_not_living_obj_kladr where processed is null order by id',sochi_conn)
df_reg_objects = pd.read_sql('SELECT address, id FROM ' + reg_objects_table + ' where processed is null order by id',reg_conn)
# df_reg_objects = pd.read_sql('SELECT address, id FROM ' + reg_objects_table + ' where id = 14248 order by id',reg_conn)
cursor = reg_conn.cursor() 
reg_objects_list = df_reg_objects.values.tolist()
for obj in reg_objects_list:
    curr_addr = obj[0]
    curr_id = obj[1]
    print(curr_id)
    reason = ""
    cad_num = ""
    processed_value = 1
    
    if curr_addr is None or curr_addr=="":
        reason = 'пустой адрес'
        processed_value = -1
    elif check_if_real_estate(curr_addr.lower()):
        reason = 'участок'
        processed_value = -1
    if curr_addr is not None:
        cad_num = re.findall(r'\d{2}:\d{2}:\d{5,9}:\d{2,4}', curr_addr)
        if len(cad_num)>0:
            cad_num = ','.join(list(set(cad_num)))
        else:
            cad_num = ""
    if processed_value!=-1:
        found_items = normalize_address(curr_addr, region_code) 
        if found_items['strcity'] == '' and found_items['strlocality'] == '' and found_items['strsnt'] == '':
            reason = 'отсутствует населенный пункт'
            processed_value = -1
        if found_items is not None:
            postgres_insert_query = "insert into " + result_table + " (obj_id, address, processed, region, region_code, district, district_code, city, city_code, locality, locality_code, snt, snt_code, reason, cad_num) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            record_to_insert = (curr_id, curr_addr, processed_value,found_items['region'],found_items['region_code'],found_items['strdistrict'],found_items['str_district_code'],found_items['strcity'],found_items['str_city_code'],found_items['strlocality'],found_items['str_locality_code'],found_items['strsnt'],found_items['str_snt_code'], reason, cad_num)
            cursor.execute(postgres_insert_query, record_to_insert)
            print(curr_id, ' ', curr_addr,' region: ', found_items['region'],' district: ',found_items['strdistrict'],' city: ',found_items['strcity'],' locality: ',found_items['strlocality'],' snt: ',found_items['strsnt'], ' reason: ',reason,' cad_num: ', cad_num)
    else:
        postgres_insert_query = "insert into " + result_table + " (obj_id, address, processed, reason, cad_num) values (%s,%s,%s,%s,%s) "
        record_to_insert = (curr_id, curr_addr, processed_value, reason, cad_num)
        cursor.execute(postgres_insert_query, record_to_insert)
        print(curr_id, ' ', curr_addr, ' reason: ',reason,' cad_num: ', cad_num)

    
    postgres_update_query = " update " + reg_objects_table + " set processed = %s where id = %s"
    cursor.execute(postgres_update_query, (1, curr_id))
    reg_conn.commit()    



cursor.close()
reg_conn.close()

print('Done')
     
 


