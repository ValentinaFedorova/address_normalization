
from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsNERTagger,
    Doc
)
import re
import pandas as pd
import datetime
import psycopg2

segmenter = Segmenter()
# morph_vocab = MorphVocab()
emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)
ner_tagger = NewsNERTagger(emb)

vowels = ['а','у','о','ы','и','й','э','я','ю','ё','е']
symbols = "().,!;'?-\"@#$%^&*+ "
key_words = ["г","обл","адрес","росреестр","ул","д","кв","республик","рх","рп","р","н",
             "корп","федерац","область","округ","город","улиц","квартир","п","пер","мкр",
             "муниципальн","район","дер","республика","улица"]

stop_words = ["федерация","муниципальный","рб","жилое","помещение","проектный",
              "номер","этаже","подъезде","общей","проектной","площадью","учётом","холодных",
              "помещений","квартира","однокомнатная","расположенная","этажного","жилого",
              "находящаяся","находящегося","кадастровый","земельного","участка","управление",
              "рф","адресу","адрес","росреестр","росреестра","республика","респ","край"]

street_key_words = ["ул","улица","улице","пр","проспект","пл","площадь","аллея","кв-л","пер",
                        "переулок","мкр","микрорайон", "р","район"]
street_kw = ["ул","улица","улице"]
avenue_kw = ["пр","проспект"]
square_kw = ["пл","площадь"]
side_street_kw = ["пер","переулок"]
city_district_kw = ["р","район"]
mcrs = ["мкр","микрорайон"]

cnxn = psycopg2.connect(database="kladr",
                        host="localhost",
                        user="postgres",
                        password="ok",
                        port="5432")

sochi_conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="ok",
                        port="5432")

streets_dict = {}    



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
    d = {ord(x):"" for x in symbols}
    return [x.translate(d) for x in re.sub(r'-',' ',word.strip()).split()]

def lemmatize_sent(sent,excludes):
    sent = sent.lower()
    sent = sent.replace('..','.')
    sent = sent.replace('р-н','район')
    sent = sent.replace('пр-кт','проспект')
    sent = sent.replace('городской округ','')
    sent = sent.replace('город','')
    #sent = sent.replace(' р.п. ',' рп ')
    sent_groups = sent.split(',')
    sent_groups_tokens = []
    for group in sent_groups:
        # if list(set(group.split(' ')) & set(excludes)):
        #     continue
        doc = Doc(group)
        doc.segment(segmenter)
        tokens = []
        for t in doc.tokens:
            tk = t.text
            if tk not in symbols and tk not in stop_words:
                mas = tk.split('-')
                for el in mas:
                    if el not in excludes:
                        tokens.append(el) 
        if len(tokens)>0:
            sent_groups_tokens.append(tokens)
    return sent_groups_tokens

  

#загрузка улиц по ключевым словам 
df_area_street = pd.read_sql("SELECT name,socr,code FROM public.street where lower(name) like '%край%' and lower(name) not like '%крайняя%' and lower(name) not like '%крайний%' and lower(name) not like '%окрайная%' and lower(name) not like '%крайние%' and lower(name) not like '%крайная%' and lower(name) not like '%крайнея%' and lower(name) not like '%крайнюка%' and lower(name) not like '%окрайный%' and lower(name) not like '%крайнюковская%' and lower(name) not like '%крайновых%' and lower(name) not like '%макрай%' and lower(name) not like '%крайникова%'" ,cnxn, index_col="code")
df_republic_street = pd.read_sql("SELECT name,socr,code FROM public.street where lower(name) like '%респ%' or lower(name) like '%республика%' and lower(name) not like '%переспективная%' and lower(name) not like '%респект%' and lower(name) not like '%корреспондентский%' and lower(name) not like '%чересполосный%' and lower(name) not like '%корреспондентов%'",cnxn, index_col="code")



def load_city_streets(code):
    codes = code.split(",")
    streets_parts_array = []
    for c in codes:
        df_streets = pd.read_sql("SELECT lower(trim(name)) as name,trim(code) as code,lower(trim(socr)) as socr, trim(index) as street_index FROM public.street where trim(code) like '"+c[:11]+"%' order by name",cnxn, index_col="code")

        if len(df_streets) == 0:
            continue
        df_streets['PARTS'] = df_streets['name'].apply(parts_of_name) 
        for row in df_streets.itertuples():
            row_name_parts = word_base_array(parts_of_name(row.name))
            row_code = row.Index
            for i in range(len(row_name_parts)):
                row_dict = {}
                row_dict['name'] = row_name_parts[i]
                row_dict['code'] = row_code + str(i+1)
                row_dict['socr'] = row.socr
                row_dict['street_index'] = row.street_index
                row_dict['FULL_city'] = row
                streets_parts_array.append(row_dict)
    if len(streets_parts_array) == 0:
        return streets_parts_array
    df_street_parts = pd.DataFrame(streets_parts_array)   
    df_street_parts = df_street_parts.set_index('code') 
    return df_street_parts

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
    
def get_house_number(house_regex,str_address_wo_flat):
    found_house_number = re.search(house_regex,str_address_wo_flat) 
    if found_house_number is not None:
        found_house_number = found_house_number.group(0)
        house_number = re.search(r'(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(\/)?(\d{1,4}?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)?)',found_house_number)
        if house_number is not None:
            found_house_number = house_number.group(0)
        else:
            found_house_number = ""
    else:
        found_house_number = ""
    return found_house_number


def getitemsBykeyword(regcities, tokens,str_address, index_code):

    found_districts = []
    checked = []

    for group in tokens:
        croped_tokens = word_base_array(group)
        max_len = 0
        key_word_type = ""
        kladr_possible_items = []
        ret=[]
        for i in range(len(group)):
            kladr_items = pd.DataFrame()
            #if tokens[i] in mcrs:
                #if i<len(tokens)-1:
                    #if tokens[i+1].isdigit():
                    # kladr_items = regcities.query( "socr == 'мкр' and name == '"+tokens[i+1]+"' ",engine = 'python')
                #if len(kladr_items)==0 and i>0 and tokens[i-1].isdigit():
                    #kladr_items = regcities.query( "socr == 'мкр' and name == '"+tokens[i-1]+"' ",engine = 'python')
                #for row in kladr_items.itertuples():
                    #kladr_possible_items.append(row)
            
            if group[i] in street_key_words:
                if group[i] in street_kw:
                    key_word_type = "STREET"
                elif group[i] in avenue_kw:
                    key_word_type = "AVENUE"
                elif group[i] in square_kw:
                    key_word_type = "SQUARE"
                elif group[i] in side_street_kw:
                    key_word_type = "SIDE"
                elif group[i] in mcrs:
                    key_word_type = "MCR"
                elif group[i] in city_district_kw:
                    key_word_type = "DISTRICT"
                
                if i<len(group)-1:           
                    cur_token=get_next_token(group,i)                    
                    wbcur_token = word_base(cur_token)
                    if len(wbcur_token) > 1 and cur_token!='дом':
                        if len(regcities) == 0:
                            continue
                        if index_code != "":
                            kladr_items = regcities.query( " name == '"+wbcur_token+"' and street_index == '" + index_code + "'",engine = 'python')
                            if len(kladr_items) == 0:
                                kladr_items = regcities.query( " name == '"+wbcur_token+"' ",engine = 'python')
                        else:
                            kladr_items = regcities.query( " name == '"+wbcur_token+"' ",engine = 'python')
                    if len(kladr_items)==0:
                        cur_token=get_prev_token(group,i)                    
                        wbcur_token = word_base(cur_token)
                        if len(wbcur_token) > 1:
                            if index_code != "":
                                kladr_items = regcities.query( " name == '"+wbcur_token+"' and street_index == '" + index_code + "'",engine = 'python')
                                if len(kladr_items) == 0:
                                    kladr_items = regcities.query( " name == '"+wbcur_token+"' ",engine = 'python')
                            else:
                                kladr_items = regcities.query( " name == '"+wbcur_token+"' ",engine = 'python')
                if (i==len(group)-1) or len(kladr_items)==0:
                    cur_token=get_prev_token(group,i)                    
                    wbcur_token = word_base(cur_token)
                    if len(wbcur_token) > 1:
                        if index_code != "":
                            kladr_items = regcities.query( " name == '"+wbcur_token+"' and street_index == '" + index_code + "'",engine = 'python')
                            if len(kladr_items) == 0:
                                kladr_items = regcities.query( " name == '"+wbcur_token+"' ",engine = 'python')
                        else:
                            kladr_items = regcities.query( " name == '"+wbcur_token+"' ",engine = 'python')
                for k_row in kladr_items.itertuples():
                    row = k_row.FULL_city
                    #почистить скобки
                    parts = row.PARTS
                    parts_amount = len(parts)
                    if parts_amount>1:
                        startindex = max(0, i - parts_amount - 1)
                        endindex = min(len(group),i + parts_amount + 1)
                        parts_cnt = 0
                        for s in range(startindex,endindex):
                            for j in range(parts_amount):
                                if croped_tokens[s] == word_base(parts[j]) and get_part_of_speech(group[s]) == get_part_of_speech(parts[j]):
                                    parts_cnt += 1
                        if parts_cnt == parts_amount: 
                            kladr_possible_items.append(row)
                            if parts_amount > max_len:
                                max_len = parts_amount
                    else:
                        if wbcur_token == word_base(row.PARTS[0]) and get_part_of_speech(cur_token) == get_part_of_speech(row.PARTS[0]):
                            kladr_possible_items.append(row)


                if len(kladr_possible_items) == 0 and len(kladr_items)==1:
                    for k_row in kladr_items.itertuples():
                        row = k_row.FULL_city
                        kladr_possible_items.append(row)

                if len(kladr_possible_items) == 0 and len(kladr_items)>1:
                        addrItem = {'code':['0'], 'name': [cur_token], 'socr': [group[i]], 'street_index': [index_code], 'parts': ['-']}
                        addrItemDf = pd.DataFrame.from_dict(addrItem) 
                        addrItemDf.set_index('code')
                        for row in addrItemDf.itertuples(): 
                            kladr_possible_items.append(row)
            
            

            for i in kladr_possible_items:
                if key_word_type == "DISTRICT":
                    found_districts.append(i)
                    continue
                if i not in ret:
                    ret.append(i)
        if len(ret) > 1:
            if key_word_type == "STREET":
                for r in ret:
                    if r.socr == 'ул':
                        checked.append(r)
            elif key_word_type == "AVENUE":
                for r in ret:
                    if r.socr == 'пр-кт':
                        checked.append(r)
            elif key_word_type == "SQUARE":
                for r in ret:
                    if r.socr == 'пл':
                        checked.append(r)
            elif key_word_type == "SIDE":
                for r in ret:
                    if r.socr == 'пер':
                        checked.append(r)
            elif key_word_type == "MCR":
                for r in ret:
                    if r.socr == 'мкр':
                        checked.append(r)
            else:
                checked = ret
        
        if len(checked) > 0:
            checked_by_name = []
            for row in checked:
                if row.name in group:
                    checked_by_name.append(row)
            if len(checked_by_name) > 0:
                checked = checked_by_name


    if len(checked) > 1:
        actual = []
        for c in checked:
            if c.Index[-2:] == '00':
                actual.append(c)
        if len(actual) == 1:
            checked = actual
    
    if len(checked) > 1:
        max_len_rn = 0  
        checked_rn = []
        for row in checked:
            if len(row.PARTS) > max_len_rn:
                max_len_rn = len(row.PARTS)
        if max_len_rn > 1:
            for row in checked:
                if len(row.PARTS) == max_len_rn:
                    checked_rn.append(row)
            checked = checked_rn
        
    if len(checked) == 0:            
        checked = ret
    if len(checked) == 1:
        street_index = str_address.find(checked[0].name)
        if street_index == -1:
            street_end_index = -1
        else:
            street_end_index = street_index + len(checked[0].name)
    else:
        street_end_index = -1
    return found_districts, checked, street_end_index

def getitems(regcities, tokens, excludes,str_address, index_code):

    kladr_possible_items = []
    ret=[]
    for group in tokens:
        
        croped_tokens = word_base_array(group)

        words=[]
        
        for i in range(len(group)):
            kladr_items = pd.DataFrame()
            if group[i] not in key_words and len(group[i]) > 1 and not group[i].isdigit() and i not in excludes: 
                cur_token=word_base(group[i])
            else:
                continue
            words.append("name == '"+cur_token+"'")
            #words.append("name.str.contains('"+cur_token+"')")
    
        strwords =" or ".join(words)
        if strwords!="":
            # return [],-1
            kladr_items = regcities.query(strwords,engine = 'python')
            
            for k_row in kladr_items.itertuples():  
                row = k_row.FULL_city
                parts = row.PARTS
                parts_amount = len(parts)
                
                parts_cnt = 0
                for s in range(0,len(group)):
                    for j in range(parts_amount):
                        if croped_tokens[s] == word_base(parts[j]) and get_part_of_speech(group[s]) == get_part_of_speech(parts[j]):
                            parts_cnt += 1
                if parts_cnt == parts_amount: 
                    kladr_possible_items.append(row)
    
        for i in kladr_possible_items:
            if i not in ret:
                ret.append(i)

    if len(ret) > 1:
        max_len_rn = 0  
        checked_rn = []
        for row in ret:
            if len(row.PARTS) > max_len_rn:
                max_len_rn = len(row.PARTS)
        if max_len_rn > 1:
            for row in ret:
                if len(row.PARTS) == max_len_rn:
                    checked_rn.append(row)
            ret = checked_rn

    if len(ret) > 1 and index_code!="":
        checked = []
        for row in ret:
            if row.street_index == index_code:
                checked.append(row)
        if len(checked) > 0:
            ret = checked


    if len(ret) == 1:
        street_index = str_address.find(ret[0].name)
        if street_index == -1:
            street_end_index = -1
        else:
            street_end_index = str_address.find(ret[0].name) + len(ret[0].name)
    else:
        street_end_index = -1

    return ret, street_end_index


def search_house_num(address, street_end_index):
    if street_end_index != -1:
        index_range = min(len(address),street_end_index+25)
        address = address[street_end_index:index_range]
    # str_address_wo_flat = ""
    # flat_regex = r"((кв(\.)?)|(квартира)|(квар(\.)?))(\ )?\d{1,4}"
    # found_flat_number = re.search(flat_regex,address) 
    # if found_flat_number is not None:
    #     found_flat_number = found_flat_number.group(0) 
    #     str_address_wo_flat = address.replace(found_flat_number,'')
    #     flat_number = re.search(r'\d{1,4}',found_flat_number)
    #     if flat_number is not None:
    #         found_flat_number = flat_number.group(0) 
    #     else:
    #         found_flat_number = ""
    # else:
    #     found_flat_number = ""

    address = re.sub(r'\-','/',address)
    address = re.sub(r'\sв районе','',address)
    address = re.sub(r'"','',address)
    address = re.sub(r'\(\)','',address)
    address = re.sub(r'к. дз','к. д3',address)
    house_part = re.search(r'(\s|,)(дом|д)(\s*|\.?\s*)(\d{1,4})', address)
    house_part_with_letter = re.search(r'(\s|,)(дом|д)(\s*|\.?\s*)(\d{1,4})\s?(/)?\s?[а-я]{1}($|\W)', address)
    house_part_with_num = re.search(r'(\s|,)(дом|д)(\s*|\.?\s*)(\d{1,4})/\d{1,3}(\s?[а-я]{1}($|\W))?', address)
    lit=''
    house_num = ''
    house_part_num = ''
    if house_part is not None:
        house_num = re.search(r'\d{1,4}', house_part.group(0)).group(0)
        if house_part_with_letter is not None:
            lit = re.sub('\W|\d','',re.search(r'(\d{1,4})\s?(/)?\s?[а-я]{1}($|\W)', house_part_with_letter.group(0)).group(0)).strip()
        if house_part_with_num is not None:
            house_part_num = re.sub('(\d{1,4}/)|[а-я]|\s','',re.search(r'(\d{1,4})/\d{1,3}(\s?[а-я]{1}($|\W))?', house_part_with_num.group(0)).group(0)).strip()
            if lit=="":
                lit = re.sub('\d|/|\s','',re.search(r'(\d{1,4})/\d{1,3}(\s?[а-я]{1}($|\W))?', house_part_with_num.group(0)).group(0)).strip()
    else:
        house_num = re.search(r'\d{1,4}', address).group(0)
        house_part_with_letter = re.search(r'(\d{1,4})\s?(/)?\s?[а-я]{1}($|\W)',address)
        if house_part_with_letter is not None:
            lit = re.sub('\W|\d','',house_part_with_letter.group(0)).strip()
        house_part_with_num = re.search(r'(\d{1,4})/\d{1,3}(\s?[а-я]{1}($|\W))?', address)
        if house_part_with_num is not None:
            house_part_num = re.sub('(\d{1,4}/)|[а-я]|\s','',house_part_with_num.group(0)).strip()
            if lit=="":
                lit = re.sub('\d|/|\s','',re.search(r'(\d{1,4})/\d{1,3}(\s?[а-я]{1}($|\W))?', house_part_with_num.group(0)).group(0)).strip()

    house_part_lit = re.search(r'\s?(литера?|лит)\s?([а-я]|\d)', address)
    if house_part_lit is not None:
        lit = re.sub(r'\s?(литера?|лит)\s?', '', house_part_lit.group(0)).strip()
    house_part_corp = re.search(r'(\s?(строение|стр(\.)?|корпус|к\.|блок)\s?)(\d{1,3}|[а-я]{1}($|\W)|\d{1,3}[а-я]{1}($|\W)|[а-я]{1}\d{1,3})', address)
    if house_part_corp is not None:
        house_part_num = re.sub(r'(\s?(строение|стр(\.)?|корпус|к\.|блок)\s?)', '', house_part_corp.group(0)).strip()


#     print("house_num: ", house_num)
#     print("liter: ",lit)
#     print("corpus: ", house_part_num)

    return house_num, lit, house_part_num


def load_addresses_from_db():
    addresses = pd.read_sql("SELECT obj_id,address,region, region_code, district, district_code, city, city_code, locality, locality_code, snt, snt_code FROM public.processed_address_new  where processed = 1 and region_code <> '' and city_code <> '' order by region,city",sochi_conn)
    
    cur = 0
    cursor = sochi_conn.cursor()
    t1 = datetime.datetime.now()
    full_city_code=''
    for addr in addresses.itertuples():
        obj_id = addr.obj_id
        region = addr.region
        region_code = addr.region_code
        district = addr.district
        district_code = addr.district_code
        city = addr.city
        city_arr = city.split(',')
        cur_city_code = addr.city_code
        cur_city_code_arr = cur_city_code.split(',')

        locality = addr.locality
        locality_arr = locality.split(',')
        cur_locality_code = addr.locality_code
        cur_locality_code_arr = cur_locality_code.split(',')

        snt = addr.snt
        snt_arr = snt.split(',')
        cur_snt_code = addr.snt_code
        cur_snt_code_arr = cur_snt_code.split(',')
       
        if len(re.findall(r'\d{6}',addr.address)) > 0:
            index_code = re.findall(r'\d{6}',addr.address)[0]
        else:
            index_code = ""
        str_address = re.sub(r'\d{6}','',addr.address).lower()
        str_address = re.sub(r'\d{2}:\d{2}:\d{5,9}:\d{3,4}','',str_address)
        #Как быть со сравнением города, если есть несколько городов
        if full_city_code!=cur_city_code:
            city_code = addr.city_code
            streets_dict = load_city_streets(city_code)
            if cur_locality_code != "":
                locality_streets_dict = load_city_streets(cur_locality_code)
                streets_dict.update(locality_streets_dict)
            if cur_snt_code != "":
                snt_streets_dict = load_city_streets(cur_snt_code)
                streets_dict.update(snt_streets_dict)
            
            full_city_code=cur_city_code
        if len(streets_dict) == 0:
                continue
        excludes = parts_of_name(region)
        if district != '':
            for d in district.split(","):
                parts = re.sub(r'-',' ',d).split()
                for p in parts:
                    excludes.append(p)
        for c in city.split(","):            
            parts = re.sub(r'-',' ',c).split()
            for p in parts:
                excludes.append(p)
                
                
       

            
             
        street = ""
        street_code = ""
        city_district = ""
        reg_mcr = re.findall(r"(\d+)(\-)*[й]*\ ((мкр)|(микрорайон)|(мк-н))",str_address)
        if len(reg_mcr) > 0:
            for i in range(len(reg_mcr)):
                if reg_mcr[0][i].isdigit():
                    street = reg_mcr[0][i]
                    continue
        if street == "":
            tokens = lemmatize_sent(str_address,excludes) 
            if len(tokens) == 0:
                continue

            city_district, street, street_end_index = getitemsBykeyword(streets_dict, tokens,str_address, index_code)
     
            if len(street) == 0:
                street, street_end_index = getitems(streets_dict,tokens,[],str_address, index_code)

        if len(street) > 0:
            house_num, lit, corpus = search_house_num(str_address, street_end_index)
            #проверяем номер квартиры
            # str_address_wo_flat = ""
            # flat_regex = r"((кв(\.)?)|(квартира)|(квар(\.)?))(\ )?\d{1,4}"
            # found_flat_number = re.search(flat_regex,str_address) 
            # if found_flat_number is not None:
            #     found_flat_number = found_flat_number.group(0) 
            #     str_address_wo_flat = str_address.replace(found_flat_number,'')
            #     flat_number = re.search(r'\d{1,4}',found_flat_number)
            #     if flat_number is not None:
            #         found_flat_number = flat_number.group(0) 
            #     else:
            #         found_flat_number = ""
            # else:
            #     found_flat_number = ""
                
            #проверяем номер дома
            #house_regex = r"(((дом)|(влд)|(вл)|(владение)|(уч(\.| №|.№))|(д)|(участок(\ ?№)?))(\.| |\. )?)?(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(/)?(\d{1,4})?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)(\ ?(((,|, | )?корпус)|((,|, | )?корп(\.)?)|((,|, | )?к(\.)?))\ ?\d{1,3})?(((\ )|,|, )?((строение)|(стр(\.)?)|(с(\.)?))\ ?\d{1,3})?(((\ )|,|, )?((литер)|(литера)|(лит(\.)?)|(л(\.)?))\ ((\d{1,3})|((а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)))?(((\ )|,|, )?((помещение)|(пом(\.| )?)|(с(\.)?))\ ?\d{1,3})?"
            # if str_address_wo_flat == "":
            #     str_address_wo_flat = str_address
            # house_regex = r"(( |,|, )((дом)|(д)){1}(\.| |\. )?(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(\/)?(\d{1,4}?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)?))"
            # found_house_number = get_house_number(house_regex,str_address_wo_flat)
            # if found_house_number == "":
            #     house_regex = r"(( |,|, )((влд)|(владение)){1}(\.| |\. )?(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(\/)?(\d{1,4}?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)?))"
            #     found_house_number = get_house_number(house_regex,str_address_wo_flat)
            # if found_house_number == "":
            #     house_regex = r"(( |,|, )((уч)|(участок)){1}(\.| |\. |№| №| № )?(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(\/)?(\d{1,4}?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)?))"
            #     found_house_number = get_house_number(house_regex,str_address_wo_flat)
            # if found_house_number == "":
            #     house_regex = r"(( |,|, )((литер)|(стр)|(строение)|(пом)|(помещение)|(корп)|(корпус)|(к)|(литера)){1}(\.| |\. |№| №| № )?(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(\/)?(\d{1,4}?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)?))"
            #     found_house_number = get_house_number(house_regex,str_address_wo_flat)
            # if found_house_number == "":
            #     house_regex = r"\d{1,4}"
            #     found_house_number = get_house_number(house_regex,str_address_wo_flat)
            # \d{1,4}-\d{1,4}        
            
                    
        if len(street) == 1 and type(street) != str:
            street_code = street[0].Index
            if len(city_district) > 0:
                city_district = city_district[0].name
            if len(cur_city_code_arr) > 1:
                city_code_by_street = street_code[:11]
                cur_city_code_arr_croped = list(map(lambda x: x[:11],cur_city_code_arr))
                ind = cur_city_code_arr_croped.index(city_code_by_street)
                checked_city = city_arr[ind]
                checked_code = city_code_by_street
                postgres_update_query = """ update public.processed_address_new set city= %s, city_code = %s where obj_id = %s """
                cursor.execute(postgres_update_query, (checked_city,checked_code,obj_id))
                # cursor.execute("update [OARB].[ADDRESS] set [city] = ?, [city_code] = ? where [obj_id] = ? and [address] = ?", checked_city,checked_code,obj_id,addr.address)
            
            postgres_update_query = """ update public.processed_address_new set processed = %s, street_district =%s, street = %s, street_code = %s, house = %s, lit = %s, corpus = %s where obj_id = %s """
            cursor.execute(postgres_update_query, (2,city_district, street[0].name,street_code, house_num, lit, corpus,obj_id))
            # cursor.execute("update [OARB].[ADDRESS] set [PROCESSED] = 2, [STREET] = ?, [STREET_code] = ?, [HOUSE] = ? where [obj_id] = ? and [address] = ?", street[0].name,street_code,found_house_number,obj_id, addr.address)
        print (cur)
        sochi_conn.commit()
        #if cur % 100 == 0:
            #cnxn.commit()
    t2 = datetime.datetime.now()
    dt = t2 - t1
    print(dt.total_seconds())
        
        
load_addresses_from_db()     

          
cnxn.close()   
sochi_conn.close()   
        
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    