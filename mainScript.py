import cx_Oracle
from bs4 import BeautifulSoup
import requests
import os
import json
import random


def db_connect() -> cx_Oracle.Cursor:
    LOCATION = r"./Instant-client"
    os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
    CONNECT = cx_Oracle.connect("C##TeamProject", "1234", "localhost:1521/xe")
    CURSOR = CONNECT.cursor()
    return CURSOR, CONNECT


def get_movie_data():
    parse_data = {
        'movie_id': [],  # 영화 코드[*]
        'title': [],  # 제목[*]
        'score': [],  # 점수[*]
        'status': [],  # 제작 상태[*]
        'director': [],  # 감독[*]
        'genre': [],  # 장르[*]
        'rate': [],  # 상영 등급
        'country': [],  # 제작 국가[*]
        'actor': [],  # 주요 배우[*]
        'company': [],  # 배급사 -> 못찾음
        'date': [],  # 제작 연도[*]
        'showing': [],  # 상영 상태[*]
        'runtime': []  # 상영 시간[*]
    }
    parse_url = []
    processing = lambda x: x.replace("\t", "").replace("\r", "").replace("\t", "").replace("\n", "").strip()
    URL = r"https://movie.naver.com/movie/sdb/rank/rmovie.naver"  # 네이버 영화의 랭킹
    html = requests.get(URL)
    soup = BeautifulSoup(html.text, 'html.parser')
    table_body = soup.find('div', {'id': 'cbody'}) \
        .find('div', {'id': 'old_content'})
    table = table_body.find('table').find('tbody')
    movie_rank = table.find_all('tr')

    for rank in movie_rank[1:]:
        try:
            raw_data = rank.find('div', {'class': 'tit3'}).find('a')
            parse_url.append(raw_data['href'])
            parse_data['title'].append(raw_data['title'])
            parse_data['movie_id'].append(parse_url[-1][-6:] if '=' not in parse_url[-1][-6:] else parse_url[-1][-5:])

        except AttributeError:
            pass
    # 메인 랭킹 페이지에서 movie_id, title 크롤링
    # 아래는 자세한 정보를 각각 URL에 접근하여 크롤링
    for movie_detail in parse_url:
        HOME_URl = "https://movie.naver.com/"
        html = requests.get(HOME_URl + movie_detail)
        soup = BeautifulSoup(html.text, 'html.parser')
        movie_info = soup.find("div", {"class": "mv_info_area"}) \
            .find("div", {"class": "mv_info"})
        try:
            query = movie_info.find("div", {"class": "main_score"}).find("div", {"class": "score"}) \
                .find("a", {"id": "actualPointPersentBasic"}).find("div", {"class": "star_score"}).get_text()
            parse_data['score'].append(query[-5:-1] if query[-5:-1] != "점 없음" else "NULL")
            parse_data['status'].append("개봉 완료")

        except AttributeError:
            parse_data['score'].append("NULL")
            parse_data['status'].append("개봉 예정")

        parse_data['director'].append(movie_info.select_one(
            "#content > div.article > div.mv_info_area > div.mv_info > dl > dd:nth-child(4) > p > a").get_text())
        overview = movie_info.find("dl", {"class": "info_spec"}).find("dd").find("p")
        overview_span = overview.find_all("span")
        parse_data['genre'].append(processing(overview_span[0].get_text()))
        parse_data['country'].append(processing(overview_span[1].get_text()))
        parse_data['runtime'].append(
            processing(overview_span[2].get_text()) if processing(overview_span[2].get_text())[-1] == '분' else "NULL")
        try:
            parse_data['date'].append(processing(overview_span[3].get_text()).split(" ")[0])
        except IndexError:
            parse_data['date'].append("NULL")

        try:
            is_showing = movie_info.select_one(
                "#content > div.article > div.mv_info_area > div.mv_info > h3 > a.opening > em").get_text()
            parse_data['showing'].append(is_showing)
        except AttributeError:
            parse_data['showing'].append("상영안함")

        try:
            overview = movie_info.find("dl", {"class": "info_spec"}).find_all("dd")
            parse_data['actor'].append(overview[2].find("p").get_text())
            parse_data['rate'].append(processing(overview[3].find("p").get_text()).split("[해")[0])

        except Exception:
            parse_data['actor'].append("NULL")
            parse_data['rate'].append("NULL")

    return parse_data


def create_table(cursor: cx_Oracle.Cursor, conn: cx_Oracle.connect, data: dict):
    for i in range(50):
        try:
            query = f"""
                    INSERT INTO MOVIE VALUES(
                    '{data["movie_id"][i]}', 
                    '{data["title"][i]}', 
                    '{data["genre"][i]}', 
                    '{data["rate"][i]}', 
                    '{data["date"][i]}', 
                    '{data["country"][i]}',
                    '{data["director"][i]}', 
                    '{data["actor"][i]}', 
                    'Disney', 
                    '{data["status"][i]}',
                    '{data["showing"][i]}', 
                    {data["score"][i]});
                    """
            cursor.execute(query)
        except Exception as e:
            print(e)
            print(query)
        conn.commit()


def save_as_json(path: str, file: dict):
    data = json.dumps(file, ensure_ascii=False, indent=4, sort_keys=True)
    print(data)
    with open(path, "w", encoding="UTF-8") as f:
        f.write(data)


def random_telephone():

    tel = ['0', '1', '0', '-']
    for i in range(8):
        tel.append(str(random.randint(0, 9)))
    tel.insert(8, '-')
    return ''.join(tel)


def uniqueid():
    seed = random.getrandbits(32)
    while True:
        yield seed
        seed += 1


def create_user_table(cursor: cx_Oracle.Cursor, conn: cx_Oracle.connect):
    mem_id = uniqueid()
    mem_name = open("./data/names.txt")
    for i in range (100):
        query = f"""
        INSERT INTO MEMBERSHIP VALUES(:1, :2, :3, :4)
        """
        val = (next(mem_id), str(next(mem_name)).replace("\n",""), random_telephone(), str(random.randint(0,1)))
        cursor.execute(query, val)
        conn.commit()


def distribution_company_list(cursor: cx_Oracle.Cursor, conn: cx_Oracle.connect):
    generator = uniqueid()
    for i in range (10):
        vals = [input() for _ in range(3)]
        sql = f"""INSERT INTO CONTRIBUTOR VALUES(:1, :2, :3, :4)"""
        vals.insert(0, next(generator))
        print(vals)
        cursor.execute(sql, tuple(vals))
        conn.commit()


def employee_table(cursor: cx_Oracle.Cursor, conn: cx_Oracle.connect):
    generator = uniqueid()
    name = open(r"data/names.txt")

    for i in range(30):
        salary = str(random.randint(2500000, 5000000))
        vals = (next(generator), str(next(name)).replace("\n",""), random_telephone(), int(salary[:-4]+"0000"))
        query = f"""INSERT INTO EMPLOYEE Values(:1, :2, :3, :4)"""
        cursor.execute(query, vals)
        conn.commit()


def cinema_table(cursor: cx_Oracle.Cursor, conn: cx_Oracle.connect):
    URL = r"https://ko.wikipedia.org/wiki/IMAX_%EC%83%81%EC%98%81%EA%B4%80_%EB%AA%A9%EB%A1%9D"
    processing = lambda x: x.replace("\t", "").replace("\r", "").replace("\t", "").replace("\n", "").strip()
    html = requests.get(URL)
    soup = BeautifulSoup(html.text, 'html.parser')
    table = soup.find('tbody')
    generator = uniqueid()

    for i in table.find_all("tr"):
        try:
            values = [next(generator)]
            column = i.find_all("td")

            values.append(processing(column[0].text) if "폐점" not in processing(column[0].text) else processing(column[0].text)[:-4])
            values.append(processing(column[1].text))
            values.append(random_telephone())
            query = f"""INSERT INTO CINEMA Values(:1, :2, :3, :4)"""
            cursor.execute(query, tuple(values))
            conn.commit()

        except Exception:
            continue



# movie_data = open(f"./data/movies.json", "r", encoding="UTF-8")
# movie_attribute = json.loads(movie_data.read())
# save_as_json(f"./data/movies.json", movie_attribute)
cursor, conn = db_connect()
# distribution_company_list(cursor, conn)
# employee_table(cursor, conn)
cinema_table(cursor, conn)
# sql = "INSERT INTO MEMBERSHIP VALUES(:1, :2, :3, :4)"
# data = ('ACS1111', 'Arial', '010-8566-9091', '1')
# cursor.execute(sql, data)
# conn.commit()

# seq = uniqueid()
# mem_name = open("./data/names.txt")
# create_user_table(cursor, conn)
# create_table(cursor, conn, movie_attribute)
