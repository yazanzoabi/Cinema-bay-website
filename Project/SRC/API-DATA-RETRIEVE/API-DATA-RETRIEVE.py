#!/usr/bin/python3

import xml.etree.ElementTree as ElementTree
import http.client
import urllib
import json
import time
import re
import io
import mysql.connector
from xml.dom import minidom


# =====================================================================================
#            	            Connecting to mysql server
# =====================================================================================


def connect_to_mysql_server():
    cnx = mysql.connector.connect(user='DbMysql07',
                                  password='BestSqlProject101',
                                  host='mysqlsrv1.cs.tau.ac.il',
                                  port=3306,
                                  database='DbMysql07')

    if not cnx.is_connected():
        print("Couldn't connect!")
        exit(-1)
    else:
        return cnx


def close_connection(cnx):
    cnx.close()


# =====================================================================================
#            Parsing IMDb html pages and extracting IMDb "Top 1000" movies
# =====================================================================================


def read_popular_movies():

    root = ElementTree.Element('most_popular_movies')
    ids = ElementTree.SubElement(root, 'movies')

    for i in range(1, 21):
        path = "./HTML_IMDB_FILES/" + str(i) + ".html"
        f = io.open(path, "r", encoding="utf8")
        movies_list = re.findall("tt[0-9]+", f.read())
        movies_set = set(movies_list)

        for movie_id in movies_set:
            item = ElementTree.SubElement(ids, 'id')
            item.text = movie_id

    file = open("./XML_FILES/popular_movies_id.xml", "w")
    file.write(ElementTree.tostring(root).decode("utf-8"))


# =====================================================================================
#         Retrieving data from API and storing the data locally in XML format
# =====================================================================================
# IMPORTANT: The following code retrieves data and uses PREMIUM accounts.
# It's NOT free so please think twice before running the code!
# =====================================================================================


# Globals
conn = http.client.HTTPSConnection("imdb8.p.rapidapi.com")

headers = {
        'x-rapidapi-key': "b07da41c27msh94c622660de330ap1f0d05jsneb6dc514b0aa",
        'x-rapidapi-host': "imdb8.p.rapidapi.com"
}


def read_write_to_xml(input_filename, output_name):

    # Input file
    input_tree = ElementTree.parse(input_filename)
    input_root = input_tree.getroot()

    # XML root element for output
    output_root = ElementTree.Element(output_name)

    return input_root, output_root


def is_bad_request(resp):
    return resp == "400 - Bad Request"


def read_actors_ids_to_xml_file():

    root = ElementTree.Element('actors_ids')
    ids = ElementTree.SubElement(root, 'ids')

    i = 0

    # iterate over all possible dates
    for month in range(1, 13):
        for day in range(1, 32):

            conn.request("GET", "/actors/list-born-today?month=" + str(month) + "&day=" + str(day), headers=headers)

            res = conn.getresponse()
            data = res.read().decode("utf-8")

            if not (is_bad_request(data)):
                actors_list = json.loads(data)
                for actor_id in actors_list:
                    item = ElementTree.SubElement(ids, 'id')
                    item.set('born_on', str(day) + '.' + str(month))
                    item.text = actor_id[6:][:-1]

            i = i + 1

            if i % 5 == 0:
                time.sleep(1.05)

    file = open("./XML_FILES/id_birth_date.xml", "w")
    file.write(ElementTree.tostring(root).decode("utf-8"))


def get_title_year(movie_id):

    conn.request("GET", "/title/auto-complete?q=" + movie_id, headers=headers)

    res = conn.getresponse()
    data = res.read().decode("utf-8")

    details = json.loads(data)

    title = details['d'][0]['l']
    year = details['d'][0]['y']

    # noinspection PyBroadException
    try:
        image_url = details['d'][0]['i']['imageUrl']
    except Exception:
        image_url = 'NO_IMAGE'

    return title, year, image_url


def read_movies_details():

    input_root, output_root = read_write_to_xml('./XML_FILES/popular_movies_id.xml', 'movies_details')

    for i in range(0, len(input_root[0])):

        movie_id = input_root[0][i].text
        title, year, image_url = get_title_year(movie_id)

        keys = ['id', 'title', 'year', 'image']
        vals = [movie_id, title, str(year), image_url]

        movie_element = ElementTree.SubElement(output_root, 'movie')

        for j in range(0, len(keys)):
            ElementTree.SubElement(movie_element, keys[j]).text = vals[j]

        if i % 5 == 0:
            time.sleep(1.05)

    file = open("./XML_FILES/movies_details_id_title_year.xml", "w")
    file.write(ElementTree.tostring(output_root).decode("utf-8"))


def get_summary(movie_id):

    conn.request("GET", "/title/get-overview-details?tconst=" + movie_id, headers=headers)

    res = conn.getresponse()
    data = res.read().decode("utf-8")

    details = json.loads(data)

    if 'plotSummary' in details:
        summary = details['plotSummary']['text']
    elif 'plotOutline' in details:
        summary = details['plotOutline']['text']
    else:
        summary = 'NO SUMMARY FOUND'

    return summary


def read_summary():

    input_root, output_root = read_write_to_xml('./XML_FILES/movies_details_id_title_year.xml', 'movies_details')

    for i in range(0, len(input_root)):

        entry = input_root[i]
        movie_id = entry[0].text

        keys = ['id', 'title', 'year', 'image', 'summary']
        vals = [movie_id, entry[1].text, str(entry[2].text), entry[3].text, get_summary(movie_id)]

        movie_element = ElementTree.SubElement(output_root, 'movie')

        for j in range(0, len(keys)):
            ElementTree.SubElement(movie_element, keys[j]).text = vals[j]

        if i % 5 == 0:
            time.sleep(1.05)

    file = open("./XML_FILES/movies_details_id_title_year_summary.xml", "w")
    file.write(ElementTree.tostring(output_root).decode("utf-8"))


def get_trailer(title, year):

    search_keyword = title + " " + year
    search_keyword = search_keyword.replace(' ', "+") + "+trailer"
    search_keyword = search_keyword.encode('ascii', 'ignore').decode('ascii')
    html = urllib.urlopen("https://www.youtube.com/results?search_query=" + search_keyword)
    video_ids = re.findall(r"watch\?v=(\S{11})", html.read().decode("utf-8"))
    url = "https://www.youtube.com/watch?v=" + video_ids[0]

    return url


def read_trailers():

    input_root, output_root = read_write_to_xml('./XML_FILES/movies_details_id_title_year_summary.xml', 'movies_details')

    for i in range(0, len(input_root)):

        entry = input_root[i]

        title = entry[1].text
        year = entry[2].text

        keys = ['id', 'title', 'year', 'image', 'summary', 'trailer']
        vals = [entry[0].text, title, str(year), entry[3].text, entry[4].text, get_trailer(title, year)]

        movie_element = ElementTree.SubElement(output_root, 'movie')

        for j in range(0, len(keys)):
            ElementTree.SubElement(movie_element, keys[j]).text = vals[j]

        if i % 5 == 0:
            time.sleep(1.05)

    file = open("./XML_FILES/movies_details_id_title_year_summary_trailer.xml", "w")
    file.write(ElementTree.tostring(output_root).decode("utf-8"))


def get_rating(movie_id):

    conn.request("GET", "/title/get-ratings?tconst=" + movie_id, headers=headers)

    res = conn.getresponse()
    data = res.read().decode("utf-8")

    details = json.loads(data)

    return str(details['rating'])


def read_rating():

    input_root, output_root = read_write_to_xml('./XML_FILES/movies_details_id_title_year_summary_trailer.xml', 'movies_details')

    for i in range(0, len(input_root)):
        entry = input_root[i]
        movie_id = entry[0].text

        keys = ['id', 'title', 'year', 'image', 'summary', 'trailer', 'rating']
        vals = [movie_id, entry[1].text, str(entry[2].text), entry[3].text,
                entry[4].text, entry[5].text, get_rating(movie_id)]

        movie_element = ElementTree.SubElement(output_root, 'movie')

        for j in range(0, len(keys)):
            ElementTree.SubElement(movie_element, keys[j]).text = vals[j]

        if i % 5 == 0:
            time.sleep(1.05)

    file = open("./XML_FILES/movies_details_id_title_year_summary_trailer_rating.xml", "w")
    file.write(ElementTree.tostring(output_root).decode("utf-8"))


def get_genres(movie_id):

    conn.request("GET", "/title/get-genres?tconst=" + movie_id, headers=headers)

    res = conn.getresponse()
    data = res.read().decode("utf-8")

    details = json.loads(data)

    return details


def read_genres():

    input_root, output_root = read_write_to_xml('./XML_FILES/movies_details_id_title_year_summary_trailer_rating.xml', 'genres')

    for i in range(0, len(input_root)):

        movie_id = input_root[i][0].text
        genres = get_genres(movie_id)

        for genre in genres:
            record_element = ElementTree.SubElement(output_root, 'record')
            ElementTree.SubElement(record_element, 'id').text = movie_id
            ElementTree.SubElement(record_element, 'genre').text = genre

        if i % 5 == 0:
            time.sleep(1.05)

    file = open("./XML_FILES/id_genre.xml", "w")
    file.write(ElementTree.tostring(output_root).decode("utf-8"))


def get_locations(movie_id):

    conn.request("GET", "/title/get-filming-locations?tconst=" + movie_id, headers=headers)

    res = conn.getresponse()
    data = res.read().decode("utf-8")

    details = json.loads(data)
    res = []

    # noinspection PyBroadException
    try:
        for m in details['locations']:
            res.append(m['location'])
    except Exception:
        res = []

    return res


def read_locations():

    input_root, output_root = read_write_to_xml('./XML_FILES/movies_details_id_title_year_summary_trailer_rating.xml', 'locations')

    for i in range(0, len(input_root)):

        movie_id = input_root[i][0].text
        locations = get_locations(movie_id)

        for location in locations:
            record_element = ElementTree.SubElement(output_root, 'record')
            ElementTree.SubElement(record_element, 'id').text = movie_id
            ElementTree.SubElement(record_element, 'location').text = location

        if i % 5 == 0:
            time.sleep(1.05)

    file = open("./XML_FILES/id_location.xml", "w")
    file.write(ElementTree.tostring(output_root).decode("utf-8"))


def get_cast(movie_id):

    conn.request("GET", "/title/get-top-cast?tconst=" + movie_id, headers=headers)

    res = conn.getresponse()
    data = res.read().decode("utf-8")

    details = json.loads(data)
    res = []

    for m in details:
        res.append(m[6:][:-1])

    return res


def read_cast():

    input_root, output_root = read_write_to_xml('./XML_FILES/movies_details_id_title_year_summary_trailer_rating.xml', 'actors')

    for i in range(0, len(input_root)):

        movie_id = input_root[i][0].text
        cast = get_cast(movie_id)

        for member in cast:
            record_element = ElementTree.SubElement(output_root, 'record')
            ElementTree.SubElement(record_element, 'id').text = movie_id
            ElementTree.SubElement(record_element, 'member').text = member

        if i % 5 == 0:
            time.sleep(1.05)

    file = open("./XML_FILES/id_cast.xml", "w")
    file.write(ElementTree.tostring(output_root).decode("utf-8"))


def get_director(movie_id):

    conn.request("GET", "/title/get-top-crew?tconst=" + movie_id, headers=headers)

    res = conn.getresponse()
    data = res.read().decode("utf-8")

    details = json.loads(data)
    director = details['directors'][0]['name']

    return director


def read_director():

    input_root, output_root = read_write_to_xml('./XML_FILES/movies_details_id_title_year_summary_trailer_rating.xml',
                                                'movies_details')

    for i in range(0, len(input_root)):
        entry = input_root[i]
        movie_id = entry[0].text

        keys = ['id', 'title', 'year', 'image', 'summary', 'trailer', 'rating', 'director']
        vals = [movie_id, entry[1].text, str(entry[2].text), entry[3].text, entry[4].text,
                entry[5].text, entry[6].text, get_director(movie_id)]

        movie_element = ElementTree.SubElement(output_root, 'movie')

        for j in range(0, len(keys)):
            ElementTree.SubElement(movie_element, keys[j]).text = vals[j]

        if i % 5 == 0:
            time.sleep(1.05)

    file = open("./XML_FILES/movies_details_id_title_year_summary_trailer_rating_director.xml", "w")
    file.write(ElementTree.tostring(output_root).decode("utf-8"))


def get_cast_ids():
    cast_ids = []

    # Input File
    input_tree = ElementTree.parse("./XML_FILES/id_cast.xml")
    input_root = input_tree.getroot()

    for i in range(0, len(input_root)):
        member = input_root[i][1].text
        if member not in cast_ids:
            cast_ids.append(member)

    return cast_ids


def get_ids_to_dates():
    ids_date_map = {}

    input_tree = ElementTree.parse("./XML_FILES/id_birth_date.xml")
    input_root = input_tree.getroot()

    for i in range(0, len(input_root[0])):
        actor_id = input_root[0][i].text
        date = input_root[0][i].attrib['born_on']

        if actor_id is None:
            continue
        ids_date_map[actor_id] = date

    return ids_date_map


def get_actor_name_image(actor_id):

    conn.request("GET", "/title/auto-complete?q=" + actor_id, headers=headers)

    res = conn.getresponse()
    data = res.read().decode("utf-8")

    details = json.loads(data)

    # noinspection PyBroadException
    try:
        name = details['d'][0]['l']
    except Exception:
        name = "NOT FOUND"

    # noinspection PyBroadException
    try:
        image = details['d'][0]['i']['imageUrl']
    except Exception:
        image = "NOT FOUND"

    return name, image


def read_cast_name():

    cast_ids = get_cast_ids()
    ids_to_date = get_ids_to_dates()

    output_root = ElementTree.Element('cast_details')

    for i in range(0, len(cast_ids)):
        name, image = get_actor_name_image(cast_ids[i])
        movie_element = ElementTree.SubElement(output_root, 'member')
        ElementTree.SubElement(movie_element, 'id').text = cast_ids[i]
        ElementTree.SubElement(movie_element, 'name').text = name
        born_on_element = ElementTree.SubElement(movie_element, 'born_on')
        ElementTree.SubElement(movie_element, 'image').text = image

        if cast_ids[i] in ids_to_date:
            born_on_element.text = ids_to_date[cast_ids[i]]
        else:
            born_on_element.text = "NOT FOUND"

        if i % 5 == 0:
            time.sleep(1.05)

    file = open("./XML_FILES/cast_id_name_born_image.xml", "w")
    file.write(ElementTree.tostring(output_root).decode("utf-8"))


def get_awards_dict(movie_id):

    awards_dict = {}
    highlighted_awards = {'Golden Globe', 'Oscar', 'BAFTA Film Award'}

    conn.request("GET", "/title/get-awards?tconst=" + movie_id, headers=headers)

    res = conn.getresponse()
    data = res.read().decode("utf-8")

    details = json.loads(data)

    # noinspection PyBroadException
    try:
        awards = details['resource']['awards']
    except Exception:
        return awards_dict

    for award in awards:
        name = award['awardName']
        is_winner = award['isWinner']
        if name in highlighted_awards and is_winner:
            if name not in awards_dict:
                awards_dict[name] = 1
            else:
                awards_dict[name] = awards_dict[name] + 1

    return awards_dict


def read_awards():
    input_root, output_root = read_write_to_xml('./XML_FILES/movies_details_id_title_year_summary_trailer_rating_director.xml',
                                                'awards')

    for i in range(0, len(input_root)):
        movie_id = input_root[i][0].text
        awards_dict = get_awards_dict(movie_id)

        for award in awards_dict:
            award_element = ElementTree.SubElement(output_root, 'record')
            ElementTree.SubElement(award_element, 'id').text = movie_id
            ElementTree.SubElement(award_element, 'award').text = award
            ElementTree.SubElement(award_element, 'count').text = str(awards_dict[award])

        if i % 5 == 0:
            time.sleep(1.05)

    file = open("./XML_FILES/id_award_count.xml", "w")
    file.write(ElementTree.tostring(output_root).decode("utf-8"))


def get_provider_data(movie_id, conn, request, headers):

    conn.request("GET", request + movie_id, headers=headers)
    res = conn.getresponse()

    # noinspection PyBroadException
    try:
        api_data = res.read()
        api_data = api_data.decode("utf-8")
        res = json.loads(api_data)
    except Exception:
        time.sleep(1.5)
        return False

    response = res.get('streamingAvailability')

    if response is None:
        if res.get('message') == "imdbid is wrong or imdbid doesn't exists in database":
            return False
        time.sleep(1.5)
        # time-out error => retrying
        return get_provider_data(movie_id, conn, request, headers)
    return response


def read_providers():

    mydoc = minidom.parse("./XML_FILES/popular_movies_id.xml")
    conn = http.client.HTTPSConnection("ott-details.p.rapidapi.com")

    headers = {
        'x-rapidapi-key': "229868ae72mshfd1cfcdc52f4a09p166d32jsn6eca710deda4",
        'x-rapidapi-host': "ott-details.p.rapidapi.com"
    }
    id_packages = mydoc.getElementsByTagName('id')

    data = ElementTree.Element('streaming_services')
    request = "/gettitleDetails?imdbid="

    i = 0
    for movie_id_package in id_packages:
        i = i + 1
        movie_id = movie_id_package.firstChild.data
        response = get_provider_data(movie_id, conn, request, headers)
        if response is False:
            time.sleep(1.5)
            continue

        # noinspection PyBroadException
        try:
            providers = response['country']['US']
        except Exception:
            time.sleep(1.5)
            continue

        # write into the xml file
        item = ElementTree.SubElement(data, 'movie')
        item.set('id', movie_id)
        for provider in providers:
            movie_url = provider.get('url')
            movie_platform = provider.get('platform')
            if movie_url is not None and movie_platform is not None:
                pro = ElementTree.SubElement(item, 'providers')
                pro.set('url', movie_url)
                pro.set('platform', movie_platform)
        time.sleep(1.5)
        if i > 1009:
            break

    my_data = ElementTree.tostring(data)
    my_file = open("./XML_FILES/services_for_movies.xml", "wb")
    my_file.write(my_data)


def retrieve_data():

    # IMPORTANT: These calls should be executed by 
    # order, as they use the output of previous calls 

    read_popular_movies()
    read_movies_details()
    read_summary()
    read_trailers()
    read_rating()
    read_genres()
    read_locations()
    read_cast()
    read_director()
    read_actors_ids_to_xml_file()
    read_cast_name()
    read_awards()
    read_providers()


# =====================================================================================
#                             Inserting data into DB
# =====================================================================================


def fill_film_table(cnx):

    input_tree = ElementTree.parse("./XML_FILES/movies_details_id_title_year_summary_trailer_rating_director.xml")
    input_root = input_tree.getroot()

    for i in range(0, len(input_root)):
        film_id = input_root[i][0].text
        title = input_root[i][1].text
        title = title.replace("'", "''")
        year = input_root[i][2].text
        image = input_root[i][3].text
        summary = input_root[i][4].text
        summary = summary.replace("'", "''")
        trailer = input_root[i][5].text
        rating = input_root[i][6].text
        director = input_root[i][7].text
        director = director.replace("'", "''")

        cur = cnx.cursor()

        no_summary = (summary == 'NO SUMMARY FOUND')
        no_image = (image == 'NO_IMAGE')

        if no_summary and no_image:
            query = ("INSERT IGNORE INTO FILM (film_id, title, year, trailer, rating, director) VALUE"
                     "('%s', '%s', '%s', '%s', '%s', '%s');"
                     % (film_id, title, year, trailer, rating, director))
        elif no_summary:
            query = ("INSERT IGNORE INTO FILM (film_id, title, year, image, trailer, rating, director) VALUE"
                     "('%s', '%s', '%s', '%s', '%s', '%s', '%s');"
                     % (film_id, title, year, image, trailer, rating, director))
        elif no_image:
            query = ("INSERT IGNORE INTO FILM (film_id, title, year, summary, trailer, rating, director) VALUE"
                     "('%s', '%s', '%s', '%s', '%s', '%s', '%s');"
                     % (film_id, title, year, summary, trailer, rating, director))

        else:
            query = ("INSERT IGNORE INTO FILM VALUE"
                     "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');"
                     % (film_id, title, year, image, summary, trailer, rating, director))

        cur.execute(query)
        cnx.commit()


def fill_film_location_table(cnx):
    # Input File
    input_tree = ElementTree.parse("./XML_FILES/id_location.xml")
    input_root = input_tree.getroot()

    for i in range(0, len(input_root)):
        film_id = input_root[i][0].text
        location = input_root[i][1].text
        location = location.replace("'", "''")

        cur = cnx.cursor()

        query = ("INSERT IGNORE INTO FILM_LOCATION VALUE"
                 "('%s', '%s');" % (film_id, location))

        cur.execute(query)
        cnx.commit()


def fill_film_genre_table(cnx):
    # Input File
    input_tree = ElementTree.parse("./XML_FILES/id_genre.xml")
    input_root = input_tree.getroot()

    for i in range(0, len(input_root)):
        film_id = input_root[i][0].text
        genre = input_root[i][1].text

        cur = cnx.cursor()

        query = ("INSERT INTO FILM_GENRE VALUE"
                 "('%s', '%s');" % (film_id, genre))

        cur.execute(query)
        cnx.commit()


def fill_film_star_table(cnx):

    input_tree = ElementTree.parse("./XML_FILES/id_cast.xml")
    input_root = input_tree.getroot()

    for i in range(0, len(input_root)):
        film_id = input_root[i][0].text
        actor_id = input_root[i][1].text

        cur = cnx.cursor()

        query = ("INSERT INTO FILM_STAR VALUE"
                 "('%s', '%s');" % (film_id, actor_id))

        cur.execute(query)
        cnx.commit()


def fill_film_provider_table(cnx):

    input_tree = ElementTree.parse("./XML_FILES/services_for_movies.xml")
    input_root = input_tree.getroot()

    for i in range(0, len(input_root)):
        film_id = input_root[i].attrib['id']
        for j in range(0, len(input_root[i])):
            provider = input_root[i][j].attrib['platform']

            cur = cnx.cursor()

            query = ("INSERT INTO FILM_PROVIDER VALUE"
                     "('%s', '%s');" % (film_id, provider))

            cur.execute(query)
            cnx.commit()


def fill_film_awards_table(cnx):

    input_tree = ElementTree.parse("./XML_FILES/id_award_count.xml")
    input_root = input_tree.getroot()

    for i in range(0, len(input_root)):
        film_id = input_root[i][0].text
        award = input_root[i][1].text
        count = input_root[i][2].text

        cur = cnx.cursor()

        query = ("INSERT INTO FILM_AWARD VALUE"
                 "('%s', '%s', '%s');" % (film_id, award, count))

        cur.execute(query)
        cnx.commit()


def fill_actor_table(cnx):
    # Input File
    input_tree = ElementTree.parse("./XML_FILES/cast_id_name_born_image.xml")
    input_root = input_tree.getroot()

    for i in range(0, len(input_root)):
        actor_id = input_root[i][0].text
        actor_name = input_root[i][1].text
        actor_name = actor_name.replace("'", "")
        birthdate = input_root[i][2].text
        image = input_root[i][3].text

        cur = cnx.cursor()

        if birthdate == 'NOT FOUND':
            query = ("INSERT INTO ACTOR (actor_id, actor_name, image) VALUE"
                     "('%s', '%s', '%s');" % (actor_id, actor_name, image))
        else:
            query = ("INSERT INTO ACTOR (actor_id, actor_name, birthdate, image) VALUE"
                     "('%s', '%s', '%s', '%s');" % (actor_id, actor_name, birthdate, image))

        cur.execute(query)
        cnx.commit()


def insert_data_into_db():
    cnx = connect_to_mysql_server()
    fill_film_table(cnx)
    fill_film_location_table(cnx)
    fill_film_genre_table(cnx)
    fill_film_star_table(cnx)
    fill_film_provider_table(cnx)
    fill_film_awards_table(cnx)
    fill_actor_table(cnx)
    close_connection(cnx)


# =====================================================================================
#                                  Setting up the DB
# =====================================================================================
# IMPORTANT: retrieve_data function is responsible for making requests to the API, and
#            so it takes TOO much time to finish (could  take  a  whole  day). But you 
#            don't really have to run this function as we saved the data in  XML files. 
#            You can find the backup files in XML_FILES directory.
#           
#            insert_data_into_db function reads the data from  XML_files  and  inserts
#            it into the DB.  So you are allowed to run it even if you  didn't run the
#            retrieve_data function.  But if you choose to run  retrieve_data then you
#            must wait until it returns back. If you  don't  the  XML  files  will  be 
#            damaged and insert_data_into_db will fail too.
#
# =====================================================================================

def set_up_db():
    #retrieve_data()
    insert_data_into_db()



set_up_db()

