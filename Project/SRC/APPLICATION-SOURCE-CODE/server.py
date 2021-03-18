from flask import Flask, render_template, request
import datetime
import random
import bleach
import mysql.connector

app = Flask(__name__)


##### Functions for connecting and using the DB ####

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


'''
Full-texte query - returns a film id with plot summary containing given input
'''
def get_film_id_by_text(txt):

    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()

    txt = txt.replace("'", "''")

    query = (   "SELECT FILM.film_id "
                "FROM FILM "
                "WHERE Match(summary) Against(+'%s' IN BOOLEAN MODE); " % txt
            )

    cur.execute(query)
    rows = cur.fetchall()

    for row in rows:
        res.append(row[0])

    close_connection(cnx)
    random.shuffle(res)

    if len(res) != 0:
        return res[0]
    return '0'

'''
Returns a list of actors names born in this month
'''
def get_born_this_month():

    res = []

    month = datetime.datetime.today().month
    cnx = connect_to_mysql_server()
    cur = cnx.cursor()

    query = (   "SELECT actor_name "
                "FROM "
                    "(SELECT ACTOR.actor_id, ACTOR.actor_name, AVG(FILM.rating) AS film_avg "
                    "FROM ACTOR, FILM, FILM_STAR "
                    "WHERE ACTOR.birthdate LIKE '%."  + "%s" % month + "' "
                    "AND ACTOR.actor_id = FILM_STAR.actor_id "
                    "AND FILM.film_id = FILM_STAR.film_id "
                    "GROUP BY ACTOR.actor_id, ACTOR.actor_name "
                    "ORDER BY film_avg DESC ) SUB_QUERY "
            )

    cur.execute(query)
    rows = cur.fetchall()

    for row in rows:
        res.append(row[0])

    close_connection(cnx)

    return res

'''
Given a film ID it returns the movie's rankings in each genre
'''
def get_ranks(f_id):

    res = {}

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()

    query = (   "SELECT FILM_GENRE.genre, COUNT(*) AS cnt "
                "FROM FILM, FILM_GENRE "
                "WHERE FILM.film_id = FILM_GENRE.film_id "
                "AND FILM.rating > "
                "(SELECT FILM.rating FROM FILM WHERE FILM.film_id = '%s')" % f_id + " "
                "AND FILM_GENRE.genre IN "
                    "(SELECT FILM_GENRE.genre "
                    "FROM FILM_GENRE "
                    "WHERE FILM_GENRE.film_id = '%s'" % f_id + ") "
                "GROUP BY FILM_GENRE.genre "
                "ORDER BY cnt; "
            )

    cur.execute(query)
    rows = cur.fetchall()

    for row in rows:
        res[row[0]] = row[1] + 1


    close_connection(cnx)
    if len(res) > 5 :
        res = dict(list(res.items())[:5])
    return res

'''
Given a film ID and a threshold (integer),
it returns a list with films IDs that are other parts (or sequel) of the given film,
we detemine if the film is another part if it has common number of actors more than
the given threshold
'''
def get_other_parts(f_id, threshold):
    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()

    query = (   "SELECT FILM.film_id, COUNT(*) AS shared "
                "FROM FILM, FILM_STAR "
                "WHERE FILM.film_id <> '%s'" % f_id + " "
                "AND FILM.film_id = FILM_STAR.film_id "
                "AND FILM_STAR.actor_id IN "
                    "(SELECT FILM_STAR.actor_id "
                    "FROM FILM, FILM_STAR "
                    "WHERE FILM.film_id = FILM_STAR.film_id "
                    "AND FILM.film_id = '%s'" % f_id + ") "
                "GROUP BY FILM.film_id "
                "HAVING shared >  %s" % threshold + " "
                "ORDER BY FILM.year; "
            )

    cur.execute(query)
    rows = cur.fetchall()

    for row in rows:
        res.append(row[0])

    close_connection(cnx)

    final = []
    for id in res:
        final.append(get_details_by_id(id))

    for i in range(len(final)):
        final[i] = final[i]['title']

    return final 

'''
Given a director name, it returns a list with actors names,
that participated the most times in this director's movies
'''
def get_director_cast(director):
    director = director.replace("'", "''")
    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()

    query = (   "SELECT ACTOR.actor_name, COUNT(*) AS times "
                "FROM ACTOR, FILM_STAR, FILM "
                "WHERE ACTOR.actor_id = FILM_STAR.actor_id "
                "AND FILM_STAR.film_id = FILM.film_id "
                "AND FILM.director = '%s'" % director + " "
                "GROUP BY ACTOR.actor_name "
                "ORDER BY times DESC"
            )

    cur.execute(query)
    rows = cur.fetchall()

    for row in rows:
        res.append(row[0])

    close_connection(cnx)
    if len(res) > 10 :
        res = res[:11]
    return res


'''
Given an actor ID, it returns the genres this actor stars in,
or specializes in
'''
def get_actor_spec(actor_id):

    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()

    query = (   "SELECT FILM_GENRE.genre, COUNT(*) popularity "
                "FROM FILM_GENRE, FILM, FILM_STAR "
                "WHERE FILM_GENRE.film_id = FILM.film_id "
                "AND FILM.film_id = FILM_STAR.film_id "
                "AND FILM_STAR.actor_id = '%s'" % actor_id + " "
                "GROUP BY FILM_GENRE.genre "
                "ORDER BY popularity DESC;"
            )


    cur.execute(query)
    rows = cur.fetchall()

    for row in rows:
        res.append(row[0])

    close_connection(cnx)

    if len(res) >=5 :
        res = res[:5]
    return res


'''
Retuns a list with all the movie titles in the DB
serves auto complete function in the search box
'''
def get_movie_names():
    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()
    cur.execute("SELECT DISTINCT title FROM FILM")
    rows = cur.fetchall()

    for row in rows:
        res.append((row[0]))

    close_connection(cnx)

    return res

'''
Returns a list with movie IDs and their posters,
serves the carousel in the background of the homepage
'''
def get_movie_posters():
    # returns a list with [{"id":"nifen", "poster":"link to poster"},]
    # size of list is 30

    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()
    cur.execute("SELECT title, image FROM FILM")
    rows = cur.fetchall()

    for row in rows:
        film_map = {'title': row[0], 'poster': row[1].replace("_V1_", "_SL500_")}
        res.append(film_map)

    close_connection(cnx)
    random.shuffle(res)

    return res[0:30]

'''
Given a movie name, it returns all the details about this movie in a dictionary
'''
def get_details_by_name(name):
    
    cnx = connect_to_mysql_server()
    cur = cnx.cursor()
    name = name.replace("'", "''")
    cur.execute("SELECT film_id FROM FILM WHERE title = '%s'" % name)

    rows = cur.fetchall()
    film_id = rows[0][0]

    close_connection(cnx)

    return get_details_by_id(film_id)

'''
Given a movie ID, it returns a list with the awards it won and their count
'''
def get_awards(film_id):
    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()
    cur.execute("SELECT award, count FROM FILM_AWARD WHERE film_id = '%s'" % film_id)
    rows = cur.fetchall()

#    awards = ["Oscar", "Golden Globe", "BAFTA Film award"]
    for row in rows:
        res.append({'award':row[0], 'count':row[1]})

    close_connection(cnx)

    return res

'''
Given a movie ID, it returns a list with the movie's genres
'''
def get_genres(film_id):
    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()
    cur.execute("SELECT genre FROM FILM_GENRE WHERE film_id = '%s'" % film_id)
    rows = cur.fetchall()

    for row in rows:
        res.append(row[0])

    close_connection(cnx)

    return res

'''
Given a movie ID, it returns a list with locations the movie was film at
'''
def get_locations(film_id):
    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()
    cur.execute("SELECT location FROM FILM_LOCATION WHERE film_id = '%s'" % film_id)
    rows = cur.fetchall()

    for row in rows:
        res.append(row[0])

    close_connection(cnx)

    return res[:4]

'''
Given a movie ID, it returns a list with streaming services
where you can watch the movie
'''
def get_providers(film_id):
    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()
    cur.execute("SELECT provider FROM FILM_PROVIDER WHERE film_id = '%s'" % film_id)
    rows = cur.fetchall()

    for row in rows:
        res.append(row[0])

    close_connection(cnx)

    if(len(res) >= 5):
        res = res[:5]

    return res

'''
Given a movie ID, it returns a dict with all the details of the movie
'''
def get_details_by_id(film_id):

    res = {}

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()
    cur.execute("SELECT * FROM FILM WHERE film_id = '%s'" % film_id)
    rows = cur.fetchall()

    details = rows[0]
    keys = ['film_id', 'title', 'year', 'image', 'summary', 'trailer', 'rating', 'director']

    for i in range(0, len(keys)):
        res[keys[i]] = details[i]

    close_connection(cnx)

    res['rating'] = str(res['rating'])[:3]

    ##COMPRESSION
    res['image'] = res['image'].replace("_V1_", "_SL300_")

    res['awards'] = get_awards(film_id)
    res['genres'] = get_genres(film_id)
    res['locations'] = get_locations(film_id)
    res['providers'] = get_providers(film_id)
    
    # makes trailers works from outside requests like our website #
    res['trailer'] = res['trailer'].replace("watch?v=","embed/")

    return res

'''
Given a movie ID, it returns a list with movies that are:
    1. Premiered close to this movie's year. (by delta_year)
    2. Rated close to this movie. (by delta_rating)
'''
def more_like_this(f_id, delta_year, delta_rating):

    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()

    query = (   "SELECT DISTINCT FILM.film_id, FILM.image "
                "FROM FILM, FILM_GENRE, "
                "(SELECT FILM.year AS f_year FROM FILM WHERE FILM.film_id = " + "'%s'" % f_id + " ) SUB_QUERY_1, "
                "(SELECT FILM.rating AS f_rating FROM FILM WHERE FILM.film_id = " + "'%s'" % f_id + " ) SUB_QUERY_2 "
                "WHERE FILM.film_id = FILM_GENRE.film_id "
                "AND FILM.film_id <> " + "'%s'" % f_id + " "
                "AND FILM_GENRE.genre in "
                    "(SELECT FILM_GENRE.genre FROM FILM_GENRE WHERE FILM_GENRE.film_id = " + "'%s'" % f_id + " ) "
                "AND ABS(FILM.year - f_year) < %s " % delta_year + " "
                "AND ABS(FILM.rating - f_rating) < %s " % delta_rating + " " )

    cur.execute(query)
    rows = cur.fetchall()

    for row in rows:
        row_map = {'id':row[0], 'poster':row[1].replace("_V1_", "_SL200_")}
        res.append(row_map)

    close_connection(cnx)

    return res

'''
helper function for get_topcast()
'''
def order_list(actors_list, ordered_actors):

    ordered_list = []

    for actor in ordered_actors:
        for d in actors_list:
            if d['id'] == actor:
                ordered_list.append(d)
                break

    return ordered_list

'''
Returns a list with the top cast of a given movie
'''
def get_topcast(f_id):

    res = []

    cnx = connect_to_mysql_server()
    cur = cnx.cursor()

    query_1 = ("SELECT ACTOR.actor_id, ACTOR.actor_name, ACTOR.birthdate, ACTOR.image, AVG(FILM.rating) "
             "FROM ACTOR, FILM_STAR, FILM "                    
             "WHERE ACTOR.actor_id IN "
             "(SELECT actor_id "
             "FROM FILM_STAR "
             "WHERE FILM_STAR.film_id = " + "'%s'" % f_id +") "
             "AND FILM_STAR.film_id = FILM.film_id "
             "AND FILM_STAR.actor_id = ACTOR.actor_id "
             "GROUP BY ACTOR.actor_id;"
             )

    cur.execute(query_1)
    rows_1 = cur.fetchall()

    query_2 =   ("SELECT actor_id "
                "FROM FILM_STAR "
                "WHERE FILM_STAR.film_id = " + "'%s'" % f_id +";")

    cur.execute(query_2)
    rows_2 = cur.fetchall()

    ordered_actors = [rows_2[i][0] for i in range(0, len(rows_2))]

    for row in rows_1:

        row_map = {}
        keys = ['id', 'name', 'birthdate', 'image', 'avg']

        for i in range(0, len(keys)):
            if keys[i] == 'image':
                if row[i] is not None:
                    row_map[keys[i]] = row[i].replace("_V1_", "_SL300_")
                else:
                    row_map[keys[i]] = " "
            else:
                row_map[keys[i]] = row[i]

        res.append(row_map)

    close_connection(cnx)

    res = order_list(res, ordered_actors)

    # adding here his specialziation
    for i in range(0, len(res)):
        res[i]['spec'] = get_actor_spec(res[i]['id'])

    if len(res) >= 10:
        res = res[:10]
    return res


#################### Server code ######################

movies_names = get_movie_names() #global list for auto complete

@app.route('/')
@app.route('/index')
def home():
    born_this_month = get_born_this_month()

    movies_posters = get_movie_posters()
    return render_template('home.html', movies_names=movies_names, movies_posters=movies_posters, born_this_month=born_this_month)



@app.route('/search')
def movie():
    context = {}

    f_id = request.args.get('id', default=None)
    text = request.args.get('text', default=None)

    if( f_id is not None):
        f_id = bleach.clean(f_id)
        context = get_details_by_id(f_id)
    elif(text is not None):
        text = bleach.clean(text)
        lucky_movie = get_film_id_by_text(text)
        if lucky_movie == '0':
            context = get_details_by_name(random.choice(movies_names))
        else:
            context = get_details_by_id(lucky_movie)
    else:
        query = request.args.get('query', default = None)
        query = bleach.clean(query)
        if(query not in movies_names):
            return render_template("not_found.html", query=query)
        context = get_details_by_name(query)

    recs = more_like_this(context['film_id'], 2, 1)
    topcast = get_topcast(context['film_id'])

    context['ranks'] = get_ranks(context['film_id']) ## put here the ranks

    d_cast = get_director_cast(context['director'])

    other_parts = get_other_parts(context['film_id'], 8) ## sequel movies

    return render_template('movie.html',context=context, movies_names=movies_names, recs=recs, topcast=topcast, d_cast=d_cast, other_parts=other_parts)




if __name__ == '__main__':
    app.run(host="0.0.0.0",port="40444")