import sqlalchemy
import os


class SampleFunc():
    def __init__(self):
        db_user = os.environ.get("DB_USER")
        db_pass = os.environ.get("DB_PASS")
        db_name = os.environ.get("DB_NAME")
        cloud_sql_connection_name = os.environ.get("CLOUD_SQL_CONNECTION_NAME")
        self.db = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL(
                drivername="mysql+pymysql",
                username=db_user,
                password=db_pass,
                database=db_name,
                query={"unix_socket": "/cloudsql/{}".format(cloud_sql_connection_name)},
            ),
            pool_size=5,
            max_overflow=2,
            # pool_timeout=30,
            pool_recycle=5,
            # pool_recycle=1800,
        )


    def filter_collections(self, bline_ids=[], season_ids=[], cities=[], genres=[], sexes=[]):
        statement = "SELECT id, season_id, city, genre, sex, news_id, bline_id FROM collections where 1=1"
        if len(bline_ids)>0:
            statement += " and bline_id in ({})".format(','.join([str(bline_id) for bline_id in bline_ids]))
        if len(season_ids)>0:
            statement += " and season_id in ({})".format(','.join(["'" + season_id + "'" for season_id in season_ids]))
        if len(cities)>0:
            statement += " and city in ({})".format(','.join(["'" + city + "'" for city in cities]))
        if len(genres)>0:
            statement += " and genre in ({})".format(','.join(["'" + genre + "'" for genre in genres]))
        if len(sexes)>0:
            statement += " and sex in ({})".format(','.join(["'" + sex + "'" for sex in sexes]))
        statement += " ORDER BY id DESC"
        statement += " LIMIT 20"
        statement += ";"
        collections = []
        collection_rows = []
        with self.db.connect() as conn:
            collection_rows = conn.execute(statement).fetchall()
            for collection_row in collection_rows:
                collection = {}
                collection["id"] = collection_row[0]
                collection["brandId"] = collection_row[6]
                statement = "SELECT name, name_en FROM blines where id={}".format(collection_row[6])
                blines = conn.execute(statement).fetchall()
                collection["brandName"] = blines[0][0]
                collection["brandNameEn"] = blines[0][1]
                statement = "SELECT * FROM seasons where id={}".format(collection_row[1])
                collection["season"] = conn.execute(statement).fetchall()[0][2]
                collection["place"] = collection_row[2]
                collection["gender"] = collection_row[4]
                collection["genre"] = collection_row[3]
                collection["newsId"] = collection_row[5]
                collection["topImageName"] = "top.jpg"
                statement = "SELECT id, name FROM images where new_id={} and position != 0".format(collection["newsId"])
                images = []
                image_rows = conn.execute(statement).fetchall()
                for image_row in image_rows:
                    image = {}
                    image["id"] = image_row[0]
                    image["collectionId"] = collection["id"]
                    image["imageName"] = image_row[1]
                    image["brandId"] = collection["brandId"]
                    image["brandName"] = collection["brandName"]
                    image["brandNameEn"] = collection["brandNameEn"]
                    image["season"] = collection["season"]
                    image["place"] = collection["place"]
                    image["gender"] = collection["gender"]
                    image["genre"] = collection["genre"]
                    image["newsId"] = collection["newsId"]
                    images.append(image)
                collection["images"] = images
                collections.append(collection)
        return collections


    def filter_images(self, bline_ids=[], season_ids=[], cities=[], genres=[], sexes=[], item_groups=[], colors=[], materials=[], patterns=[]):
        statement = """SELECT i.id, i.name, c.id, b.id, b.name, b.name_en, c.city, c.sex, c.genre, c.news_id, s.name_en,
                    	GROUP_CONCAT(CONCAT(d.item_group,",", d.color, ",", d.material, ",", d.pattern) order by d.id)
                    	FROM image_details AS d
                    		LEFT JOIN images AS i  ON i.id=d.image_id
                    		JOIN collections AS c ON i.new_id = c.news_id
                    		LEFT OUTER JOIN blines AS b ON c.bline_id = b.id
                    		LEFT OUTER JOIN seasons AS s ON c.season_id = s.id
                    	    WHERE 1=1"""
        if len(bline_ids)>0:
            statement += " and c.bline_id in ({})".format(','.join([str(bline_id) for bline_id in bline_ids]))
        if len(season_ids)>0:
            statement += " and c.season_id in ({})".format(','.join(["'" + season_id + "'" for season_id in season_ids]))
        if len(cities)>0:
            statement += " and c.city in ({})".format(','.join(["'" + city + "'" for city in cities]))
        if len(genres)>0:
            statement += " and c.genre in ({})".format(','.join(["'" + genre + "'" for genre in genres]))
        if len(sexes)>0:
            statement += " and c.sex in ({})".format(','.join(["'" + sex + "'" for sex in sexes]))
        if len(item_groups)>0:
            statement += " and d.item_group in ({})".format(','.join(["'" + item_group + "'" for item_group in item_groups]))
        if len(materials)>0:
            statement += " and d.material in ({})".format(','.join(["'" + material + "'" for material in materials]))
        if len(patterns)>0:
            statement += " and d.pattern in ({})".format(','.join(["'" + pattern + "'" for pattern in patterns]))
        if len(colors)>0:
            statement += " and d.color in ({})".format(','.join(["'" + color + "'" for color in colors]))
        statement += "GROUP BY d.image_id, c.id, b.id, s.id ORDER BY i.id DESC LIMIT 20;"
        images = []
        with self.db.connect() as conn:
            rows = conn.execute(statement).fetchall()
            for row in rows:
                image = {}
                image["id"] = row[0]
                image["imageName"] = row[1]
                image["collectionId"] = row[2]
                image["brandId"] = row[3]
                image["brandName"] = row[4]
                image["brandNameEn"] = row[5]
                image["place"] = row[6]
                image["gender"] = row[7]
                image["genre"] = row[8]
                image["newsId"] = row[9]
                image["season"] = row[10]
                # image["tags"] = row[11]
                images.append(image)
        return images

    def get_brands(self):
        brands = []
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT id, name, name_en FROM blines;"
            ).fetchall()
            for row in rows:
                brand = {}
                brand["value"] = str(row[0])
                brand["name"] = row[1]
                brand["type"] = "Designer"
                brands.append(brand)
        return brands

    def get_seasons(self):
        seasons = []
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT id, name_en FROM seasons ORDER BY id DESC;"
            ).fetchall()
            for row in rows:
                season = {}
                season["value"] = str(row[0])
                season["name"] = row[1]
                season["type"] = "Season"
                seasons.append(season)
        return seasons

    def get_item_groups(self):
        item_groups = []
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT `key`, name FROM image_vals where `group`='item_group' and name!='未入力';"
            ).fetchall()
            for row in rows:
                item_group = {}
                item_group["value"] = str(row[0]).replace(" ", "+")
                item_group["name"] = row[1]
                item_group["type"] = "Category"
                item_groups.append(item_group)
        return item_groups

    def get_colors(self):
        colors = []
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT `key`, name FROM image_vals where `group` = 'color' and name!='未入力';"
            ).fetchall()
            for row in rows:
                color = {}
                color["value"] = str(row[0]).replace(" ", "+")
                color["name"] = row[1]
                color["type"] = "Color"
                colors.append(color)
        return colors

    def get_materials(self):
        materials = []
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT `key`, name FROM image_vals where `group` = 'material' and name!='未入力';"
            ).fetchall()
            for row in rows:
                material = {}
                material["value"] = str(row[0]).replace(" ", "+")
                material["name"] = row[1]
                material["type"] = "Material"
                materials.append(material)
        return materials

    def get_patterns(self):
        patterns = []
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT `key`, name FROM image_vals where `group` = 'pattern' and name!='未入力';"
            ).fetchall()
            for row in rows:
                pattern = {}
                pattern["value"] = str(row[0]).replace(" ", "+")
                pattern["name"] = row[1]
                pattern["type"] = "Pattern"
                patterns.append(pattern)
        return patterns

    def filter_articles(self):
        articles = []
        with self.db.connect() as conn:
            news_rows = conn.execute(
                """SELECT bn.news_id, buff.title, buff.content, buff.category, buff.sex, buff.tags, GROUP_CONCAT(bn.bline_id)
                	FROM blines_news bn
                		JOIN (SELECT n.id AS id, n.title AS title, n.content AS content, n.category_sub AS category, n.sex AS sex,
                				GROUP_CONCAT(CONCAT(t.id, ":", t.name)) AS tags
                				FROM news AS n
                					LEFT OUTER JOIN news_tags nt ON n.id=nt.news_id
                					LEFT OUTER JOIN tags t ON t.id = nt.tag_id
                				WHERE n.category='fashion'
                					AND n.id < 32634
                				GROUP BY n.id, nt.news_id
                		) AS buff ON buff.id=bn.news_id
                    GROUP BY bn.news_id
                    ORDER BY bn.news_id DESC
                    LIMIT 20
                    ;"""
            ).fetchall()
            for news_row in news_rows:
                article = {}
                article["id"] = news_row[0]
                article["title"] = news_row[1]
                article["content"] = news_row[2]
                article["category"] = news_row[3]
                article["sex"] = news_row[4]
                if news_row[6] is None:
                    article["blineIds"] = []
                else:
                    article["blineIds"] = [int(bline_id) for bline_id in news_row[6].split(",")]
                # statement = "SELECT t.id, t.name FROM tags t JOIN news_tags nt ON t.id=nt.tag_id WHERE nt.news_id={};".format(article["id"])
                # tag_rows = conn.execute(statement).fetchall()
                # tags = []
                # for tag_row in tag_rows:
                #     tag = {}
                #     tag["value"] = str(tag_row[0])
                #     tag["name"] = tag_row[1]
                #     tag["type"] = "Tag"
                #     tags.append(tag)
                # article["tags"] = tags
                if news_row[5] is None:
                    article["tags"] = []
                else:
                    article["tags"] = [{"value": tag.split(":")[0], "name": tag.split(":")[1], "type": "Tag"} for tag in news_row[5].split(",")]
                statement = "SELECT twitter_fav, twitter_rt FROM sns_counts where targetable_type='News' and targetable_id={};".format(article["id"])
                sns_count_rows = conn.execute(statement).fetchall()
                if len(sns_count_rows) != 0:
                    article["fav"] = str(sns_count_rows[0][0])
                    article["rt"] = str(sns_count_rows[0][1])
                else:
                    article["fav"] = "0"
                    article["rt"] = "0"
                articles.append(article)
        return articles




        # statement = "SELECT news_id FROM collections where 1=1"
        # if len(bline_ids)>0:
        #     statement += " and bline_id in ({})".format(','.join([str(bline_id) for bline_id in bline_ids]))
        # if len(season_ids)>0:
        #     statement += " and season_id in ({})".format(','.join(["'" + season_id + "'" for season_id in season_ids]))
        # if len(cities)>0:
        #     statement += " and city in ({})".format(','.join(["'" + city + "'" for city in cities]))
        # if len(genres)>0:
        #     statement += " and genre in ({})".format(','.join(["'" + genre + "'" for genre in genres]))
        # if len(sexes)>0:
        #     statement += " and sex in ({})".format(','.join(["'" + sex + "'" for sex in sexes]))
        # statement += " order by id desc"
        # statement += ";"
        # news_ids = []
        # with self.db.connect() as conn:
        #     rows = conn.execute(statement).fetchall()
        #     for row in rows:
        #         news_ids.append(str(row[0]))
        #
        # statement = "SELECT image_id FROM image_details where 1=1"
        # if len(item_groups)>0:
        #     statement += " and item_group in ({})".format(','.join(["'" + item_group + "'" for item_group in item_groups]))
        # if len(materials)>0:
        #     statement += " and material in ({})".format(','.join(["'" + material + "'" for material in materials]))
        # if len(patterns)>0:
        #     statement += " and pattern in ({})".format(','.join(["'" + pattern + "'" for pattern in patterns]))
        # if len(colors)>0:
        #     statement += " and color in ({})".format(','.join(["'" + color + "'" for color in colors]))
        # statement += " order by id desc"
        # statement += ";"
        # image_ids = []
        # with self.db.connect() as conn:
        #     rows = conn.execute(statement).fetchall()
        #     for row in rows:
        #         image_ids.append(str(row[0]))

        # statement = "SELECT * FROM images where 1=1"
        # if len(image_ids)>0:
        #     statement += " and id in ({})".format(','.join(["'" + image_id + "'" for image_id in image_ids]))
        # if len(news_ids)>0:
        #     statement += " and new_id in ({})".format(','.join(["'" + news_id + "'" for news_id in news_ids]))
        # statement += " order by id desc"
        # statement += ";"
        # images = []
        # with self.db.connect() as conn:
        #     image_rows = conn.execute(statement).fetchall()
        #     for image_row in image_rows:
        #         image = {}
        #         image["id"] = image_row[0]
        #         image["newsId"] = image_row[5]
        #         image["imageName"] = image_row[1]
        #         statement = "SELECT id, season_id, city, genre, sex, bline_id FROM collections where news_id = {};".format(image["newsId"])
        #         collection_row = conn.execute(statement).fetchall()[0]
        #         image["collectionId"] = collection_row[0]
        #         image["brandId"] = collection_row[5]
        #         season_id = collection_row[1]
        #         image["place"] = collection_row[2]
        #         image["gender"] = collection_row[4]
        #         image["genre"] = collection_row[3]
        #         statement = "SELECT name_en FROM seasons where id = {};".format(season_id)
        #         image["season"] = conn.execute(statement).fetchall()[0][0]
        #         statement = "SELECT `name`, name_en FROM blines where id = {};".format(image["brandId"])
        #         bline_row = conn.execute(statement).fetchall()[0]
        #         image["brandName"] = bline_row[0]
        #         image["brandNameEn"] = bline_row[1]
        #         images.append(image)
        # return statement
