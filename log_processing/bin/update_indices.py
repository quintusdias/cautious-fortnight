import argparse
import sqlite3

import pandas as pd


class Update(object):

    def __init__(self, project, dbfile):

        self.project = project
        self.dbfile = dbfile

    def update_ip_address_table(self):
        conn = sqlite3.connect(self.dbfile)
        cursor = conn.cursor()

        sql = "PRAGMA foreign_keys=off"
        conn.execute(sql)

        df = pd.read_sql('select count(*) from ip_address_logs', conn)
        print(df)

        sql = "ALTER TABLE ip_address_logs RENAME TO _ip_address_logs_old"
        cursor.execute(sql)

        # Create the IP address logs table.
        sql = """
              CREATE TABLE ip_address_logs (
                  date integer,
                  id integer,
                  hits integer,
                  errors integer,
                  nbytes integer,
                  CONSTRAINT fk_known_ip_address_id
                      FOREIGN KEY (id)
                      REFERENCES known_ip_addresses(id)
                      ON DELETE CASCADE
              )
              """
        cursor.execute(sql)
        sql = """
              CREATE INDEX idx_ip_address_logs_date
              ON ip_address_logs(date)
              """
        cursor.execute(sql)

        sql = """
              INSERT INTO ip_address_logs
            SELECT a.date, a.id, SUM(a.hits) as hits, SUM(a.errors) as errors,
                   SUM(a.nbytes) as nbytes
            FROM _ip_address_logs_old a
            GROUP BY a.date, a.id
            ORDER BY a.date
            """
        cursor.execute(sql)

        sql = """
              DROP TABLE _ip_address_logs_old
              """
        cursor.execute(sql)

        conn.commit()
        conn.execute('PRAGMA foreign_keys=on')

        df = pd.read_sql('select count(*) from ip_address_logs', conn)
        print(df)

    def update_referer_table(self):

        conn = sqlite3.connect(self.dbfile)
        cursor = conn.cursor()

        sql = "PRAGMA foreign_keys=off"
        conn.execute(sql)

        df = pd.read_sql('select count(*) from referer_logs', conn)
        print(df)

        sql = "ALTER TABLE referer_logs RENAME TO _referer_logs_old"
        cursor.execute(sql)

        sql = """
              CREATE TABLE referer_logs (
                  date integer,
                  id integer,
                  hits integer,
                  errors integer,
                  nbytes integer,
                  CONSTRAINT fk_known_referers_id
                      FOREIGN KEY (id)
                      REFERENCES known_referers(id)
                      ON DELETE CASCADE
              )
              """
        cursor.execute(sql)

        # Unfortunately the index cannot be unique here.
        sql = """
              CREATE UNIQUE INDEX idx_referer_logs_date
              ON referer_logs(date, id)
              """
        cursor.execute(sql)

        sql = """
              INSERT INTO referer_logs
            SELECT a.date, a.id, SUM(a.hits) as hits, SUM(a.errors) as errors,
                   SUM(a.nbytes) as nbytes
            FROM _referer_logs_old a
            GROUP BY a.date, a.id
            ORDER BY a.date
            """
        cursor.execute(sql)

        sql = """
              DROP TABLE _referer_logs_old
              """
        cursor.execute(sql)

        conn.commit()
        conn.execute('PRAGMA foreign_keys=on')

        df = pd.read_sql('select count(*) from referer_logs', conn)
        print(df)

    def update_services_table(self):

        conn = sqlite3.connect(self.dbfile)
        cursor = conn.cursor()

        sql = "PRAGMA foreign_keys=off"
        conn.execute(sql)

        df = pd.read_sql('select count(*) from service_logs', conn)
        print(df)

        sql = "ALTER TABLE service_logs RENAME TO _service_logs_old"
        cursor.execute(sql)

        sql = """
              CREATE TABLE service_logs (
                  date integer,
                  id integer,
                  hits integer,
                  errors integer,
                  nbytes integer,
                  export_mapdraws integer,
                  wms_mapdraws integer,
                  CONSTRAINT fk_known_services_id
                      FOREIGN KEY (id)
                      REFERENCES known_services(id)
                      ON DELETE CASCADE
              )
              """
        cursor.execute(sql)

        sql = """
              CREATE UNIQUE INDEX idx_service_logs_date
              ON service_logs(date, id)
              """
        cursor.execute(sql)

        sql = """
              INSERT INTO service_logs
            SELECT
                a.date, a.id,
                SUM(a.hits) as hits,
                SUM(a.errors) as errors,
                SUM(a.nbytes) as nbytes,
                SUM(a.export_mapdraws) as export_mapdraws,
                SUM(a.wms_mapdraws) as wms_mapdraws
            FROM _service_logs_old a
            GROUP BY a.date, a.id
            ORDER BY a.date
            """
        cursor.execute(sql)

        sql = """
              DROP TABLE _service_logs_old
              """
        cursor.execute(sql)

        conn.commit()
        conn.execute('PRAGMA foreign_keys=on')

        df = pd.read_sql('select count(*) from service_logs', conn)
        print(df)

    def update_user_agent_table(self):
        conn = sqlite3.connect(self.dbfile)
        cursor = conn.cursor()

        sql = "PRAGMA foreign_keys=off"
        conn.execute(sql)

        df = pd.read_sql('select count(*) from user_agent_logs', conn)
        print(df)

        sql = "ALTER TABLE user_agent_logs RENAME TO _user_agent_logs_old"
        cursor.execute(sql)

        sql = """
              CREATE TABLE user_agent_logs (
                  date integer,
                  id integer,
                  hits integer,
                  errors integer,
                  nbytes integer,
                  CONSTRAINT fk_user_agents_id
                      FOREIGN KEY (id)
                      REFERENCES known_user_agents(id)
                      ON DELETE CASCADE
              )
              """
        cursor.execute(sql)

        sql = """
              CREATE UNIQUE INDEX idx_user_agent_logs_date
              ON user_agent_logs(date, id)
              """
        cursor.execute(sql)

        sql = """
              INSERT INTO user_agent_logs
            SELECT a.date, a.id, SUM(a.hits) as hits, SUM(a.errors) as errors,
                   SUM(a.nbytes) as nbytes
            FROM _user_agent_logs_old a
            GROUP BY a.date, a.id
            ORDER BY a.date
            """
        cursor.execute(sql)

        sql = """
              DROP TABLE _user_agent_logs_old
              """
        cursor.execute(sql)

        conn.commit()
        conn.execute('PRAGMA foreign_keys=on')

        df = pd.read_sql('select count(*) from user_agent_logs', conn)
        print(df)

    def update_summary_table(self):

        conn = sqlite3.connect(self.dbfile)
        cursor = conn.cursor()

        sql = "PRAGMA foreign_keys=off"
        conn.execute(sql)

        df = pd.read_sql('select count(*) from summary', conn)
        print(df)

        sql = "ALTER TABLE summary RENAME TO _summary_old"
        cursor.execute(sql)

        sql = """
              CREATE TABLE summary (
                  date integer,
                  hits integer,
                  errors integer,
                  nbytes integer
              )
              """
        cursor.execute(sql)

        sql = """
              INSERT INTO summary
            SELECT
                date,
                SUM(hits) as hits,
                SUM(errors) as errors,
                SUM(nbytes) as nbytes
            FROM _summary_old
            GROUP BY date
            ORDER BY date
            """
        cursor.execute(sql)

        sql = """
              DROP TABLE _summary_old
              """
        cursor.execute(sql)

        conn.commit()
        conn.execute('PRAGMA foreign_keys=on')

        df = pd.read_sql('select count(*) from summary', conn)
        print(df)

    def run(self):
        self.update_ip_address_table()
        self.update_referer_table()
        self.update_services_table()
        self.update_user_agent_table()
        self.update_summary_table()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('dbfile')

    args = parser.parse_args()

    o = Update(args.project, args.dbfile)
    o.run()
