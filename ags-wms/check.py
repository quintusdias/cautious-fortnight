# Standard library imports
import argparse
import datetime as dt
import io
import logging
import pathlib
import pprint
import sqlite3

# Third party library imports
from lxml import etree
import requests


class WMSCheck(object):

    def __init__(self, project, service=None):
        """
        Parameters
        ----------
        project : str
            Identifies the site.
        service : str
            If not None, restrict the checks to this one service.
        logger : 
            Print informative messages as needed.
        nsmap : 
            XML namespaces for a service.  This is retrieved over and over
            again.
        """
        self.project = project
        
        if service is None:
            self.excluded_service_name = None
            self.excluded_service_type = None
        else:
            parts = service.split('.')
            self.excluded_service_name = parts[0]
            self.excluded_service_type = parts[1]

        logging.basicConfig(level=logging.INFO, format='%(message)s')
        self.logger = logging.getLogger(__name__)

        self.initialize_database()

        # Open a requests session, don't validate SSL.
        self.s = requests.Session()
        self.s.verify = False

    def run(self):
        self.logger.info('running')

        services = self.get_list_of_services()
        for service in services:

            if (((self.excluded_service_name is not None) and
                 not ((service['name'] == self.excluded_service_name) and 
                      (service['type'] == self.excluded_service_type)))):
                # Exclude if not the specified service.
                continue

            # Add the database ID to the service description.
            service['id'] = self.validate_service_in_database(service)
            self.verify_service_wms(service)

        self.conn.commit()

    def verify_service_wms(self, service):

        service_name = f"{service['name']}/{service['type']}"
        self.logger.info(f"Looking at WMS for {service_name}")
        # Is WMS even supported for this service?
        url = (
            f"http://{self.project}.ncep.noaa.gov"
            f"/arcgis/rest/services/{service_name}"
        )
        params = {'f': 'json'}
        r = self.s.get(url, params=params)
        j = r.json()

        if service['type'] == 'FeatureServer':
            return

        # Looks like ImageServer always has WMS?
        if service['type'] == 'MapServer':
            if 'supportedExtensions' not in j.keys():
                self.logger.info(f"{service_name} supports no extensions")
                return

            if 'WMSServer' not in j['supportedExtensions']:
                self.logger.info(f"{service_name} does not support WMS")
                return

        # Get the GetCapabilities file.
        url = (
            f"http://{self.project}.ncep.noaa.gov"
            f"/arcgis/services/{service_name}/WMSServer"
        )
        params = {'request': 'GetCapabilities', 'service': 'WMS'}
        self.logger.info(url)
        try:
            r = self.s.get(url, params=params)
            r.raise_for_status()
        except Exception as e:
            self.logger.info(f"Can't get capabilites for {service_name}")
            self.log_wms_result(service, -1, success=False)
            return

        b = io.BytesIO(r.content)
        try:
            doc = etree.parse(b)
        except Exception as e:
            # Can't go forward if we cannot parse the GetCapabilities file.
            self.log_wms_result(service, -1, success=False)
            return

        # Fix the namespace!
        # There's an implied WMS namespace.  Just give it a proper name.  Then
        # keep it around in order to interrogate the WMS XML file as needed.
        nsmap = doc.getroot().nsmap
        nsmap['wms'] = nsmap[None]
        nsmap.pop(None)
        self.nsmap = nsmap

        # Get the list of image layer IDs.  Right now it looks like we are
        # interested in any layer that has a Name field and whose Title child
        # element does not have the words Boundary, Footprint, or Label in it.
        path = (
            '//wms:Layer['
            '    not(child::wms:Layer) and '
            '    not(contains(wms:Title/text(), "Boundary")) and '
            '    not(contains(wms:Title/text(), "Footprint")) and '
            '    not(contains(wms:Title/text(), "Label"))'
            ']'
        )
        elts = doc.xpath(path, namespaces=nsmap)
        for elt in elts:
            self.process_layer(service, elt)

    def process_layer(self, service, layer_elt):
        """
        Query the WMS layer.  Log the result in the database.

        Parameters
        ----------
        service : dict
            service configuration return from AGS.  Also has an 'id' key added
            that provides the row ID of the service instance in the database
        layer_elt : ElementTree Element
            XML for the layer from the GetCapabilities file for this service
        """
        service_name = f"{service['name']}/{service['type']}"

        # Get the WMS layer ID
        elt = layer_elt.xpath('wms:Name', namespaces=self.nsmap)[0]
        layer_id = int(elt.text)

        # Get the bounding box
        path = 'wms:BoundingBox[@CRS="EPSG:4326"]'
        elt = layer_elt.xpath(path, namespaces=self.nsmap)[0]
        minx = float(elt.attrib['minx'])
        maxx = float(elt.attrib['maxx'])
        miny = float(elt.attrib['miny'])
        maxy = float(elt.attrib['maxy'])
        bbox = f"{miny},{minx},{maxy},{maxx}"

        url = (
            f"http://{self.project}.ncep.noaa.gov"
            f"/arcgis/services/{service_name}/WMSServer"
        )

        parameters = {                                                          
            'SERVICE': 'WMS',                                                   
            'STYLES': '',                                                       
            'VERSION': '1.3.0',                                                 
            'REQUEST': 'GetMap',                                                
            'FORMAT': 'image/png',                                              
            'TRANSPARENT': 'true',                                              
            'WIDTH': 400,                                                       
            'HEIGHT': 400,                                                      
            'BBOX': bbox,                                                       
            'CRS': 'EPSG:4326',                                                 
            'LAYERS': layer_id,                                                 
        }  

        try:
            r = self.s.get(url, params=parameters)
            r.raise_for_status()
        except Exception as e:
            self.log_wms_result(service, layer_id, success=False)
            return

        if r.content[:4] != b'\x89PNG':
            self.log_wms_result(service, layer_id, success=False)
        else:
            self.log_wms_result(service, layer_id, success=True)


    def log_wms_result(self, service, layer_id, success=True):
        """
        Record the result of the test in the database.
        """
        sql = """
              INSERT INTO log (service_id, wms_id, ts, success)
              VALUES (?, ?, ?, ?)
              """
        parameters = (service['id'], layer_id, dt.datetime.now(), success)
        print(sql)
        print(parameters)
        self.cursor.execute(sql, parameters)

    def validate_service_in_database(self, service):
        """
        Make sure that the service is in the service table.  If it is not, add
        it.

        Returns
        -------
        ID of service in database
        """

        service_name =  f"{service['name']}.{service['type']}"

        self.logger.info(f"Validating {service_name}")

        sql = f"""
               SELECT id FROM service
               WHERE project_id = {self.project_id}
                     AND name = '{service_name}'
               """
        self.cursor.execute(sql)
        rs = self.cursor.fetchone()
        if rs is not None:
            return rs[0]

        # Must make an entry for this service and project in the service
        # database.
        self.logger.info(f"Adding {service_name} to service table...")
        sql = """
              INSERT INTO service (project_id, name)
              VALUES (?, ?)
              """
        self.cursor.execute(sql, (self.project_id, service_name))

        return self.cursor.lastrowid

    def get_list_of_services(self):
        """
        Get list of services from the ArcGIS server

        Returns
        -------
        lst
            List of services.  Format of the items is {'name': value, 'type':
            "MapServer|ImageServer|FeatureServer"}
        """
        lst = []

        url = f"http://{self.project}.ncep.noaa.gov/arcgis/rest/services"
        params = {'f': 'json'}
        r = self.s.get(url, params=params)
        j = r.json()

        for folder in j['folders']:
            if folder in ['Utilities']:
                continue

            url = (
                f"http://{self.project}.ncep.noaa.gov"
                f"/arcgis/rest/services/{folder}"
            )
            r = self.s.get(url, params=params)
            j = r.json()
            lst.extend(j['services'])

        return lst

    def verify_project_table(self):
        """
        Make sure that the project table exists.
        """
        sql = """
              SELECT name
              FROM sqlite_master
              WHERE type="table" AND name="project"
              """
        self.cursor.execute(sql)
        rs = self.cursor.fetchall()
        if rs is None:
            # Must create the table.
            self.logger.info('Must create project table')
            sql = """
                  CREATE TABLE project
                  (id INTEGER PRIMARY KEY, name TEXT NOT NULL)
                  """
            self.cursor.execute(sql)

        # Does the project exist?
        sql = """
              SELECT id FROM project WHERE name = ?
              """
        self.cursor.execute(sql, (self.project,))
        rs = self.cursor.fetchone()
        if rs is None:
            sql = """
                  INSERT INTO project (name) VALUES (?)
                  """
            self.cursor.execute(sql, (self.project,))
            self.project_id = self.cursor.lastrowid
        else:
            self.project_id = rs[0]

    def verify_service_table(self):
        """
        Does the service table exist?
        """
        sql = """
              SELECT name
              FROM sqlite_master
              WHERE type="table" AND name="service"
              """
        self.cursor.execute(sql)
        rs = self.cursor.fetchall()
        if len(rs) < 1:
            # Must create the table.
            self.logger.info('Must create service table')
            sql = """
                  CREATE TABLE service(
                      id INTEGER PRIMARY KEY,
                      project_id INTEGER,
                      name TEXT NOT NULL,
                      FOREIGN KEY(project_id) REFERENCES project(id)

                  )
                  """
            self.cursor.execute(sql)

            sql = """
                  INSERT INTO project (name) VALUES (?)
                  """
            self.cursor.execute(sql, (self.project,))

    def verify_log_table(self):
        """
        Does the log table exist?
        """
        sql = """
              SELECT name
              FROM sqlite_master
              WHERE type="table" AND name="log"
              """
        self.cursor.execute(sql)
        rs = self.cursor.fetchall()
        if rs is None:
            # Must create the table.
            self.logger.info('Must create log table')
            sql = """
                  CREATE TABLE log(
                      id INTEGER PRIMARY KEY,
                      service_id INTEGER,
                      wms_id INTEGER,
                      ts TIMESTAMP,
                      success INTEGER,
                      FOREIGN KEY(service_id) REFERENCES service(id)

                  )
                  """
            self.cursor.execute(sql)

    def initialize_database(self):

        path = pathlib.Path.home() / 'data' / 'sqlite3' / 'wms.db'
        detect_types = sqlite3.PARSE_COLNAMES | sqlite3.PARSE_DECLTYPES
        self.conn = sqlite3.connect(str(path), detect_types=detect_types)
        self.cursor = self.conn.cursor()

        self.verify_project_table()
        self.verify_service_table()
        self.verify_log_table()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    # Who are we checking?
    choices = [
        'idpgis.bldr',
        'nowcoast.bldr',
        'nowcoastqa.bldr',
        'idpgisqa.bldr',
        'nowcoastdev.bldr',
        'idpgisdev.bldr',
    ]
    help = "The site being checked"
    parser.add_argument('project', choices=choices, help=help)

    help = "Restrict WMS checks to this service"
    parser.add_argument('--service', help=help)

    args = parser.parse_args()

    obj = WMSCheck(args.project, service=args.service)
    obj.run()
