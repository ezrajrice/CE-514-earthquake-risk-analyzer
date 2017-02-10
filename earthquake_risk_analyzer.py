"""
Title: earthquake_risk_analyzer.py
Authors: Ezra Rice, Michael Cope
Date: 30 January, 2017
Description:
    This script is in partial fulfillment of Project 1 for CE En 514 at Brigham Young University, taught by
    Dr. Dan Ames.
"""

# Import required modules
import arcpy
import os
import urllib2
from lxml import etree
import zipfile
import csv
import shutil


class EarthquakeAnalyzer:
    def __init__(self):
        # Collect script parameters and set defaults if parameters are not given
        self.state = arcpy.GetParameterAsText(0)
        if self.state is None or not self.state:
            self.state = 'UT'
        self.start_year = int(arcpy.GetParameterAsText(1))
        if self.start_year is None or not self.start_year:
            self.start_year = 2013
        self.end_year = int(arcpy.GetParameterAsText(2))
        if self.end_year is None or not self.end_year:
            self.end_year = self.start_year
        self.output_file_location = arcpy.GetParameterAsText(3)
        if self.output_file_location is None or not self.output_file_location:
            self.output_file_location = os.getcwd()  # Gets the current working directory
        self.dem = arcpy.GetParameterAsText(4)
        if self.dem is None or not self.dem:
            self.dem = self.output_file_location + '\\DEM\\StatewideDEM_90meter.dem'

        # Specify other variables
        # Only variables that are used between methods go here
        self.temp_folder = self.output_file_location + "\\temp\\"
        self.layers_folder_path = self.temp_folder + "layers\\"
        self.rasters_folder_path = self.temp_folder + "rasters\\"
        self.event_metadata = {}
        self.symbology_layer = os.path.dirname(os.path.realpath(__file__)) + "\\atlas_template\\symbology.lyr"

        # Environment settings
        arcpy.CheckOutExtension("spatial")
        arcpy.env.overwriteOutput = True
        arcpy.env.extent = "MAXOF"

        if not os.path.isdir(self.temp_folder):
            os.mkdir(self.temp_folder)
        if not os.path.isdir(self.layers_folder_path):
            os.mkdir(self.layers_folder_path)
        if not os.path.isdir(self.rasters_folder_path):
            os.mkdir(self.rasters_folder_path)
        if not os.path.isdir(self.temp_folder + "raw_data\\"):
            os.mkdir(self.temp_folder + "raw_data\\")

        # Create pdfDocument
        self.pdf_template = os.path.dirname(os.path.realpath(__file__)) + "\\atlas_template\\template.mxd"
        self.pdfDoc = arcpy.mapping.PDFDocumentCreate(self.output_file_location + "\\Earthquake_report.pdf")

    def collect_data(self, event_year):
        """
        Assigned to Ezra Rice
        Collects earthquake data from USGS REST api for the given years
        Extracts raw data from zip files to folder '<output_directory>\temp\raw_data\
        """
        event_ids = []
        # Get html DOM tree from web page
        url = 'http://earthquake.usgs.gov/earthquakes/shakemap/list.php?y={0}&n={1}'.format(event_year, self.state.lower())
        event_page = urllib2.urlopen(url)
        a = event_page.read()  # read the page
        html = etree.HTML(a)  # convert to DOM tree
        tr_rows = html.xpath('//table[@id="tblResults"]//tbody//tr')  # get table rows in table results
        # Filter returned table rows based on state
        for tr in tr_rows:
            td_event_name = tr.xpath('td')[1].xpath('a')[0].text
            # Cannot guarantee event_state_id will contain ', '
            # In such cases, assume text value contains only the state
            try:
                event_state_id = td_event_name.split(', ')[1]
            except:
                event_state_id = td_event_name
            # add event id to list if in correct state
            if event_state_id == self.state:
                event_ids.append(tr.xpath('td')[3].text)

        # Make temporary directory to contain zip files
        if not os.path.isdir(self.output_file_location + '\\temp\\'):
            os.mkdir(self.output_file_location + '\\temp\\')

        arcpy.AddMessage('Downloading xyz raw data for year {0}...'.format(event_year))

        for event_id in event_ids:
            try:
                download_url = 'http://earthquake.usgs.gov/earthquakes/shakemap/{0}/' \
                               'shake/{1}/download/grid.xyz.zip'.format(self.state.lower(), event_id)
                zip_name = '{0}.zip'.format(event_id)
                output_path = self.output_file_location + '\\temp\\' + zip_name
                data = urllib2.urlopen(download_url)
                open(output_path, 'wb').write(data.read())
                zip_ref = zipfile.ZipFile(output_path, 'r')
                zip_ref.extractall(self.output_file_location + '\\temp\\raw_data\\')
                # Rename the file so we don't get duplicates
                os.rename(self.output_file_location + '\\temp\\raw_data\\grid.xyz',
                          self.output_file_location + '\\temp\\raw_data\\{0}.xyz'.format(event_id))
                zip_ref.close()

                # let os catch up
                for sleep in range(1000):
                    pass
                # Clean up by removing the zip archive that was just extracted from
                os.remove(output_path)
            except ValueError:
                arcpy.AddMessage('Raw data not available for event {0}, year {1}'.format(event_id, event_year))

        arcpy.AddMessage('Finished downloading raw data for year {0}'.format(event_year))

    def convert_xyz_to_csv(self):
        """
        Assigned to Ezra Rice
        This method converts all of the xyz raw data files to csv format to be
        converted later to shapefiles
        """
        original_xyz = []
        for raw_data_file in os.listdir(self.output_file_location + '\\temp\\raw_data\\'):
            if raw_data_file.endswith(".xyz"):
                csv_file = raw_data_file.replace('.xyz', '.csv')
                with open(self.temp_folder + 'raw_data\\' + raw_data_file, "rb") as infile:
                    original_xyz.append(['lon', 'lat', 'Pga', 'Pgv', 'abc'])
                    first_line = True
                    for line in infile:
                        if first_line:
                            first_line = False
                            meta_row = line.split(' ')
                            self.event_metadata[meta_row[0]] = {'magnitude': meta_row[1],
                                                                'lon': meta_row[2],
                                                                'lat': meta_row[3],
                                                                'date': '{1} {0} {2}'.format(meta_row[4], meta_row[5],
                                                                                             meta_row[6])}
                            continue
                        newline = line.replace('\n', '')
                        newlinesplit = newline.split(' ')
                        original_xyz.append(newlinesplit)

                with open(self.temp_folder + 'raw_data\\' + csv_file, 'wb') as outfile:
                    writer = csv.writer(outfile)
                    writer.writerows(original_xyz)

    def geoprocess_data(self):
        """
        Assigned to Michael Cope and Ezra Rice
        This section converts each csv file into a raster layer. This resultant raster is then multiplied by
        the slope raster to create a final intensity raster.
        """
        # Process: Slope
        arcpy.gp.Slope_sa(self.dem, self.rasters_folder_path + "slope_raster", "DEGREE", "1")

        for csv_file in os.listdir(self.output_file_location + '\\temp\\raw_data\\'):
            if csv_file.endswith(".csv"):
                output_layer = csv_file.replace('.csv', '_layer')
                event_id = csv_file.replace('.csv', '')
                input_table = self.output_file_location + '\\temp\\raw_data\\' + csv_file
                arcpy.AddMessage("Generating intensity raster for event {0}".format(event_id))

                # Create layer
                arcpy.MakeXYEventLayer_management(input_table, "lon", "lat", output_layer,
                                                  "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],\
                                                                  PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];\
                                                                  -400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119522E-09;\
                                                                  0.001;0.001;IsHighPrecision")

                arcpy.gp.Idw_sa(output_layer, "abc", self.rasters_folder_path + event_id, "", "2", "VARIABLE 12", "")
                arcpy.AddMessage("IDW from event {0}".format(event_id))

                # extract by mask
                clipped_raster = arcpy.sa.ExtractByMask(self.rasters_folder_path + "slope_raster",
                                                        self.rasters_folder_path + event_id)

                # Create intensity rasters
                arcpy.gp.Times_sa(self.rasters_folder_path + event_id, clipped_raster,
                                  self.rasters_folder_path + event_id + "_i")

                arcpy.AddMessage("Intensity raster successfully created for event {0}".format(event_id))

    def generate_atlas(self):
        """
        Assigned to Michael Cope
        """
        for intensity_raster in os.listdir(self.rasters_folder_path):
            if os.path.isdir(self.rasters_folder_path + intensity_raster) and intensity_raster.endswith('_i'):
                arcpy.AddMessage("Creating PDF for event {0}".format(intensity_raster))
                event_id = intensity_raster[:-2]
                mxd = arcpy.mapping.MapDocument(self.pdf_template)

                # lists the current dataframes in the mxd file and selects the first one
                df = arcpy.mapping.ListDataFrames(mxd)[0]

                # convert raster to a layer and add to the mxd file
                resultant_layer = arcpy.MakeRasterLayer_management(self.rasters_folder_path + intensity_raster,
                                                                   intensity_raster)
                result = arcpy.SaveToLayerFile_management(intensity_raster,
                                                          self.layers_folder_path + intensity_raster)

                extent_layer = arcpy.MakeRasterLayer_management(self.rasters_folder_path + event_id,
                                                                event_id)

                # Apply symbology
                arcpy.ApplySymbologyFromLayer_management(resultant_layer,
                                                         self.symbology_layer)

                layerfile = resultant_layer.getOutput(0)
                extent_layerfile = extent_layer.getOutput(0)
                arcpy.mapping.AddLayer(df, layerfile, "AUTO_ARRANGE")
                arcpy.mapping.AddLayer(df, extent_layerfile, "AUTO_ARRANGE")

                # describe raster so it can zoom to area affected by earthquake
                selected_layer = arcpy.mapping.ListLayers(mxd, str(event_id), df)[0]
                df.extent = selected_layer.getSelectedExtent()

                # refresh the map and table of contents
                arcpy.RefreshActiveView()
                arcpy.RefreshTOC()
                for sleep in range(1000):
                    pass

                # Hide all layers not associated with current event
                for layer in arcpy.mapping.ListLayers(mxd, "", df):
                    try:
                        if layer.name == event_id + "_i" or layer.name == 'World_Imagery':
                            layer.visible = True
                        else:
                            layer.visible = False
                    except:
                        pass

                # Collect metadata for atlas page
                title = 'USGS Event Code: {0}'.format(event_id)
                magnitude = 'Magnitude: {0}'.format(self.event_metadata[str(event_id)]['magnitude'])
                lon = 'Longitude: {0}'.format(self.event_metadata[str(event_id)]['lon'])
                lat = 'Latitude: {0}'.format(self.event_metadata[str(event_id)]['lat'])
                date = 'Date: {0}'.format(self.event_metadata[str(event_id)]['date'])

                # Add metadata to map
                for elem in arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT"):
                    if elem.text == "Title":
                        elem.text = title
                    elif elem.text == "Magnitude":
                        elem.text = magnitude
                    elif elem.text == "Longitude":
                        elem.text = lon
                    elif elem.text == "Latitude":
                        elem.text = lat
                    elif elem.text == "Date":
                        elem.text = date

                # refresh the map and table of contents
                arcpy.RefreshActiveView()
                arcpy.RefreshTOC()
                for sleep in range(1000):
                    pass

                current_pdf = self.temp_folder + '\\' + event_id + '.pdf'
                # Export current atlas page to pdf
                arcpy.mapping.ExportToPDF(mxd, current_pdf)  # exports current layout to a new pdf

                # Revert element tags
                for elem in arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT"):
                    if elem.text == title:
                        elem.text = "Title"
                    elif elem.text == magnitude:
                        elem.text = "Magnitude"
                    elif elem.text == lon:
                        elem.text = "Longitude"
                    elif elem.text == lat:
                        elem.text = "Latitude"
                    elif elem.text == date:
                        elem.text = "Date"

                # Append atlas page to final report
                self.pdfDoc.appendPages(str(current_pdf))

    def clean_temp(self):
        arcpy.AddMessage("Removing temporary files...")
        try:
            shutil.rmtree(self.temp_folder)
            arcpy.AddMessage("Temporary files deleted.")
        except:
            arcpy.AddMessage("Error occurred trying to delete temporary files.")

ea = EarthquakeAnalyzer()
for year in range(ea.start_year, ea.end_year + 1):  # add 1 to end_year so the loop completes that year
    ea.collect_data(year)
ea.convert_xyz_to_csv()
ea.geoprocess_data()
ea.generate_atlas()
ea.clean_temp()
