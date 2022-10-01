# Code imports
import xml.etree.ElementTree as ET
# Imports for extracting ZIP file from URL
from io import BytesIO
import urllib.request
from zipfile import ZipFile
# Imports for converting xml data  to csv
import pandas as pd
# Logging imports
import logging

logging.basicConfig(level=logging.INFO,  filename='data_file.log',
                    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')


class SteeleyeXMLTask:
    def parse_xml(self):
        """
        Function to parse the XML from given link, go to first download link
        whose file_type is DLTINS, and download the corresponding zip file.

        Returns: (str) link of required zip file
        """

        zip_link = ""
        tree = ET.parse('data_xml.xml')
        root = tree.getroot()

        for child in root[1][0].iter():
            if (child.attrib.get('name') == 'download_link'):
                zip_link = child.text

        # sources: https://docs.python.org/3/library/xml.etree.elementtree.html

        return zip_link

    def pull_from_zip(self, zip_link):
        """ Function to go to ZIP-file link, download, extract, and save the
         XML file in suitable system directory.

         Returns: (str) filepath of final saved XML file"""

        r = urllib.request.urlopen(str(zip_link))
        file_path = 'D:/Steeleye/DLTINS_20210117_01of01'

        with r as zipresp:
            with ZipFile(BytesIO(zipresp.read())) as zfile:
                zfile.extractall(file_path)

        # sources: https://svaderia.github.io/articles/downloading-and-unzipping-a-zipfile/ with modification
        # Modification involved using urllib.request.urlopen(str(link)) instead of
        # just passing URL to urlopen(URL) which led to type error.
        # Link: https://stackoverflow.com/questions/4981977/how-to-handle-response-encoding-from-urllib-request-urlopen-to-avoid-typeerr Ivan_Klass answer
        complete_file_path = file_path + '/DLTINS_20210117_01of01.xml'
        return complete_file_path

    def retrieve_data_from_xml(self, complete_file_path):
        """
        Function to retrieve the desired attributes, namely Id, FullNm, ClssfctnTp,
        CmmdtyDerivInd, NtnlCcy, and Issr.

        Returns: <zip object> zipped object containing lists of corresponding attribute values.

        """
        id_list = []
        fn_list = []
        ct_list = []
        cdi_list = []
        nc_list = []
        issr_list = []

        tree = ET.parse(
            complete_file_path)
        root = tree.getroot()

        # FinInstrmGnlAttrbts specific attributes retrieval
        e1 = ".//{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts"
        for ele in root[1][0].findall(e1):
            for e in ele:
                if e.tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Id':
                    id_list.append(e.text)
                elif e.tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FullNm':
                    fn_list.append(e.text)
                elif e.tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ClssfctnTp':
                    ct_list.append(e.text)
                elif e.tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}CmmdtyDerivInd':
                    cdi_list.append(e.text)
                elif e.tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy':
                    nc_list.append(e.text)

        # ISSR retrieval
        e2 = ".//{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Issr"
        for issr in root[1][0].findall(e2):
            issr_list.append(issr.text)

        # Sources:
        # 1. findall rough idea: https://www.datacamp.com/tutorial/python-xml-elementtree  and https://stackoverflow.com/questions/70926664/how-can-i-find-an-element-by-name-in-a-xml-file As per accepted answer at this link, you need to specify the xmlns inside the search string for each element.
        # 2. suggestion for findall Xpath: https://stackoverflow.com/questions/1319385/using-xpath-in-elementtree Chetan_vasudevan answer

        return (zip(id_list, fn_list, ct_list, cdi_list,
                    nc_list, issr_list))

    def save_data_to_csv(self, zipped_object):
        """
        Function to save the extracted values in a CSV file with desired
        header. A pandas DataFrame is created which gets saved as
        the CSV file.

        """

        header_list = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm',
                       'FinInstrmGnlAttrbts.ClssfctnTp',
                       'FinInstrmGnlAttrbts.CmmdtyDerivInd',
                       'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']

        df = pd.DataFrame(zipped_object, columns=header_list)
        df.to_csv("xml_to_csv_data.csv", index=False)


st_obj = SteeleyeXMLTask()

zip_link = st_obj.parse_xml()
complete_file_path = st_obj.pull_from_zip(zip_link)
zipped_object = st_obj.retrieve_data_from_xml(complete_file_path)
st_obj.save_data_to_csv(zipped_object)

"""The above CSV file has been saved to an AWS S3 bucket. 
To access the file, please sign into your AWS account and go to:
https://myxmldatabucket.s3.ap-south-1.amazonaws.com/xml_to_csv_data.csv

Note: "Read" access has been granted only to Authenticated users group (i.e. anyone with an AWS account).
Without login, an access denied error will be obtained.
Alternatively, you can check the screenshot of the file upload to the S3 bucket (S3_bucket_Screenshot.png file).
"""
