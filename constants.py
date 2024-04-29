#########################################################
#Arquivo usado para armazenar as variáveis usadas na API#
#########################################################

import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), 'configurações/config1.local'))

YOUTUBE_API_KEY = parser.get('youtube', 'API_KEY')
PLAYLIST_ID     = parser.get('youtube', 'PLAYLIST_ID') #Video sobre problemas de física.