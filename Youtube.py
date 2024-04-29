####################################################################################################
#Arquivo responsável por retornar os dados provenientes da API do youtube e envia-los para o broker#
####################################################################################################


import requests
import json
import logging

from constants import YOUTUBE_API_KEY, PLAYLIST_ID

from pprint import pprint

from kafka import KafkaProducer


'''
Faz uma solicitação à API do YouTube, retorna os dados da página em formato JSON
'''
def req_páginas(url, parametros, token=None):
    param = {**parametros, 'key': YOUTUBE_API_KEY, 'token': token}
    req = requests.get(url, param)
    carga = json.loads(req.text)

    return carga

'''
Retorna uma lista de itens da página da API do YouTube, sobre todas as páginas.
'''
def req_páginas_listas(url, parametros, token=None):
    while True:
        carga = req_páginas(url, parametros, token)
        yield from carga['items']

        token = carga.get('nextPageToken')
        if token is None:
            break

'''
Formatação da requisição e preparação para envio dos dados para o broker.
'''
def formata_req(video):
    req_video = {
        'Título:': video['snippet']['title'],
        'Likes': int(video['statistics'].get('likeCount', 0)),
        'Comentários': int(video['statistics'].get('commentCount', 0)),
        'Visualizações': int(video['statistics'].get('viewCount', 0)),
        'Favoritos': int(video['statistics'].get('favoriteCount', 0)),
        'Thumb': video['snippet']['thumbnails']['default']['url']
    }
    return req_video


'''
 Configura o logging, cria o producer, busca vídeos de uma playlist específica, formata os dados envia para o tópico no Kafka.
'''
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])  

    for video_item in req_páginas_listas(
            "https://www.googleapis.com/youtube/v3/playlistItems",
            {'playlistId': PLAYLIST_ID, 'part': 'snippet,contentDetails'},
            None):
        video_id = video_item['contentDetails']['videoId']

        for video in req_páginas_listas(
                "https://www.googleapis.com/youtube/v3/videos",
                {'id': video_id, 'part': 'snippet,statistics'},
                None):
            #logging.info("Video: %s", pprint(formata_req(video)))
            producer.send('videos_do_youtube', json.dumps(formata_req(video)).encode('utf-8'), 
                          key=video_id.encode('utf-8'))
            #producer.flush()
