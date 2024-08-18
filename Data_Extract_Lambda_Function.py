import json
import os
import spotipy as sp
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
   
   # getting spotify client_id and client_secret
   client_id = os.environ.get('client_id')
   client_secret = os.environ.get('client_secret')
   
   # Getting access token to access spotipy api by providing client credentials
   Client_Credentials_Manager = SpotifyClientCredentials(client_id = client_id,client_secret = client_secret)
   
   # Creating spofipy object 
   sp_object = sp.Spotify(client_credentials_manager = Client_Credentials_Manager)
   
   # Getting top Global songs playlist link
   playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
   
   # Getting playlist id
   playlist_id = playlist_link.split("/")[-1]
   
   # Getting top Global songs data using playlist id
   raw_data = sp_object.playlist_tracks(playlist_id)
   
   # creating boto3 s3 client
   client = boto3.client("s3")
   
   # storing raw data into s3 bucket as object
   client.put_object(
         Bucket ="spotify-data-pipeline-karthikeya",
         Key = f"raw_data/to_processed/spotify_raw_data_{datetime.now()}.json",
         Body = json.dumps(raw_data)
      )
   
