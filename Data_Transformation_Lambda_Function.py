import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
def lambda_handler(event, context):
    
    def get_albums_data(data):
        albums_data = []
        for item in data['items']:
            album_data = {
                "album_id" : item['track']['album']['id'],
                "album_name" : item['track']['album']['name'],
                "release_date" : item['track']['album']['release_date'],
                "total_tracks" : item['track']['album']['total_tracks'],
                "album_url" : item['track']['album']['external_urls']['spotify']
            }
            albums_data.append(album_data)
        return albums_data
    
    def get_artists_data(data):
        artists_data = []
        for item in data['items']:
            for artist in item['track']['artists']:
                artist_data = {
                    "artist_id" : artist['id'],
                    "artist_name" : artist['name'],
                    "artist_url" : artist['external_urls']['spotify']
                }
                artists_data.append(artist_data)
        return artists_data
    
    def get_tracks_data(data):
        tracks_data = []
        for item in data['items']:
            track_data = {
                'track_id' : item['track']['id'],
                "track_name" : item['track']['name'],
                "track_added" : item['added_at'],
                "track_number" : item['track']['track_number'],
                "track_popularity" : item['track']['popularity'],
                "track_duration" : item['track']['duration_ms'],
                "track_url" : item['track']['external_urls']['spotify'],
                "album_id": item['track']['album']['id'],
                "artists_id" : ",".join([artist['id'] for artist in item['track']['artists']]),
            }
            tracks_data.append(track_data)
        return tracks_data
            
    
    s3 = boto3.client("s3")
    
    Bucket = "spotify-data-pipeline-karthikeya"
    Key = "raw_data/to_processed/"
    
    # Creating empty lists which stores json data for each file and key
    spotify_data = []
    spotify_keys = []
    
    # Listing objects present under given prefix
    for file in s3.list_objects(Bucket = Bucket,Prefix = Key)['Contents']:
        file_key = file['Key'] # storing current key
        if file_key.split(".")[-1] == 'json':
            #getting actual contents inside each object
            response = s3.get_object(Bucket = Bucket,Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
    # transforming raw data into albums,artists and tracks and converting them to dataframes
    for data in spotify_data:
        albums_list = get_albums_data(data)
        artists_list = get_artists_data(data)
        tracks_list = get_tracks_data(data)
        
        albums_df = pd.DataFrame(albums_list)
        artists_df = pd.DataFrame(artists_list)
        tracks_df = pd.DataFrame(tracks_list)
        
        # Converting date columns in albums and tracks to datetime data type
        albums_df['release_date'] = pd.to_datetime(albums_df['release_date'])
        tracks_df['track_added'] =  pd.to_datetime(tracks_df['track_added'])
        
        # Removing duplicates from artists data
        artists_df.drop_duplicates(['artist_id'],inplace=True)
        
        albums_key = f"transformed_data/album_data/album_transformed_{datetime.now()}.csv"
        artists_key = f"transformed_data/artists_data/artist_transformed_{datetime.now()}.csv"
        tracks_key = f"transformed_data/songs_data/song_transformed_{datetime.now()}.csv"
        
        # Converting dataframes to file like objects using StringIO
        
        albums_buffer = StringIO()
        albums_df.to_csv(albums_buffer)
        albums_content = albums_buffer.getvalue()
        s3.put_object(
            Bucket = Bucket,
            Key = albums_key,
            Body = albums_content   
            )
        
        artists_buffer = StringIO()
        artists_df.to_csv(artists_buffer)
        artists_content = artists_buffer.getvalue()
        s3.put_object(
            Bucket = Bucket,
            Key = artists_key,
            Body = artists_content
            )
            
        songs_buffer = StringIO()
        tracks_df.to_csv(songs_buffer)
        songs_content = songs_buffer.getvalue()
        s3.put_object(
            Bucket = Bucket,
            Key = tracks_key,
            Body = songs_content
            )
        
        
    # once file is processed, moving that file to processed folder
    for key in spotify_keys:
        copy_source = {
            'Bucket' : Bucket,
            'Key' : key
        }    
        s3.copy_object(CopySource = copy_source,Bucket = Bucket, Key = "raw_data/processed/"+key.split("/")[-1])
        s3.delete_object(Bucket=Bucket,Key=key)   
            
            
            
    