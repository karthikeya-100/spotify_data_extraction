import api_connection

# creating empty lists for storing albums,artists and tracks data
albums_data = []
artists_data = []
tracks_data = []

# Top 10 global songs playlist link
playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"

# Extracting playlist ID
playlist_id = playlist_link.split("/")[-1]

# Getting playlist information using spotify object providing playlist ID
raw_data = api_connection.spotify.playlist_tracks(playlist_id=playlist_id)

# Getting albums data 
for item in raw_data['items']:
    album_id = item['track']['album']['id']
    album_name = item['track']['album']['name']
    release_date = item['track']['album']['release_date']
    total_tracks = item['track']['album']['total_tracks']
    album_url = item['track']['album']['external_urls']['spotify']
    album_data = {
        "album_id" : album_id,
        "album_name" : album_name,
        "release_date" : release_date,
        "total_tracks" : total_tracks,
        "album_url" : album_url
    }
    albums_data.append(album_data)

# Getting artists data
for item in raw_data['items']:
    for artist in item['track']['artists']:
        artist_id = artist['id']
        artist_name = artist['name']
        artist_url = artist['external_urls']['spotify']
        artist_data = {
            "artist_id" : artist_id,
            "artist_name" : artist_name,
            "artist_url" : artist_url
        }
        artists_data.append(artist_data)

# Getting tracks data
for item in raw_data['items']:
    track_id = item['track']['id']
    track_name = item['track']['name']
    track_added = item['added_at']
    track_number = item['track']['track_number']
    track_popularity = item['track']['popularity']
    track_duration = item['track']['duration_ms']
    track_url = item['track']['external_urls']['spotify']
    album_id = item['track']['album']['id']
    artists_id = ",".join([artist['id'] for artist in item['track']['artists']])
    track_data = {
        "track_id" : track_id,
        "track_name" : track_name,
        "track_added" : track_added,
        "track_number" : track_number,
        "track_popularity" : track_popularity,
        "track_duration" : track_duration,
        "track_url" : track_url,
        "album_id" : album_id,
        "artists_id" : artists_id
    }
    tracks_data.append(track_data)

