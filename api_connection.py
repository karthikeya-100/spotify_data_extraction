import spotipy as sp
from spotipy.oauth2 import SpotifyClientCredentials
from config import SPOTIPY_CLIENT_ID,SPOTIPY_CLIENT_SECRET

# Provides access token to create spotify object based on provided cliend ID and client secredt
Client_Credentials_manager = SpotifyClientCredentials(
    client_id=SPOTIPY_CLIENT_ID,
    client_secret=SPOTIPY_CLIENT_SECRET
)

# creating spotify object
spotify = sp.Spotify(client_credentials_manager=Client_Credentials_manager)
