import json
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.basemap import Basemap
import requests


def bilinear_interpolation(lat, lon, coordinates):
    """
    Perform bilinear interpolation to estimate the aurora intensity at a given latitude and longitude.
    
    Parameters:
        lat (float): The latitude for which to estimate the intensity.
        lon (float): The longitude for which to estimate the intensity.
        coordinates (list): A list of [lon, lat, intensity] values.
    
    Returns:
        float: Interpolated aurora intensity.
    """
    # Convert coordinates to a structured numpy array
    data = np.array(coordinates)
    longitudes = np.unique(data[:, 0])
    latitudes = np.unique(data[:, 1])
    
    # Create a grid for intensity
    intensity_grid = data[:, 2].reshape(len(longitudes), len(latitudes))
    
    # Ensure the input lat/lon is within the bounds
    if lat < latitudes.min() or lat > latitudes.max() or lon < longitudes.min() or lon > longitudes.max():
        raise ValueError("Latitude or longitude is out of bounds.")
    
    # Find the four nearest grid points
    lon_idx = np.searchsorted(longitudes, lon) - 1
    lat_idx = np.searchsorted(latitudes, lat) - 1
    
    # Get the bounding coordinates
    lon1, lon2 = longitudes[lon_idx], longitudes[lon_idx + 1]
    lat1, lat2 = latitudes[lat_idx], latitudes[lat_idx + 1]
    
    # Get the intensities at the four corners
    q11 = intensity_grid[lon_idx, lat_idx]
    q21 = intensity_grid[lon_idx + 1, lat_idx]
    q12 = intensity_grid[lon_idx, lat_idx + 1]
    q22 = intensity_grid[lon_idx + 1, lat_idx + 1]
    
    # Perform bilinear interpolation
    f_lat1 = ((lon2 - lon) / (lon2 - lon1)) * q11 + ((lon - lon1) / (lon2 - lon1)) * q21
    f_lat2 = ((lon2 - lon) / (lon2 - lon1)) * q12 + ((lon - lon1) / (lon2 - lon1)) * q22
    f = ((lat2 - lat) / (lat2 - lat1)) * f_lat1 + ((lat - lat1) / (lat2 - lat1)) * f_lat2
    
    return f


# Load the JSON data
#file_path = 'ovation_aurora_latest.json'  # Replace with the correct file path
#with open(file_path, 'r') as f:
#    data = json.load(f)
url = 'https://services.swpc.noaa.gov/json/ovation_aurora_latest.json'
response = requests.get(url)
response.raise_for_status()  # 检查请求是否成功
data = json.loads(response.text)
#data = response.text

# Extract coordinates data
coordinates = data['coordinates']

# Convert coordinates to a numpy array for processing
coordinates_array = np.array(coordinates)

# Extract longitude, latitude, and aurora intensity
longitude = coordinates_array[:, 0]
latitude = coordinates_array[:, 1]
aurora_intensity = coordinates_array[:, 2]

# Plot the aurora intensity overlaid on a world map
plt.figure(figsize=(14, 8))

# Set up the Basemap for a global projection
m = Basemap(projection='cyl', llcrnrlat=-90, urcrnrlat=90, llcrnrlon=-180, urcrnrlon=180, resolution='c')

# Draw coastlines and map boundaries
m.drawcoastlines()
m.drawmapboundary(fill_color='lightblue')
m.fillcontinents(color='lightgrey', lake_color='lightblue')

# Prepare data for the contour plot
unique_longitudes = np.unique(longitude)
unique_latitudes = np.unique(latitude)
lon_grid, lat_grid = np.meshgrid(unique_longitudes, unique_latitudes)
intensity_grid = coordinates_array[:, 2].reshape(len(unique_longitudes), len(unique_latitudes)).T

# Plot the aurora intensity as a heatmap over the map
m.contourf(lon_grid, lat_grid, intensity_grid, cmap='viridis', latlon=True)

# Add a colorbar for intensity values
plt.colorbar(label='Aurora Intensity')
plt.title('Aurora Intensity Distribution Overlaid on a World Map')

# Display the plot
#plt.show()
plt.savefig("output_image_ovation.png")


kiruna_lat = 67.85572
kiruna_lon = 20.22513
proba = bilinear_interpolation(kiruna_lat,kiruna_lon, coordinates_array)

print("Probability of seeing northern light right now is ", proba)