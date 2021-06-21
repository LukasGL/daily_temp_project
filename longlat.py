import csv
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="MDP project")

file1 = open(r'C:\Users\lukas\Google Drive\Tareas\Patos\proyecto\ciudades3.csv', 'r')
cities = []
line=file1.readline()
cities.append("Latitud,Longitud,Ciudad,Pais"+"\n")
while line:
  
  line=file1.readline()
  print(line)
  location = geolocator.geocode(line)
  if location!=None:
    print(location.address)
    #city=[line,location.latitude,location.longitude]
    cities.append(str(location.latitude)+","+ str(location.longitude)+","+line)
    #cities.append(city)



  
def save_csv(array):
    try:
        f = open(r'C:\Users\lukas\Google Drive\Tareas\Patos\proyecto\citieslatlong.txt', "a+")
        #csvwriter = csv.writer(f)
    except:
        print("Something went wrong when opening modified objects log file")
        return -1
    try:
      for a in array:
        f.write(a)
    except:
        print("Something went wrong when writing to modified objects log file")
        return -2
    finally:
        f.close()
    return 0

save_csv(cities)