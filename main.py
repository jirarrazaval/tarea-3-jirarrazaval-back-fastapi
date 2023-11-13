from fastapi import FastAPI, HTTPException
from google.cloud import storage
from google.cloud.exceptions import NotFound
import xml.etree.ElementTree as ET
import json
import yaml
import csv

app = FastAPI()

# Me conecto al bucket de Google Cloud Storage
bucket_name = '2023-2-tarea3'
client = storage.Client.from_service_account_json('tarea3-service-key.json')
bucket = client.bucket(bucket_name)

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/files/")
async def list_files():
    try:
        files = bucket.list_blobs()
        file_list = [{'name': file.name, 'url': f'/{bucket_name}/{file.name}'} for file in files]
        return {"files": file_list}

    except NotFound:
        raise HTTPException(status_code=404, detail="Bucket not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 1. Aircrafts.xml
@app.get("/files/aircrafts")
async def get_aircrafts():
    try:
        blob = bucket.get_blob('aircrafts.xml')
        aircrafts_data = blob.download_as_string().decode('utf-8')

        # Parse XML
        root = ET.fromstring(aircrafts_data)
        aircraft_list = []
        for aircraft in root:
            aircraft_dict = {}
            for attribute in aircraft:
                aircraft_dict[attribute.tag] = attribute.text
            aircraft_list.append(aircraft_dict)

        return {"aircrafts": aircraft_list}

    except NotFound:
        raise HTTPException(status_code=404, detail="File not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
# 2. Airports.csv
@app.get("/files/airports")
async def get_airports():
    try:
        blob = bucket.get_blob('airports.csv')
        airports_data = blob.download_as_string().decode('utf-8').split('\n')

        # Extract headers and airport data
        headers = airports_data[0].split(',')
        airport_data = [line.split(',') for line in airports_data[1:] if line]

        # Organize data by attributes
        airport_list = []
        for entry in airport_data:
            airport_dict = {headers[i]: entry[i] for i in range(len(headers))}
            airport_list.append(airport_dict)

        return {"airports": airport_list}

    except NotFound:
        raise HTTPException(status_code=404, detail="File not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 3. Flight Detail
@app.get("/files/flights/{year}/{month}/")
async def get_flight_data(year: int, month: int):
    try:
        # Generate the path to the flight data file
        file_path = f'flights/{year:04d}/{month:02d}/flight_data.json'
        blob = bucket.get_blob(file_path)

        # Check if the file exists
        if blob is None:
            raise HTTPException(status_code=404, detail="Flight data not found")

        # Download and parse JSON data
        file_content = blob.download_as_text()

        # Check if the file is empty
        if not file_content:
            raise HTTPException(status_code=404, detail="Flight data is empty")

        flight_data = json.loads(file_content)

        # Organize the flight data by attributes
        organized_data = []
        for flight in flight_data:
            organized_flight = {
                "flightNumber": flight["flightNumber"],
                "originIATA": flight["originIATA"],
                "destinationIATA": flight["destinationIATA"],
                "airline": flight["airline"],
                "aircraftID": flight["aircraftID"],
            }
            organized_data.append(organized_flight)

        return {"flights_data": organized_data}

    except NotFound:
        raise HTTPException(status_code=404, detail="Flight data not found")

    except HTTPException as e:
        # Re-raise HTTPException to pass through custom exceptions
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 4. Passegners.yaml
@app.get("/files/passengers/")
async def get_passengers():
    try:
        # Specify the path to the passengers.yaml file
        file_path = 'passengers.yaml'
        blob = bucket.get_blob(file_path)

        # Check if the file exists
        if blob is None:
            raise HTTPException(status_code=404, detail="Passenger data not found")

        # Download and parse YAML data
        file_content = blob.download_as_text()

        # Check if the file is empty
        if not file_content:
            raise HTTPException(status_code=404, detail="Passenger data is empty")

        passengers_data = yaml.safe_load(file_content)

        return {"passengers": passengers_data}

    except NotFound:
        raise HTTPException(status_code=404, detail="Passenger data not found")

    except HTTPException as e:
        # Re-raise HTTPException to pass through custom exceptions
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# 5. Tickets.csv
@app.get("/files/tickets/")
async def get_tickets():
    try:
        # Specify the path to the tickets.csv file
        file_path = 'tickets.csv'
        blob = bucket.get_blob(file_path)

        # Check if the file exists
        if blob is None:
            raise HTTPException(status_code=404, detail="Ticket data not found")

        # Download and parse CSV data
        file_content = blob.download_as_text()

        # Check if the file is empty
        if not file_content:
            raise HTTPException(status_code=404, detail="Ticket data is empty")

        tickets_data = []
        csv_reader = csv.reader(file_content.splitlines())
        header = next(csv_reader)  # Assuming the first row is the header

        for row in csv_reader:
            ticket_dict = dict(zip(header, row))
            tickets_data.append(ticket_dict)

        return {"tickets": tickets_data}

    except NotFound:
        raise HTTPException(status_code=404, detail="Ticket data not found")

    except HTTPException as e:
        # Re-raise HTTPException to pass through custom exceptions
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))