import csv
import pymongo
import pprint

def create_fields_dict(row, fields):
    returnObj = {}
    for field in fields:
        returnObj[field] = row[field]
    return returnObj

def main():
    
    client = pymongo.MongoClient()
    db = client['philly-data']

    required_fields = ['id', 'address', 'lat', 'lon', 'tweeted', 'city', 'state']
    extra_fields = ['market_value', 'number_of_bedrooms', 'number_of_bathrooms', 'number_stories', 'sale_date', 'sale_price', 'total_area', 'total_livable_area', 'year_built', 'central_air', 'category_code', 'exterior_condition', 'fuel', 'garage_spaces', 'garage_type', 'interior_condition', 'other_building', 'owner_1', 'owner_2', 'quality_grade', 'type_heater', 'view_type', 'unit', 'zip_code', 'zoning']
    enriched_fields = ['neighborhood']

    with open('philly_props.csv', 'w', newline='') as writecsvfile:
        
        fieldnames = required_fields + extra_fields + enriched_fields
        writer = csv.DictWriter(writecsvfile, fieldnames=fieldnames)
        writer.writeheader()

        with open('./data/opa_properties_public.csv') as readcsvfile:
            reader = csv.DictReader(readcsvfile)
            for row in reader:
                req_fields_dict = {}
                req_fields_dict['id'] = row['parcel_number']
                req_fields_dict['address'] = row['location']
                req_fields_dict['lat'] = row['lat']
                req_fields_dict['lon'] = row['lng']
                req_fields_dict['tweeted'] = 0 
                req_fields_dict['city'] = 'Philadelphia'
                req_fields_dict['state'] = 'PA'
                
                write_dict = req_fields_dict | create_fields_dict(row, extra_fields)

                neighborhood = ''
                try:
                    doc = db.hoods.find_one({ 'geometry': { '$geoIntersects': { '$geometry': { 'type': "Point", 'coordinates': [ float(row['lat']), float(row['lng']) ] } } } })
                    neighborhood = doc['properties']['name']
                except:
                    neighborhood = 'Unknown'
                write_dict['neighborhood'] = neighborhood
                writer.writerow(write_dict)

if __name__ == "__main__":
    main()
