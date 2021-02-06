# -*- coding: utf-8 -*-                                                                                                                                
"""                                                                                                                                                    
Created on Fri Apr 11, 2020                                                                                                                            
@author: Sai Madhuri Yerramsetti                                                                                                                       
"""                                                                                                                                                    
                                                                                                                                                       
import json                                                                                                                                            
import requests as rq                                                                                                                                  
import pymongo                                                                                                                                         
from bson import json_util                                                                                                                             
                                                                                                                                                       
# Public API listener to get the requested data                                                                                                        
def apilistener():                                                                                                                                     
     
# Define required variables                                                                                                                                                   
    api_key = '7425f30224e6d1256ab575f74d637096'                                                                                                       
    fm_type = 'application/json'                                                                                                                       
    url = "https://developers.zomato.com/api/v2.1/search?entity_type=city&start=126&count=20"                                                          
    headers =  {'Accept' : fm_type, 'user-key' : api_key} 

# Get the data from the API url                                                                                             
    response = rq.request("GET", url, headers=headers) 

#Save the response text into a variable                                                                                                
    zomato_entry = json.loads(response.text)                                                                                                           
    print("we are done:" +str(type(zomato_entry))) 

# define Mongo db client, db and collecton name                                                                                                    
    client = pymongo.MongoClient('localhost',27017)                                                                                                    
    db = client["rest_db"]                                                                                                                             
    col = db["rest_collection"]                                                                                                                        
     
#Get the data from various required tags of JSON                                                                                                                                                   
    for col_data in zomato_entry['restaurants']:                                                                                                       
        restaurant_id = col_data['restaurant']['id']                                                                                                   
        name = col_data['restaurant']['name']                                                                                                          
        country_id = col_data['restaurant']['location']['country_id']                                                                                  
        city = col_data['restaurant']['location']['city']                                                                                              
        address = col_data['restaurant']['location']['address']                                                                                        
        locality = col_data['restaurant']['location']['locality']                                                                                      
        locality_verbose = col_data['restaurant']['location']['locality_verbose']                                                                      
        latitude = col_data['restaurant']['location']['latitude']                                                                                      
        longitude = col_data['restaurant']['location']['longitude']                                                                                    
        cuisines = col_data['restaurant']['cuisines']                                                                                                  
        avg_cost_for_two = col_data['restaurant']['average_cost_for_two']                                                                              
        price_range = col_data['restaurant']['price_range']                                                                                            
        currency = col_data['restaurant']['currency']                                                                                                  
        has_online_delivery = col_data['restaurant']['has_online_delivery']                                                                            
        is_delivering_now = col_data['restaurant']['is_delivering_now']                                                                                
        include_bogo_offers = col_data['restaurant']['include_bogo_offers']                                                                            
        has_table_booking = col_data['restaurant']['has_table_booking']                                                                                
        aggregate_rating = col_data['restaurant']['user_rating']['aggregate_rating']                                                                   
        rating_color = col_data['restaurant']['user_rating']['rating_color']                                                                           
        rating_text = col_data['restaurant']['user_rating']['rating_text']                                                                             
        votes = col_data['restaurant']['user_rating']['votes']                                                                                         
        all_reviews_count = col_data['restaurant']['all_reviews_count'] 

# Save all the data collected in a dict object                                                                               
        mydict = { "restaurant_id": restaurant_id, "name": name,                                                                                       
                  "country_id": country_id, "city": city, "address": address,                                                                          
                  "locality": locality, "locality_verbose": locality_verbose,                                                                          
                  "latitude": latitude, "longitude": longitude,                                                                                        
                  "cuisines": cuisines, "avg_cost_for_two": avg_cost_for_two,                                                                          
                  "price_range": price_range, "currency": currency,                                                                                    
                  "has_online_delivery": has_online_delivery,                                                                                          
                  "is_delivering_now": is_delivering_now,                                                                                              
                  "include_bogo_offers": include_bogo_offers,                                                                                          
                  "has_table_booking": has_table_booking,                                                                                              
                  "aggregate_rating": aggregate_rating,                                                                                                
                  "rating_color": rating_color, "rating_text": rating_text,                                                                            
                  "votes": votes, "all_reviews_count": all_reviews_count}   

# Insert the dict into collection of mongo db                                                                           
        col.insert_one(mydict)                                                                                                                         
        print("Inserted One entry with id:")
        print(mydict['restaurant_id'])                                                                                                                 
 
# print one document stored in db, count of documens with a particular id and distinct ids                                                                                                                                                      
    print(db.rest_collection.find_one())                                                                                                               
    print(db.rest_collection.count_documents({"restaurant_id" : "18936095"}))                                                                          
    print(db.rest_collection.distinct("restaurant_id"))                                                                                                
                                                                                                                                                       
if __name__ == "__main__":                                                                                                                             
    apilistener() 
#
