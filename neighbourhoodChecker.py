import pandas as pd
import requests as rq
import sys
from io import StringIO
from time import sleep
from prettytable import PrettyTable
from pandas.io.json import json_normalize

danmarks_statistik_URL = "https://api.statbank.dk/v1/data"

def fetch_data_education(addresse):
    """
    Gets educational data for the area around addresse
    :param addresse: the address for which to fetch data from
    :return:
    """
    data_to_post = {
        "table": "HFUDD11",                             #Højeste fuldførte uddanelse
        "format": "CSV",
        "valuePresentation": "CodeAndValue",
        "variables": [
            {
                "code": "BOPOMR",
                "values": [
                    "*"

                ]
            },
            {
                "code": "HFUDD",
                "values": [
                    "H10",      #Grundskole
                    "H20",      #Gymnasie
                    "H30",      #Erhvervsuddannelse
                    "H35",      #Adgangsgivende forløb
                    "H40",      #Kort videregående
                    "H50",      #Mellemlang videregående
                    "H60",      #Bachelor uddannelse
                    "H70",      #Lang videregående
                    "H80",      #PHD + forsker

                ]
            },
            {
                "code": "ALDER",
                "values": [
                    "TOT"                       # Gets all ages
                ]
            },
            {
                "code": "KØN",
                "values": [
                    "TOT"                       # Gets all genders
                ]
            },
            {
                "code": "TID",
                "values": [
                    "2020"
                ]
            }
        ]                                       
    }
    # Request call #
    r = rq.post(url=danmarks_statistik_URL, json=data_to_post)
    if r.status_code != 200:
        print(r.status_code)
        sys.exit(-1)
    
    # Formatting #
    csv_file = r.text
    pandas_object = pd.read_csv(StringIO(csv_file), delimiter=';', header=0)
    pandas_object['BOPOMR'] = pandas_object['BOPOMR'].str.replace(r'[0-9]{3} ',"", regex=True)  # Regex to drop the first 3 numbers of area code
    all_educations_from_area = pandas_object.loc[pandas_object['BOPOMR'] == addresse].values    # Collects all the educations from the given address

    # Creating a dictionary with all the data #
    education_and_amount = dict()
    for entry in all_educations_from_area:
        education = entry[1]
        amount_educated = entry[5]
        education_and_amount[education]=amount_educated
    return education_and_amount

def fetch_data_net(addresseID):
    """
    Get information about the internet speed and type for the given address
    :param addresseID: The unique ID of this address, as found by DAWA.
    :return: A dict where each tech is mapped to a tuple of its maximum download and upload speed
    """
    # Request call #
    data_to_post = "https://tjekditnet.dk/pls/wopdprod/tdn_feed?uid=736912&adgadrid={}&format=json".format(addresseID)
    r = rq.get(data_to_post)
    if(r.status_code != 200):
        print("Not successful")

    # Formatting #
    json = r.json()
    data = json_normalize(json)
    data = data['daekninger']

    # Creating a dictionary with each available technology mapped to upload and download speed #
    tech_and_speed = dict()
    for i in range(3):
        try:
            data_list = data.values.flatten()[0][i]['daekning'][0]
            available_tech = data_list['teknologi']
            max_speed_download = data_list['download_udt_privat_mbits']
            max_speed_upload = data_list['upload_udt_privat_mbits']
            tech_and_speed[available_tech] = (max_speed_download, max_speed_upload)
        except:
            pass
    return(tech_and_speed)

def fetch_data_DAWA(address):
    """
    Retrieves information about a Danish address
    :param String address which has been input from user
    :return: City name, addressID
    """
    # Request call #
    address, housenumber, postalnumber, city = address[0], address[1], address[2], address[3]
    data_to_post = "https://api.dataforsyningen.dk/datavask/adresser?" + \
                   "betegnelse={}, {}, {} {}".format(address, housenumber, postalnumber, city)
    r = rq.get(data_to_post)
    if (r.status_code != 200):
        print("Not successful")

    # Formatting #
    json_response = json_normalize(r.json())                             #Get json response to "find address"
    data = json_response['resultater']                                      #Get the "Results" column of the response
    first_result = data.values[0]                                           #Get most probable address
    dataframe_result = json_normalize(first_result)                      #create the dataframe of the json contained withing the first result, get href to actual address data
    actual_address_page = rq.get(dataframe_result['adresse.href'][0])       #Get request to the url containing data we actually want
    data = json_normalize(actual_address_page.json())                    #Create dataframe from the response with actual data about the address
    return(data['adgangsadresse.kommune.navn'].iloc[0], data['adgangsadresse.id'].iloc[0])



def fetch_data_straf(kommune="none"):
    """
    :param kommune: The city where the address is located
    :return: An assortment of crime data
    """

    dataToPost = {
        "table": "STRAF11",
        "format": "CSV",
        "valuePresentation": "CodeAndValue",
        "variables": [
            {
                "code": "OMRÅDE",
                "values": [
                    "*"                     # Gets all areas

                ]
            },
            {
                "code": "OVERTRÆD",
                "values": [
                    "*"                     # Gets all crimes
                ]
            },
            {
                "code": "Tid",
                "values": [
                    "2021K3"
                ]
            }
        ]
    }
    # Request call #
    r = rq.post(url = danmarks_statistik_URL, json=dataToPost)
    if r.status_code != 200:
        print(r.status_code)
        sys.exit(-1)

    # Formatting #
    csv_file = r.text
    pandas_object = pd.read_csv(StringIO(csv_file), delimiter=';', header=0)
    pandas_object.columns = ['Area', 'Crime', 'Year', 'Number']
    pandas_object = pandas_object[pandas_object['Crime'].isin([
                                                            "1110 Blodskam mv.",
                                                            "11 Seksualforbrydelser i alt",
                                                            "12 Voldsforbrydelser i alt",
                                                            "1312 Brandstiftelse",
                                                            "1320 Indbrud i beboelser",
                                                            "1328 Tyveri fra bil, båd mv.",
                                                            "1339 Tyveri/brugstyveri af køretøj",
                                                            "1380 Røveri",
                                                            "1390 Hærværk",
                                                            "1435 Salg af narkotika mv.",
                                                            "1440 Smugling mv. af narkotika",
                                                            "1460 Uagtsomt manddrab mv. i forbindelse med færdselsuheld",
                                                            "1485 Freds- og ærekrænkelser"
                                                            ])] # Instead of having all crimes in our data, we only chose relevant data
    pandas_object['Area'] = pandas_object['Area'].str.replace(r'[0-9]{3} ', "",regex=True) #Regex to drop the first 3 chars of area code
    return (pandas_object.loc[pandas_object['Area'].isin([kommune, "Hele landet"])])

def score_data_net(data: dict) -> int:
    """
    Calculates a score from a dictionary of internet speeds
    :param data: dictionary of internet speeds
    :return: int representing a score from 0-10
    """

    score, maxD, maxU = 10, 0, 0 # initial values
    if data.get('Fiber') == ('0', '0'):
        # You don't have fibernet, pleb. You get a -2.
        score -= 2

    # Get the maximum upload and download
    for key in data.keys():
        download = int(data.get(key)[0])
        if download > maxD:
            maxD = download
        upload = int(data.get(key)[1])
        if upload > maxU:
            maxU = upload

    # Checks maximum download and upload and deducts scores #
    if maxD < 1000:
        score -= 1
    if maxD < 100:
        score -= 2
    if maxU < 100:
        score -= 1
    if maxU < 25:
        score -= 2
    return score

def score_data_straf(data: pd.DataFrame) -> float:
    """
    Returns a 0-10 score according to the crime data
    :param data: A Pandas Dataframe containing crime data for a
    :return: A float representing a score from 0-10, where 5 is the country average
    """

    alt_krimi = data[data["Area"] == "Hele landet"]
    kommunenavn = data[data["Area"] != "Hele landet"]["Area"].values[0]


    data_to_post ={ # Gets number of citizens in every city
   "table": "BY1",
   "format": "CSV",
   "variables": [
      {
         "code": "BYER",
         "values": [
            "10100101",
            "14700147",
            "15100151",
            "16100161",
            "17300173",
            "18300183",
            "19000190",
            "20100201",
            "21000210",
            "22300223",
            "23000230",
            "24000240",
            "25010012",
            "26000260",
            "27000270",
            "30600306",
            "31600316",
            "32000320",
            "33000330",
            "34000340",
            "35000350",
            "36000360",
            "37000370",
            "39000390",
            "40000400",
            "41000410",
            "42000420",
            "43000430",
            "44000440",
            "45000450",
            "46100461",
            "47900479",
            "48000480",
            "49200492",
            "51000510",
            "53000530",
            "54000540",
            "55000550",
            "56100561",
            "57300573",
            "58000580",
            "60700607",
            "61500615",
            "62100621",
            "63000630",
            "65700657",
            "66100661",
            "67100671",
            "70600706",
            "71000710",
            "72700727",
            "73000730",
            "74000740",
            "75100751",
            "76000760",
            "77300773",
            "78700787",
            "79100791",
            "81000810",
            "82000820",
            "84000840",
            "85100851",
            "86000860",
            "74100741"
             #Maybe more? Dunno, might have missed one or two.
         ]
   }
   ]
    }

    # Request call #
    r = rq.post(url=danmarks_statistik_URL, json=data_to_post)
    if r.status_code != 200:
        print(r.status_code)
        sys.exit(-1)

    # Formatting #
    csv = r.text.split('\n')
    found_string = ""
    for string in csv:
        if kommunenavn in string:
            found_string = string
    found_string = found_string.split(';')
    befolkningstal = int(found_string[2]) #Get the integer of the string
    omraade_krimi = data[data["Area"] != "Hele landet"]

    # Calculating average for the area and country #
    omraade_gns = (omraade_krimi["Number"].sum()/befolkningstal)*100000 #100000 as to get crime pr. 100000 inhabitants
    lands_gns = (alt_krimi["Number"].sum()/5831000)*100000

    return (10 - (min(omraade_gns**2/lands_gns**2*5, 10)))

def education_score(educations):
    '''
    Scores data for education level in the area. Higher education affect the score exponentially.
    :param educations: a dict of education levels mapped to how many in the area achieve that education level
    :return: A score from 0-10 depicting how well educated the area is
    '''
    weight, educated_total, score = 1.0, 0, 0
    for key in educations.keys():
        educated_total+= educations.get(key)
        score += educations.get(key)*weight
        weight = weight * 1.6
    return min(score/educated_total-1,10)


def score(net_data, crime_data, education_data):
    '''
    Scores the three data-sets on a scale from 0-10
    :param net_data: Data about the network connections of the address
    :param crime_data: Data about the crime rate of the address
    :param education_data: Data about the education level of the address
    :return: The average score is returned as the final score of the address, and a tuple of individual scores
                is also returned
    '''
    edu_score = round(education_score(education_data), ndigits=2)
    net_score = score_data_net(net_data)
    crime_score = round(score_data_straf(crime_data),ndigits=2)
    return (round((net_score + crime_score + edu_score) / 3, ndigits=2), tuple([net_score, crime_score, edu_score]))

def get_address():
    '''
    Small method to take address as input
    :return: The formatted address for use in other methods
    '''
    address = ""
    while (len(address) != 4):
        address = input("Input a correct address, fx: 'Campusvej, 55, 5230, Odense M'\n").split(", ")
    return address

def check_address(print_data = True):
    '''
    Asks the user for an address, then gets the score for that address. Can be used to print the scores.
    :param print_data: If True, this method will print the data in a table
    :return: The total score of the address, and a tuple of the individual scores
    '''
    address = get_address()
    kommune, addressID = fetch_data_DAWA(address)
    crime_data = fetch_data_straf(kommune)
    net_data = fetch_data_net(addressID)
    education_data = fetch_data_education(kommune)
    final_score, individual_scores = score(net_data, crime_data, education_data)

    if print_data:
        t = PrettyTable([" ", (address[0] + " " + address[1] + " " + address[2] + " " + address[3])])
        t.add_row(["WiFi score", individual_scores[0]])
        t.add_row(["Area Safety", individual_scores[1]])
        t.add_row(["Education score", individual_scores[2]])
        print(t.get_string(title="Total score for {}: {}".format(address[0]+ " " + address[1], final_score)))

    return final_score, individual_scores, address

def compare():
    '''
    Asks for two addresses, gets the score for each, and compares them in a neat little table
    :return: None
    '''
    final_score1, individual_scores1, address_1 = check_address(False)
    final_score2, individual_scores2, address_2 = check_address(False)
    t = PrettyTable([" ", address_1[0] + " " + address_1[1], address_2[0] + " " + address_2[1]])
    t.add_row(["Total score", final_score1, final_score2])
    t.add_row(["WiFi Score", individual_scores1[0], individual_scores2[0]])
    t.add_row(["Safety Score", individual_scores1[1], individual_scores2[1]])
    t.add_row(["Education Score", individual_scores1[2], individual_scores2[2]])
    if (final_score1 > final_score2):
        print(t.get_string(title="Best total score: {}, {}".format(address_1[0] + " " + address_1[1], final_score1)))
    else:
        print(t.get_string(title="Best total score: {}, {}".format(address_2[0] + " " + address_2[1], final_score2)))

def apply_filters(filters: list):
    new_filters = []
    for f in filters:
        new_filters.append(f)
    return new_filters

if __name__ == '__main__':
    user_input = -1
    print("Welcome to the neighbourhood checker!")
    switcher = {
        0: lambda: print("Thank you for using the neighbourhood checker. See you another time!"),
        1: lambda: check_address(),
        2: lambda: print("Feature not yet implemented!"),
        3: lambda: compare()
    }
    while user_input != 0:
        print("----------------------------------")
        print("Please select an option:")
        print("1. Check address \n2. Apply Filters \n3. Compare addresses")
        print("----------------------------------")
        user_input = int(input("1/2/3 or 0 to cancel \n"))
        func = switcher[user_input]   #Get the funciton from switch statement
        func()
    print("System exiting.", end = "")
    sleep(1)
    print(".", end = "")
    sleep(1)
    print(".", end = "")
