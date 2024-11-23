import requests as Requests
from bs4 import BeautifulSoup
import time, random, fake_useragent
global headers
global horse_object_list

horse_object_list = []

url = "https://www.bloodhorse.com/stallion-register/Results/ResultsTable?ReferenceNumber=0&Page=1&IncludePrivateFees=False"
url_base = "https://www.bloodhorse.com"

session = Requests.Session()

class HorseData:
    def __init__(self, name, crops=None, foals=None, racing_age_foals=None, b_type_winners=None,
                 starters=None, winners=None, starts=None, wins=None, earnings=None, salesdata=None) -> None:
        self.name = name
        self.crops = crops
        self.foals = foals
        self.racing_age_foals = racing_age_foals
        self.b_type_winners = b_type_winners
        self.starters = starters
        self.winners = winners
        self.starts = starts
        self.wins = wins
        self.earnings = earnings
        self.salesdata = salesdata

class SalesData:
    def __init__(self, num_weanlings=None, sales_weanlings=None, num_yearlings=None, sales_yearlings=None) -> None:
        self.num_weanlings = num_weanlings
        self.sales_weanlings = sales_weanlings
        self.num_yearlings = num_yearlings
        self.sales_yearlings = sales_yearlings

horse_data_sections = HorseData("name","crops","foals","racing_age_foals",
                                "black_type_winners","starters","winners",
                                "starts","wins","earnings",
                                SalesData("num_weanlings","sales_weanlings",
                                          "num_yearlings","sales_yearlings"))

# Along with a request, the browser sends a list of "headers"
# The headers contain some information about the system
# we can lie about the system we're using to make it seem like "not a bot"
headers_list = [
    # Firefox 77 Windows
    {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.bloodhorse.com/stallion-register/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
        },
    # Chrome 83 Mac
    {
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Dest": "document",
        "Referer": "https://www.bloodhorse.com/stallion-register/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"
        },
    # Firefox 77 Mac
    {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.bloodhorse.com/stallion-register/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
        },
    # Chrome 83 Windows
    {
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Referer": "https://www.bloodhorse.com/stallion-register/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9"
        },
    # Firefox 130 Linux
    {
        "Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
        "Accept-Language" : "en-US,en;q=0.5",
        "Referer": "https://www.bloodhorse.com/stallion-register/",
        "Sec-Fetch-Dest" : "document",
        "Sec-Fetch-Mode" : "navigate",
        "Sec-Fetch-Site" : "cross-site",
        "Upgrade-Insecure-Requests" : "1",
        "user-agent" : "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0"
    },
    # Firefox 126 Mac
    {
        "Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
        "Accept-Language" : "en-US,en;q=0.5",
        "Referer": "https://www.bloodhorse.com/stallion-register/",
        "Sec-Fetch-Dest" : "document",
        "Sec-Fetch-Mode" : "navigate",
        "Sec-Fetch-Site" : "cross-site",
        "Upgrade-Insecure-Requests" : "1",
        "user-agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:126.0) Gecko/20100101 Firefox/126.0"
    }
]


def Header_Select(failures):
    ua = fake_useragent.UserAgent(platforms="pc")
    
    #select_number = failures

    #if select_number > len(headers_list) -1:
    headers_list[0].update({"user-agent": ua.random})
    #print(headers_list[0])
    return headers_list[0]

    #return headers_list[select_number]

headers = Header_Select(10)


def Bloodhorse_Get():
    global headers
    
    bloodhorse_failures = 0
    link_list = []
    
    connecting_url = url
    all_links_added = False
    pagenum = 1
    while bloodhorse_failures < len(headers_list) and not all_links_added :
        
        print("Connecting to Bloodhorse; attempt #"+str(bloodhorse_failures +1) + "\nshould be page #"+str(pagenum))

        time.sleep(60)
        # This is a Response object with the information returned from our request
        bloodhorse_response = session.post(connecting_url, headers=headers)

        bloodhorse_text = bloodhorse_response.text

        # "Beautiful Soup" is a Python library that can format and search HTML like what's found in our response
        bloodhorse_data = BeautifulSoup(bloodhorse_text, "html.parser")


        # link_element = bloodhorse_data.find("a", recursive=True, attrs={"class":"equineline"})
        # searchresult = bloodhorse_data.find("div", recursive=True, attrs={"class":"resultHead"})
        # print(searchresult)
        
        horse_link_divs = bloodhorse_data.find_all("a", recursive=True)
        
        next_page = bloodhorse_data.find("a", attrs={"class":"next"}, recursive=True)


        temp_url_copy = connecting_url
        
                
        try:
            connecting_url = url_base + str(next_page).split('href="')[1].split('">')[0].replace("amp;", "")
            pagenum +=1
            print(connecting_url)
        except IndexError:
            print("Error getting next page; trying with next headers")
            bloodhorse_failures += 1
            headers = Header_Select(bloodhorse_failures)
        
        is_last_page = bloodhorse_data.find("a", attrs={"class":"next disabled"})
        if is_last_page != None:
            all_links_added = True
        
        if connecting_url != temp_url_copy:
            for element in horse_link_divs:
                if "/stallion-register/stallions/" in str(element) and "class" not in str(element):
                    link = url_base + str(element).split('href="')[1].split('">')[0]
                    if link not in link_list:
                        link_list.append(link)
                
    return link_list
    

def Equineline_Get(link_list):
    # Format the link to be something we can use
    # (might not need to remove "amp;" as it's the character reference for "&" and browsers usually understand it)
    equineline_link = equineline_href.replace("amp;","")
    equineline_link = equineline_link.strip("href=").strip('"')


    equineline_link = equineline_link.split("ASCID=")
    equineline_link = equineline_link[0] + "MareRef=0&hem=N"

    equineline_link = equineline_link.split("bh.cfm")
    equineline_link = equineline_link[0] + "bh_main.cfm" + equineline_link[1]

    print("Connecting to Equineline")

    # Response from Equineline (where the stats are held)
    equineline_response = session.get(equineline_link, headers=headers)

    equineline_text = equineline_response.text
    equineline_data = BeautifulSoup(equineline_text, "html.parser")


    pre_elements = equineline_data.find_all("pre", recursive=True)

    # file = open("output.txt", "w")
    # file.write(str(pre_elements[0]) + "\n\n" + str(pre_elements[1]))
    return str(pre_elements)


def Data_Get():
    global horse_object_list
    
    for name in horse_name_list:
        new_horse = HorseData(name=name, salesdata=SalesData())
        equineline_href = Bloodhorse_Get(new_horse.name)


        data = Equineline_Get(equineline_href)

        data_parsing_list = data.splitlines()

        for i in range(len(data_parsing_list)):
            data_parsing_list[i] = list(filter(None, data_parsing_list[i].strip().split(" ")))

        data_parsing_list = list(filter(None, data_parsing_list))

        weanling_data_collected = False
        yearling_data_collected = False

        for item in data_parsing_list:
            if len(item) == 2:
                if 'crops' in item:
                    new_horse.crops = int(item[0].replace(",", ""))
                    print("crops: ", new_horse.crops)
                elif 'foals' in item:
                    new_horse.foals = int(item[0].replace(",", ""))
                    print("foals: ", new_horse.foals)
                    
            if len(item) == 5:
                if 'foals' in item:
                    new_horse.racing_age_foals = int(item[0].replace(",", ""))
                    print("racing age foals: ", new_horse.racing_age_foals)
                elif 'Earnings' in item:
                    new_horse.earnings = int(item[1].replace(",", "").replace("$",''))
                    print("earnings: ", new_horse.earnings)
                elif 'Starts' in item:
                    new_horse.starts = int(item[1].replace(",", ""))
                    print("starts: ", new_horse.starts)
                    
            if '(/foals' in item:
                if 'Winners' in item:
                    new_horse.winners = int(item[4].split("(")[0].replace(",", ""))
                    print("winners: ", new_horse.winners)
                elif ('Blacktype' and 'Winners') in data_parsing_list[data_parsing_list.index(item) -1]:
                    new_horse.b_type_winners = int(item[3].split("(")[0].replace(",", ""))
                    print("btype winners: ", new_horse.b_type_winners)
                    
            if 'Starters(/foals' in item:
                new_horse.starters = int(item[3].split("(")[0].replace(",", ""))
                print("starters: ", new_horse.starters)
                
            if 'Wins' in item:
                new_horse.wins = int(item[2].split("(")[0].replace(",", ""))
                print("wins: ", new_horse.wins)
                
            if 'Weanlings' in item and weanling_data_collected == False:
                weanling_data_collected = True
                new_horse.salesdata.num_weanlings = item[1]
                new_horse.salesdata.sales_weanlings = item[2].replace(",", "").replace("$",'')
                print("# weanlings: ", new_horse.salesdata.num_weanlings,'\nsales weanlings: ', new_horse.salesdata.sales_weanlings)
                
            if 'Yearlings' in item and yearling_data_collected == False:
                yearling_data_collected = True
                new_horse.salesdata.num_yearlings = item[1]
                new_horse.salesdata.sales_yearlings = item[2].replace(",", "").replace("$",'')
                print("# yearlings: ", new_horse.salesdata.num_yearlings,'\nsales yearlings: ', new_horse.salesdata.sales_yearlings)
                
        horse_object_list.append(new_horse)
        time.sleep(3)


print(Bloodhorse_Get())

exit()

for horse in horse_object_list:
    print(horse.name)