import requests as Requests
from bs4 import BeautifulSoup
import time, random, threading, sqlite3
from fake_useragent import UserAgent
from requests_ip_rotator import ApiGateway, EXTRA_REGIONS
from queue import Queue
from tkinter import *

global headers

horse_object_list = []

url = "https://www.bloodhorse.com/stallion-register/Results/ResultsTable?ReferenceNumber=0&Page="
url_addition = "&IncludePrivateFees=False"
url_base = "https://www.bloodhorse.com"

global set_horse_links
set_horse_links=False

def Header_Select(failures):
    
    select_number = failures

    headers = {
        'user-agent' :  UserAgent(platforms=['pc']).random,
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Referer": "https://www.bloodhorse.com/stallion-register/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9"
    }

    #print(headers)

    return headers

headers = Header_Select(0)

if set_horse_links:
    with sqlite3.connect("horses.db") as sqlconnection:
        sqlcursor = sqlconnection.cursor()

        sqlcursor.execute(""" DROP TABLE IF EXISTS HORSES; """)

        table = """ CREATE TABLE HORSES (
                    BH_link TEXT NOT NULL,
                    Name TEXT,
                    Crops TEXT,
                    Foals TEXT,
                    RA_Foals TEXT,
                    Winners TEXT,
                    Winners_B TEXT,
                    Wins TEXT,
                    Starters TEXT,
                    Starts TEXT,
                    Earnings TEXT,
                    Num_Weanlings TEXT,
                    Sales_Weanlings TEXT,
                    Num_Yearlings TEXT,
                    Sales_Yearlings TEXT
                    ); """
        sqlcursor.execute(table)

def Bloodhorse_Get(starting_page_num, thread_num, out_q, accessKeyID, accessKeySecret):
    global set_horse_links

    print(starting_page_num)

    link_list = []
    
    if set_horse_links:
        global headers
        
        bloodhorse_failures = 0
        
        
        pagenum=starting_page_num
        
        all_links_added = False

        gateway = ApiGateway("https://www.bloodhorse.com", access_key_id=accessKeyID, access_key_secret=accessKeySecret)
        gateway.start()


        session = Requests.Session()
        session.mount("https://www.bloodhorse.com",gateway)

        connecting_url = url + str(pagenum) + url_addition
        while pagenum <= starting_page_num + 20 and not all_links_added and bloodhorse_failures <= 5:
            
            print(f"THREAD {thread_num} Connecting to Bloodhorse; attempt #"+str(bloodhorse_failures +1) + "\nshould be page #"+str(pagenum+1))

            time.sleep(10)

            bloodhorse_response = session.post(connecting_url, headers=headers)

            bloodhorse_text = bloodhorse_response.text

            bloodhorse_data = BeautifulSoup(bloodhorse_text, "html.parser")
            
            horse_link_divs = bloodhorse_data.find_all("a", recursive=True)
            
            next_page = bloodhorse_data.find("a", attrs={"class":"next"}, recursive=True)


            temp_url_copy = connecting_url
            
                    
            try:
                test_url = url_base + str(next_page).split('href="')[1].split('">')[0].replace("amp;", "")
                pagenum += 1

                connecting_url = url + str(pagenum) + url_addition

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

    if len(link_list) > 0:
        for i in range(len(link_list) -1):
            sqlInsertRowString = f""" INSERT INTO HORSES (BH_link) VALUES({'"'+str(link_list[i])+'"'}); """
            out_q.put(sqlInsertRowString)

        print("sentinel " + str(thread_num))

    out_q.put(sentinel)

def Process_SQL_Commands(in_q, thread_count):
    sqlconnection = sqlite3.connect("horses.db", timeout=10)
    sqlcursor = sqlconnection.cursor()

    sentinel_count = 0

    while True:
         
        if sentinel_count == thread_count:
            sqlcursor.close()
            break

        command = in_q.get()

        if command == "////":
            sentinel_count +=1
        else:
            sqlcursor.execute(command)

        in_q.task_done()

    sqlconnection.commit()



sentinel = "////"

# Soon to be obsolete when thread constructor is made
def Start_Threads(threadcount, accessID, accessKey):
    q = Queue()
    t4 = threading.Thread(target=Process_SQL_Commands, args=(q, 3, ))
    t4.start()

    t1 = threading.Thread(target=Bloodhorse_Get, args=(0, 1, q, ))
    t2 = threading.Thread(target=Bloodhorse_Get, args=(21, 2, q, ))
    t3 = threading.Thread(target=Bloodhorse_Get, args=(43, 3, q, ))

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()

    q.join()

# TKinter App
class App(Tk):

    def __init__(self):
        Tk.__init__(self)
        
        # Window Title
        self.title("Bloodhorse and Equineline Scraper")
        
        # Window Size
        self.geometry('700x250')

            
        self.AWS_SECRET = ''
        self.AWS_ID = ''
        self.pages = 0

        # First label, also becomes "running" when running
        self.lbl = Label(self, text="Update Bloodhorse Links?")
        self.lbl.grid(row=0, column=0)

        # User chosen thread count display
        self.thcount = IntVar(value=1)
        self.lbl_thcount = Label(self, text='Thread Count: ' + str(self.thcount.get()))
        self.lbl_thcount.grid(column=0,row=1)

        # Checkbox for whether to update links
        self.linkupdatevar = BooleanVar()
        self.linkupdate = Checkbutton(self, variable=self.linkupdatevar,onvalue=True, offvalue=False)
        self.linkupdate.grid(column=1,row=0)

        # Start Button
        self.btn = Button(self, text = 'Start',
            fg= 'red', command=self.start_clicked)
        self.btn.grid(column=5,row=6)
    
        # Thread Count Selection Menu
        self.thcountMenu = Menubutton(self, text="-> Select Thread Count")

        self.menu = Menu(self.thcountMenu, tearoff=0)
        for i in range(1, 9):
            self.menu.add_radiobutton(label=i,variable=self.thcount, command=self.Select_Thread_Num)

        self.thcountMenu["menu"] = self.menu

        self.thcountMenu.grid(column=1, row=1)

        # AWS access information entry
        self.accessid_entry = Entry(self)
        self.accessid_entry.grid(column=0, row=3)
        self.accessid_lbl = Label(text='AWS Access Key ID')
        self.accessid_lbl.grid(column=1,row=3)

        self.accessSecret_entry = Entry(self)
        self.accessSecret_entry.grid(column=0, row=4)
        self.accessSecret_lbl = Label(text='AWS Access Key Secret')
        self.accessSecret_lbl.grid(column=1,row=4)

        self.set_aws_id = Button(self, text="Set ID", fg='green', command = self.setAWSID)
        self.set_aws_id.grid(row=3, column=3)

        self.set_aws_secret = Button(self, text="Set Secret", fg='green', command = self.setAWSSECRET)
        self.set_aws_secret.grid(row=4, column=3)

        # Page number detection
        self.pages=0
        self.pageCount = Button(self, text = 'Detect Pages',
                    fg= 'green', command=self.pageCount_clicked)
        self.pageCount.grid(column=5, row=3)

        self.page_entry = Entry(self)
        self.page_entry.grid(row=1, column=5)

        self.page_entry_button = Button(self, text= 'Enter Pages',
                                        fg='green', command = self.enterPages)
        self.page_entry_button.grid(row=2, column=5)

        # Page number display
        self.pagesNum = Label(text='Detect or enter pages.\n(If setting links)')
        self.pagesNum.grid(row=0, column=5)

        # Warning
        self.warning = Label(text='Make sure all your information is correct.')
        self.warning.grid(row=5, column=5)

        # Reset Button
        self.reset_button = Button(self, text = "Reset", fg='green', command=self.ResetClicked)
        self.reset_button.grid(row=6, column=0)

        self.aws_id_label = Label(self, text = '')
        self.aws_secret_label = Label(self, text = '')
        self.unset_values = Label(self, text='')

    
    def reset_widgets(self):
    
        self.AWS_SECRET = ''
        self.AWS_ID = ''
        self.pages = 0

        self.lbl.configure(text="Update Bloodhorse Links?")
        
        self.lbl_thcount.configure(text='Thread Count: ' + str(self.thcount.get()))

        self.linkupdate.grid(column=1,row=0)

        self.btn.grid(column=5,row=6)
        self.thcountMenu.grid(column=1, row=1)

 
        self.accessid_entry.grid(column=0, row=3)

        self.accessid_lbl.grid(column=1,row=3)

        self.accessSecret_entry.grid(column=0, row=4)
 
        self.accessSecret_lbl.grid(column=1,row=4)

        self.set_aws_id.grid(row=3, column=3)

        self.set_aws_secret.grid(row=4, column=3)

        self.pageCount.grid(column=5, row=3)

        self.page_entry.grid(row=1, column=5)

        self.page_entry_button.grid(row=2, column=5)

        self.pagesNum.configure(text='Detect or enter pages.')

        self.warning.grid(row=5, column=5)

        self.reset_button.grid(row=6, column=0)

        self.aws_id_label.configure(text = '')
        self.aws_secret_label.configure(text = '')
        self.unset_values.configure(text='')

    # Run Program
    def start_clicked(self):
        global set_horse_links
        self.lbl.configure(text='Running')
        self.btn.grid_forget()
        self.linkupdate.grid_forget()
        
        self.page_entry_button.grid_forget()
        self.pageCount.grid_forget()
        self.page_entry.grid_forget()
        self.thcountMenu.grid_forget()

        
        self.warning.grid_forget()
        set_horse_links = self.linkupdatevar.get()

        self.setAWSID()
        self.setAWSSECRET()

        if self.AWS_ID == '' or self.AWS_SECRET == '' or (set_horse_links and self.pages == 0):
            self.unset_values.configure(text='Missing required values (Check page count and AWS)\nClick Reset to continue')
            self.unset_values.grid(row=8, column = 1)
        else:
            self.reset_button.grid_forget()

            print("Setting links: " + str(set_horse_links))
            print("AWS ID: " + self.AWS_ID, "AWS Secret: " + self.AWS_SECRET)
            print("Starting " + str(self.thcount.get()) + " Thread(s)")
            print("If setting links, checking " + str(self.pages) + " pages.")
            




    # Update Thread Number
    def Select_Thread_Num(self):
        self.lbl_thcount.configure(text='Thread Count: ' + str(self.thcount.get()))

    # Auto-Detect Page Number
    def pageCount_clicked(self, *args):
        try:
            bh_response = Requests.post(url=url + '1' + url_addition, headers=Header_Select(0))
            bh_text = bh_response.text
            bh_data = BeautifulSoup(bh_text, "html.parser")   
            last_page = bh_data.find("a", attrs={"class":"last"}, recursive=True)
            self.pages=int(str(last_page).split('Page=')[1].split('&')[0])
            self.pagesNum.configure(text='Pages: '+ str(self.pages))
        except IndexError:
            self.pagesNum.configure(text='Error getting page count.\nTry manually setting a number\nor trying again.')
    
    # Update Page Number Manually
    def enterPages(self):
        try:
            self.pages = int(self.page_entry.get())
            self.pagesNum.configure(text='Pages: '+ str(self.pages))
        except ValueError:
            self.pagesNum.configure(text='Error NaN')
    
    def setAWSID(self):
        self.AWS_ID = self.accessid_entry.get()
        self.accessid_entry.grid_forget()
        self.aws_id_label.configure(text=self.AWS_ID)
        self.aws_id_label.grid(column=0, row=3)
        self.set_aws_id.grid_forget()

    def setAWSSECRET(self):
        self.AWS_SECRET = self.accessSecret_entry.get()
        self.accessSecret_entry.grid_forget()
        self.aws_secret_label.configure(text=self.AWS_SECRET)
        self.aws_secret_label.grid(column=0, row=4)
        self.set_aws_secret.grid_forget()

    def ResetClicked(self):
        self.aws_secret_label.grid_forget()
        self.aws_id_label.grid_forget()
        self.unset_values.grid_forget()
        
        
        self.reset_widgets()



if __name__ == "__main__":
    app = App()
    app.mainloop()
