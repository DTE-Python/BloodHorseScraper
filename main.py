import requests as Requests
from bs4 import BeautifulSoup
import time, random, threading, sqlite3, math
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
    
    # Previously there was a list of headers that we would select from.
    # This variable is, as of now, unneeded, but if it's needed we have it.
    select_number = failures

    # With most random user agents we appear like a normal user
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

    return headers

headers = Header_Select(0)


# Step 1: Store the links for each horse's Bloodhorse page from their search index.
# Step 1.5: We can save these links to optionally skip step 1 in the future, saving time.
# Step 2: From each Bloodhorse page, we get the link to Equineline (mostly for the horse ID)
# Step 3: We get the Equineline document and save its data to the SQLite database

def Bloodhorse_Get(starting_page_num, thread_num, out_q, accessKeyID, accessKeySecret, page_range):
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
        while pagenum <= starting_page_num + page_range and not all_links_added and bloodhorse_failures <= 5:
            
            print(f"THREAD {thread_num} Connecting to Bloodhorse; attempt #"+str(bloodhorse_failures +1) + "\nshould be page #"+str(pagenum+1))

            time.sleep(8)

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
            sqlInsertRowString = f""" INSERT OR IGNORE INTO HORSES (BH_link) VALUES({'"'+str(link_list[i])+'"'}); """
            out_q.put(sqlInsertRowString)

        print("sentinel " + str(thread_num))

    out_q.put(sentinel)

def Bloodhorse_Find_Equineline(starting_index, thread_num, out_q, accessKeyID, accessKeySecret, index_range, bhorse_link_list):
    global headers
    global set_horse_links

    bloodhorse_failures = 0

    gateway = ApiGateway("https://www.bloodhorse.com", access_key_id=accessKeyID, access_key_secret=accessKeySecret)
    gateway.start()


    session = Requests.Session()
    session.mount("https://www.bloodhorse.com",gateway)
    
    counter_index=starting_index
    
    if set_horse_links:
        while counter_index <= starting_index + index_range and bloodhorse_failures <= 5:
            link = bhorse_link_list[counter_index]
            print("Fetching from "+link+" on thread #"+thread_num)

            time.sleep(8)

            bloodhorse_response = session.post(link, headers=headers)

            bloodhorse_text = bloodhorse_response.text

            bloodhorse_data = BeautifulSoup(bloodhorse_text, "html.parser")
                
            equineline_link_element = str(bloodhorse_data.find("a", attrs={"class":"equineline"}, recursive=True))

            try:
                equineline_link = equineline_link_element.split('href="')[1].split('" class')[0].replace("amp;", "").replace("bh.cfm?", "bh_main.cfm?")
                counter_index +=1
            except AttributeError:
                print(f"Error getting equineline link (Thread #{thread_num}); trying with next headers")
                bloodhorse_failures += 1
                headers = Header_Select(bloodhorse_failures)
            except Exception as exception:
                if str(exception) is not str(AttributeError):
                    print("----------- THREAD #"+thread_num+" Error: "+str(exception))
                    equineline_link = exception
                    counter_index +=1

            sqlUpdateString = f""" UPDATE HORSES SET EQ_link = {'"'+str(equineline_link)+'"'} WHERE BH_link = {'"'+str(link)+'"'}; """
            out_q.put(sqlUpdateString)

    out_q.put(sentinel)

def Equineline_Get(starting_index, thread_num, out_q, accessKeyID, accessKeySecret, index_range, eq_link_list):
    global headers

    name_index      =   [0, -1, -1]
    crops_index     =   [3, 0]
    foals_index     =   [4, 0]
    RA_foals_index  =   [22, 4]
    winners_index   =   [24, 4, 0]
    b_winners_index =   [26, 3, 0]
    starters_index  =   [23, 3, 0]
    wins_index      =   [30, 2, 0]
    starts_index    =   [29, 1]
    earnings_index  =   [33, 1, 1]
    weanlings_index =   [60, 1]
    Wean_sales_index=   [60, 2, 1]
    yearlings_index =   [61, 1]
    Year_sales_index=   [61, 2, 1]


    equineline_failures = 0

    gateway = ApiGateway("http://www.equineline.com", access_key_id=accessKeyID, access_key_secret=accessKeySecret)
    gateway.start()


    session = Requests.Session()
    session.mount("http://www.equineline.com",gateway)
    
    counter_index=0
    while counter_index <= starting_index + index_range and equineline_failures <= 5:
        link = eq_link_list[counter_index]
        time.sleep(8)

        equineline_response = session.get(link, headers=headers)

        equineline_text = equineline_response.text
        equineline_data = BeautifulSoup(equineline_text, "html.parser")

        data = equineline_data.find_all("pre", recursive=True)

        data = data[0] + data[1]

        horse_data = data.splitlines()

        # Until Equineline changes their data format, we know where each piece is stored (it's a <pre>)
        name          = horse_data[name_index[0]].split(" ")[name_index[1]][:name_index[2]]
        crops         = [data for data in horse_data[crops_index[0]].split(" ") if data != ''][crops_index[1]]
        foals         = [data for data in horse_data[foals_index[0]].split(" ") if data != ''][foals_index[1]]
        RA_foals      = [data for data in horse_data[RA_foals_index[0]].split(" ") if data != ''][RA_foals_index[1]]
        winners       = [data for data in horse_data[winners_index[0]].split(" ") if data != ''][winners_index[1]].split("(")[winners_index[2]]
        b_winners     = [data for data in horse_data[b_winners_index[0]].split(" ") if data != ''][b_winners_index[1]].split("(")[b_winners_index[2]]
        starters      = [data for data in horse_data[starters_index[0]].split(" ") if data != ''][starters_index[1]].split("(")[starters_index[2]]
        wins          = [data for data in horse_data[wins_index[0]].split(" ") if data != ''][wins_index[1]].split("(")[wins_index[2]]
        starts        = [data for data in horse_data[starts_index[0]].split(" ") if data != ''][starts_index[1]]
        earnings      = [data for data in horse_data[earnings_index[0]].split(" ") if data !=''][earnings_index[1]][earnings_index[2]:]
        num_weanlings = [data for data in horse_data[weanlings_index[0]].split(" ") if data !=''][weanlings_index[1]]
        sale_weanling = [data for data in horse_data[Wean_sales_index[0]].split(" ") if data !=''][Wean_sales_index[1]][Wean_sales_index[2]:]
        num_yearlings = [data for data in horse_data[yearlings_index[0]].split(" ") if data !=''][yearlings_index[1]]
        sale_yearling = [data for data in horse_data[Year_sales_index[0]].split(" ") if data !=''][Year_sales_index[1]][Year_sales_index[2]:]

        sqlUpdateString = f""" UPDATE HORSES SET 
                            Name = {'"'+name+'"'}, 
                            Crops = {'"'+crops+'"'},
                            Foals = {'"'+foals+'"'},
                            RA_Foals = {'"'+RA_foals+'"'},
                            Winners = {'"'+winners+'"'},
                            Winners_B = {'"'+b_winners+'"'},
                            Wins = {'"'+wins+'"'},
                            Starters = {'"'+starters+'"'},
                            Starts = {'"'+starts+'"'},
                            Earnings = {'"'+earnings+'"'},
                            Num_Weanlings = {'"'+num_weanlings+'"'},
                            Sales_Weanlings = {'"'+sale_weanling+'"'},
                            Num_Yearlings = {'"'+num_yearlings+'"'},
                            Sales_Yearlings = {'"'+sale_yearling+'"'},
                            WHERE EQ_link = {'"'+str(link)+'"'}; """
        out_q.put(sqlUpdateString)

    out_q.put(sentinel)


        





        

def Process_SQL_Commands(in_q, thread_count):

    sqlconnection = sqlite3.connect("horses.db", timeout=8)
    sqlcursor = sqlconnection.cursor()

    sentinel_count = 0

    while True:
        command = in_q.get()

        if command == "////":
            sentinel_count +=1
        else:
            sqlcursor.execute(command)

        in_q.task_done()
        
        if sentinel_count == thread_count:
            print("Queue emptied.")
            sqlcursor.close()
            break

    sqlconnection.commit()


# This can be any unique identifier as long as Process_SQL_Commands() increases sentinel_count when received
sentinel = "////"

created_thread_list = []
def Start_Threads(threadcount, taskcount, accessID, accessKey, target_function, param=None):
    q = Queue()
    queue_processor = threading.Thread(target=Process_SQL_Commands, args=(q, threadcount, ))
    queue_processor.start()


    # Divides the number of pages as equally as possible among the chosen number of threads.
    tasks_per_thread = math.floor(taskcount / threadcount)

    # Shows how many pages are left over.
    remainder_tasks = taskcount % threadcount

    # If a thread is assigned additional pages, this variable updates the starting point for the other threads
    carry = 0
    # These two variables are (hopefully) for readability
    count_from_first = 1
    include_endpoint = 1

    for threadnum in range(threadcount):
        start= (threadnum*tasks_per_thread) + carry + (count_from_first if target_function == Bloodhorse_Get else 0)

        # Assign the remainder to a thread (and decrease the remaining remainder)
        if threadnum > threadcount - remainder_tasks-1 and remainder_tasks > 0:
            carry += 1
            remainder_tasks -= 1
        
        end = (threadnum+1) * tasks_per_thread + carry

        # Find the number of pages each thread takes care of
        p_range = end + include_endpoint - start

        # Create thread
        thread = threading.Thread(target=target_function, args=(start, threadnum, q, accessID, accessKey, p_range, param))
        
        # Add to list of threads (in case needed later)
        created_thread_list.append(thread)

    for thread in created_thread_list:
        thread.start()

    for thread in created_thread_list:
        thread.join()

    q.join()

    queue_processor.join()
    
    print(str(threadcount) + " Threads finished.")

    



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
        self.btn_start = Button(self, text = 'Start',
            fg= 'red', command=self.start_clicked)
        self.btn_start.grid(column=5,row=6)
    
        # Thread Count Selection Menu
        self.thcountMenu = Menubutton(self, text="-> Select Thread Count")

        self.menu = Menu(self.thcountMenu, tearoff=0)
        for i in range(1, 17):
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
        self.TaskNumDisplay = Label(text='Detect or enter pages.\n(If setting links)')
        self.TaskNumDisplay.grid(row=0, column=5)

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

        self.btn_start.grid(column=5,row=6)
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

        self.TaskNumDisplay.configure(text='Detect or enter pages.')

        self.warning.grid(row=5, column=5)

        self.reset_button.grid(row=6, column=0)

        self.aws_id_label.configure(text = '')
        self.aws_secret_label.configure(text = '')
        self.unset_values.configure(text='')

    # Run Program
    def start_clicked(self):
        global set_horse_links
        self.lbl.configure(text='Running')
        self.btn_start.grid_forget()
        self.linkupdate.grid_forget()
        
        self.page_entry_button.grid_forget()
        self.pageCount.grid_forget()
        self.page_entry.grid_forget()
        self.thcountMenu.grid_forget()

        
        self.warning.grid_forget()
        set_horse_links = self.linkupdatevar.get()

        self.setAWSID()
        self.setAWSSECRET()
        self.update()

        if self.AWS_ID == '' or self.AWS_SECRET == '' or (set_horse_links and self.pages == 0):
            self.unset_values.configure(text='Missing required values (Check page count and AWS)\nClick Reset to continue')
            self.unset_values.grid(row=8, column = 1)
        else:
            self.reset_button.grid_forget()

            print("Setting links: " + str(set_horse_links))
            print("AWS ID: " + self.AWS_ID, "AWS Secret: " + self.AWS_SECRET)
            print("Starting " + str(self.thcount.get()) + " Thread(s)")
            print("If setting links, checking " + str(self.pages) + " pages.")

            if set_horse_links:
                with sqlite3.connect("horses.db") as sqlconnection:
                    sqlcursor = sqlconnection.cursor()

                    sqlcursor.execute(""" DROP TABLE IF EXISTS HORSES; """)

                    table = """ CREATE TABLE HORSES (
                                BH_link TEXT UNIQUE NOT NULL,
                                EQ_link TEXT,
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

            Start_Threads(self.thcount.get(), self.pages, self.AWS_ID, self.AWS_SECRET, Bloodhorse_Get)

            self.lbl.configure(text='Bloodhorse Links Saved.')

            

            with sqlite3.connect("horses.db") as sqlconnection:
                sqlcursor = sqlconnection.cursor()

                count = sqlcursor.execute(" SELECT COUNT (BH_LINK) FROM HORSES; ")
                countINT = count.fetchone()[0]
                print(str(countINT) + " Links found." )
                self.TaskNumDisplay.configure(text="Getting Equineline data for:\n"+ str(countINT) +" horses.")

                bh_links = [bh_links[0] for bh_links in sqlcursor.execute(" SELECT BH_LINK FROM HORSES; ")]

                sqlconnection.commit()
                
            self.update()

            Start_Threads(self.thcount.get(), countINT, self.AWS_ID, self.AWS_SECRET, Bloodhorse_Find_Equineline, bh_links)







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
            self.TaskNumDisplay.configure(text='Pages: '+ str(self.pages))
        except IndexError:
            self.TaskNumDisplay.configure(text='Error getting page count.\nTry manually setting a number\nor trying again.')
    
    # Update Page Number Manually
    def enterPages(self):
        try:
            self.pages = int(self.page_entry.get())
            self.TaskNumDisplay.configure(text='Pages: '+ str(self.pages))
        except ValueError:
            self.TaskNumDisplay.configure(text='Error NaN')
    
    # Enter AWS information
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

    # Reset layout 
    def ResetClicked(self):
        self.aws_secret_label.grid_forget()
        self.aws_id_label.grid_forget()
        self.unset_values.grid_forget()
        
        
        self.reset_widgets()


# Run
if __name__ == "__main__":
    app = App()
    app.mainloop()
