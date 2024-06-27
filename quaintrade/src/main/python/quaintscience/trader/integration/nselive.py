import yaml
import os
from jugaad_data.nse import NSELive
import datetime

class NSELiveHandler:

    def __init__(self, cache=".nsecache"):
        self.nse_live = None
        self.cache = cache
        self.data = {}
        self.__load_cache()
    
    def __load_cache(self):
        self.clear_cache()
        if os.path.exists(self.cache):
            with open(self.cache, 'r', encoding='utf-8') as fid:
                self.data = yaml.safe_load(fid)
    
    def get_nse_live(self):
        if self.nse_live is None:
            self.nse_live = NSELive()
        return self.nse_live

    def index_option_chain(self, index):
        if index not in self.data["index_option_chain"]:
            res = self.get_nse_live().index_option_chain(index)
            self.data["index_option_chain"][index] = res
            self.__save_cache()
        return self.data["index_option_chain"][index]

    def all_indices(self):
        if self.data["all_indices"] is None or len(self.data["all_indices"]) == 0:
            self.data["all_indices"] = self.get_nse_live().all_indices()
            self.__save_cache()
        return self.data["all_indices"]

    def __save_cache(self):
        with open(self.cache, 'w', encoding='utf-8') as fid:
            yaml.dump(self.data, fid)

    def clear_cache(self):
        self.data = {"index_option_chain": {}, "all_indices": None}
    
    def get_expiries(self, index: str):
        nifty_options_chain = self.index_option_chain(index)
        expiry_dates = list(map(lambda x: datetime.datetime.strptime(x, "%d-%b-%Y"), nifty_options_chain["records"]["expiryDates"]))
        expiry_dates = sorted(expiry_dates, key=lambda x: x - datetime.datetime.now())
        expiries = []
        expiries.extend(map(lambda x: x.strftime("%d-%b-%Y"), expiry_dates[:2]))
        return expiries

    def get_strikes(self, index: str, expiry: str):
        all_indices = self.all_indices()
        mapper = {"NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY BANK"}
        index_info = [item for item in all_indices["data"] if item["index"] == mapper[index]]
        if len(index_info) == 0:
            raise KeyError(f"{index} not found")
        index_info = index_info[0]
        ltp = index_info["last"]
        expiry = datetime.datetime.strptime(expiry, "%d-%b-%Y")
        print(expiry, "NSE LIVE")
        opts = []
        atm = round(ltp/100) * 100

        options = []
        if index == "NIFTY":
            options.extend(zip([100, 50], ["ITM1", "ITM2"]))
            options.extend(zip([0, -50, -100], ["ATM", "OTM1", "OTM2"]))
        elif index == "BANKNIFTY":
            options.extend(zip([200, 100], ["ITM1", "ITM2"]))
            options.extend(zip([0, -100, -200], ["ATM", "OTM1", "OTM2"]))
        
        for ty in ["CE", "PE"]:
            for price, loc in options:
                opts.append(f"{ty} > {index} > {loc} > {atm + price} > {expiry.strftime('%Y%m%d')}")
        print(opts)
        return opts
