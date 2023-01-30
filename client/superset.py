import requests
from bs4 import BeautifulSoup as bs
from decouple import config


class Client:

    BASE_URL = config("SUPERSET_BASE_URL", cast=str)
    USERNAME = config("SUPERSET_USERNAME", cast=str)
    PASSWORD = config("SUPERSET_PASSWORD", cast=str)

    def __init__(self):
        self.s = requests.Session()
        self.set_header()
        self.login()

    def get(self, url_path):
        return self.s.get(self.url(url_path), headers=self.headers)

    def post(self, url_path, data=None, json_data=None, **kwargs):
        kwargs.update({"url": self.url(url_path), "headers": self.headers})
        if data:
            data["csrf_token"] = self._csrf
            kwargs["data"] = data
        if json_data:
            kwargs["json"] = json_data
        return self.s.post(**kwargs)

    def login(self):
        payload = {
            "username": self.USERNAME,
            "password": self.PASSWORD,
            "csrf_token": self._csrf,
        }
        result = self.s.post(self.url("login/"), data=payload)
        if "Invalid login" in result.text:
            raise ValueError("Provided credentials not valid")
        else:
            print(self.USERNAME, "Login Successful")

        return

    def url(self, url_path):
        return self.BASE_URL + url_path

    def set_header(self):
        self.set_csrf()
        self.headers = {
            "X-CSRFToken": self._csrf,
            "Referer": self.url("login/"),
        }
        return

    def set_csrf(self):
        response = self.s.get(self.BASE_URL)
        soup = bs(response.content, "html.parser")
        for tag in soup.find_all("input", id="csrf_token"):
            self._csrf = tag["value"]
        return
