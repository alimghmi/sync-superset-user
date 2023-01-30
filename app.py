import pandas as pd
from bs4 import BeautifulSoup as bs
from decouple import config

from client import superset
from database import mssql


class App:

    ON_DELETE = config("CLIENT_USER_ON_DELETE", default="delete", cast=str)
    IGNROE_USERS = config(
        "CLIENT_IGNORE_USERS",
        default="admin",
        cast=lambda v: [
            x.lower().strip() for x in v.split(",") if x.lower().strip() != ""
        ],
    )

    def __init__(self):
        self.db = mssql.MSSQLDatabase()
        self.api = superset.Client()
        self.db_users = self.get_db_user()
        self.roles = self.get_superset_role()
        self.users = self.get_superset_user()
        self.generate_user_list()

    def run(self):
        self.sync_add_user()
        self.sync_activate_user()
        if self.ON_DELETE == "delete":
            self.sync_delete_user()
        else:
            self.sync_deactivate_user()

    def sync_activate_user(self):
        for user in self.db_user_list:
            if user in self.IGNROE_USERS:
                continue
            if user not in self.user_list:
                continue
            user_data = self.get_user(self.users, user)
            if user_data["is_active"]:
                continue

            payload = {
                "first_name": user_data["first_name"],
                "last_name": user_data["last_name"],
                "username": user_data["username"],
                "email": user_data["email"],
                "roles": user_data["role_id"],
            }

            self.activate_superset_user(payload, user_data["user_id"])
            print(user, "Activated")

        return

    def sync_deactivate_user(self):
        for user in self.user_list:
            if user in self.IGNROE_USERS:
                continue
            if user in self.db_user_list:
                continue
            user_data = self.get_user(self.users, user)
            if not user_data["is_active"]:
                continue

            payload = {
                "first_name": user_data["first_name"],
                "last_name": user_data["last_name"],
                "username": user_data["username"],
                "email": user_data["email"],
                "roles": user_data["role_id"],
            }

            self.deactivate_superset_user(payload, user_data["user_id"])
            print(user, "Deactivated")

    def sync_delete_user(self):
        for user in self.user_list:
            if user in self.IGNROE_USERS:
                continue

            if user not in self.db_user_list:
                user_data = self.get_user(self.users, user)
                self.delete_superset_user({"delete": ""}, user_data["user_id"])
                print(user, "Deleted")

    def sync_add_user(self):
        for user in self.db_user_list:
            if user in self.user_list:
                continue

            user_data = self.get_user(self.db_users, user)
            payload = {
                "first_name": user_data["firstname"],
                "last_name": user_data["lastname"],
                "username": user_data["username"].lower(),
                "email": user_data["email"].lower(),
                "active": "y",
                "conf_password": user_data["password"].lower(),
                "password": user_data["password"].lower(),
                "roles": self.roles[user_data["role"].lower()],
            }

            self.add_superset_user(payload)
            print(user_data["username"], "Created")

    def add_superset_user(self, payload):
        return self.api.post(url_path="users/add", json=payload)

    def delete_superset_user(self, payload, userid):
        return self.api.post(url_path=f"users/delete/{userid}", json=payload)

    def deactivate_superset_user(self, payload, userid):
        if "active" in payload:
            del payload["active"]

        return self.api.post(url_path=f"users/edit/{userid}", json=payload)

    def activate_superset_user(self, payload, userid):
        payload["active"] = "y"
        return self.api.post(url_path=f"users/edit/{userid}", json=payload)

    def get_db_user(self):
        def parse_role(item):
            item["username"] = item["username"].lower().strip()
            item["email"] = item["email"].lower().strip()
            return item

        result = self.db.select_table("[clients].[v_user]").to_dict("records")
        return list(map(parse_role, result))

    def get_superset_user(self):
        def parse_role(item):
            item["username"] = item["username"].lower().strip()
            item["role"] = (
                item["role"].replace("[", "").replace("]", "").split(",")
            )
            item["role_id"] = list(
                map(
                    lambda role: self.roles.get(role.lower(), None),
                    item["role"],
                )
            )
            return item

        content = self.api.get(
            url_path="users/list?psize_UserDBModelView=1000"
        )
        columns = [
            "first_name",
            "last_name",
            "username",
            "email",
            "is_active",
            "role",
        ]
        result = self.parse_html_table(content.text, 1, columns, "user_id")
        return list(map(parse_role, result))

    def get_superset_role(self):
        content = self.api.get(url_path="roles/list")
        columns = ["name"]
        result = self.parse_html_table(content.text, 2, columns, "role_id")
        return {item["name"].lower(): item["role_id"] for item in result}

    def generate_user_list(self):
        self.user_list = [i["username"].lower().strip() for i in self.users]
        self.db_user_list = [
            i["username"].lower().strip() for i in self.db_users
        ]
        return

    @staticmethod
    def get_user(collection, username):
        for item in collection:
            if item["username"].lower() == username.lower():
                return item

    @staticmethod
    def parse_html_table(content, skip_column, columns, id_column):
        soup = bs(content, "html.parser")
        table = soup.find_all("table", class_="table table-hover")[0]
        df = pd.read_html(str(table))[0].iloc[:, skip_column:]
        df.columns = columns
        df[id_column] = [
            a["href"].split("/")[-1]
            for a in table.find_all("a", {"class": "btn btn-sm btn-default"})
            if "edit" not in a["href"]
        ]

        return df.to_dict("records")
