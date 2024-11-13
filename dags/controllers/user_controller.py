from models.user_model import UserModel

class UserController:
    def __init__(self):
        self.model = UserModel()

    def insert_user(self,data):
        self.model.insert_user(data=data)
    