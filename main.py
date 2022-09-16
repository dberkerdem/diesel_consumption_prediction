from src.db_ops.db_ops import PosgreOps
from src.utils.config import load_config

def main():
    config = load_config("config.yaml")
    print("config success")
    pgops = PosgreOps(config=config)
    print("success with connector")
    data = pgop.get_data()
    print(data)
if __name__ == '__main__':
    print("Name is: ",__name__)
    main()
    print("Success running main")
else:
    print("Called by another script")
    print("Name is : ",__name__)        