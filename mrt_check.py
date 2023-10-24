from datetime import datetime


def count_work_day():
    today = datetime.today()


try:

    holiday = int(input(f"How many hloiday in this month? : "))
    ticket_price = {
        '15': {'price': 450, 'per_round': '30'},
        '25': {'price': 700, 'per_round': '28'},
    }

    for i in ticket_price:
        print(f"{i}:{ticket_price[i]}")

except ValueError as err:
    print(f"wrong input type : {err}")
