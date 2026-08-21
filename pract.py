def make_data():
    return [10,20,30]

print(make_data())

def raw_orders():
    data = [100, 200, 300]
    return data

print(raw_orders())

def user_names():
    names = ["Alice", "Bob"]
    return names

print(user_names())

def greeting_message(user_names):
    greetings = [f"Hello, {name}!" for name in user_names]
    return greetings

print(greeting_message(user_names()))

def name_count(user_names):
    count = len(user_names)
    return count

print(name_count(user_names()))