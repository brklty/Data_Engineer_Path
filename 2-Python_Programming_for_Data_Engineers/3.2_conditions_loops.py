###############################################
# CONDITIONS
###############################################

# True-False
1 == 1
1 == 2

# - if
if 1 == 1:
    print("something")

if 1 == 2:
    print("something")

number = 11

if number == 10:
    print("number is 10")

number = 10
number = 20


def number_check(number):
    if number == 10:
        print("number is 10")


number_check(12)


# - else


def number_check(number):
    if number == 10:
        print("number is 10")


number_check(12)


def number_check(number):
    if number == 10:
        print("number is 10")
    else:
        print("number is not 10")


number_check(12)


# - elif


def number_check(number):
    if number > 10:
        print("greater than 10")
    elif number < 10:
        print("less than 10")
    else:
        print("equal to 10")


number_check(6)

# LOOPS

# - for loop

students = ["John", "Mark", "Venessa", "Mariam"]

students[0]
students[1]
students[2]

for student in students:
    print(student)

for student in students:
    print(student.upper())

salaries = [1000, 2000, 3000, 4000, 5000]

for salary in salaries:
    print(salary)

for salary in salaries:
    print(int(salary * 20 / 100 + salary))

for salary in salaries:
    print(int(salary * 30 / 100 + salary))

for salary in salaries:
    print(int(salary * 50 / 100 + salary))


def new_salary(salary, rate):
    return int(salary * rate / 100 + salary)


new_salary(1500, 10)
new_salary(2000, 20)

# !!!!
for salary in salaries:
    print(new_salary(salary, 20))

salaries2 = [10700, 25000, 30400, 40300, 50200]

for salary in salaries2:
    print(new_salary(salary, 15))

for salary in salaries:
    if salary >= 3000:
        print(new_salary(salary, 10))
    else:
        print(new_salary(salary, 20))

# Example - Interview Question

# Objective: We want to write a function that changes the string as follows.

# before: "hi my name is john and i am learning python"
# after: "Hi mY NaMe iS JoHn aNd i aM LeArNiNg pYtHoN"


range(len("George"))
range(0, 5)

for i in range(len("George")):
    print(i)


# 4 % 2 == 0
# m = "George"
# m[0]

def alternating(string):
    new_string = ""

    for string_index in range(len(string)):
        # If the index is double, change it to upper case.
        if string_index % 2 == 0:
            new_string += string[string_index].upper()
        # If the index is odd, change it to lower case.
        else:
            new_string += string[string_index].lower()
    print(new_string)


alternating("george")


# break & continue & while


salaries = [1000, 2000, 3000, 4000, 5000]

for salary in salaries:
    if salary == 3000:
        break
    print(salary)

for salary in salaries:
    if salary == 3000:
        continue
    print(salary)

# while

number = 1
while number < 5:
    print(number)
    number += 1


# Enumerate: Automatic Counter/Indexer with for loop


students = ["John", "Mark", "Venessa", "Mariam"]

for student in students:
    print(student)

for index, student in enumerate(students):
    print(index, student)

A = []
B = []

for index, student in enumerate(students):
    if index % 2 == 0:
        A.append(student)
    else:
        B.append(student)


# Example - Interview Question


# Write the divide_students function.
# Put the students in even index into one list.
# Put the students with odd index into another list.
# But return these two lists as a single list.

students = ["John", "Mark", "Venessa", "Mariam"]


def divide_students(students):
    groups = [[], []]
    for index, student in enumerate(students):
        if index % 2 == 0:
            groups[0].append(student)
        else:
            groups[1].append(student)
    print(groups)
    return groups


st = divide_students(students)
st[0]
st[1]


# writing alternating function with enumerate


def alternating_with_enumerate(string):
    new_string = ""
    for i, letter in enumerate(string):
        if i % 2 == 0:
            new_string += letter.upper()
        else:
            new_string += letter.lower()
    print(new_string)


alternating_with_enumerate("hi my name is john and i am learning python")


# - Zip


students = ["John", "Mark", "Venessa", "Mariam"]

departments = ["mathematics", "statistics", "physics", "astronomy"]

ages = [23, 30, 26, 22]

list(zip(students, departments, ages))


# - lambda


def summer(a, b):
    return a + b


summer(1, 3) * 9

new_sum = lambda a, b: a + b

new_sum(4, 5)

# - map
salaries = [1000, 2000, 3000, 4000, 5000]


def new_salary(x):
    return x * 20 / 100 + x


new_salary(5000)

for salary in salaries:
    print(new_salary(salary))

list(map(new_salary, salaries))

# -- del new_sum
list(map(lambda x: x * 20 / 100 + x, salaries))
list(map(lambda x: x ** 2, salaries))

# - FILTER
list_store = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
list(filter(lambda x: x % 2 == 0, list_store))

# - REDUCE
from functools import reduce

list_store = [1, 2, 3, 4]
reduce(lambda a, b: a + b, list_store)

