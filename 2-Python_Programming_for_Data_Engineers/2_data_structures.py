###############################################

# DATA STRUCTURES

################################################

# - Introduction to Data Structures and Quick Summary

# numbers: integer
x = 46
type(x)

# numbers: float
x = 10.3
type(x)

# numbers: complex
x = 2j + 1
type(x)

# String
x = "Hella ai "
type(x)

# Boolean
True
False

type(True)

5 == 4
3 == 2
1 == 1

type(3 == 2)

# List

x = ["asd", "ert", "hbf"]
type(x)

# Dictionary
x = {"name": "George", "age": 35}
type(x)

# Tuple
x = ("python", "ml", "ds", "de")
type(x)

# Set
x = {"python", "ml", "ds", "de"}
type(x)



# - Numbers: int, float, complex

a = 5
b = 10.05

a * 3
a / 7
a * b / 10
a ** 2


# -- float to integer / integer to float

int(b)
float(a)
int(a * b / 10)

c = a * b / 10
int(c)


# - Strings: str

print("George")
print('George')

"George"

name = "George"

# -- Multiline Character Strings

"""Multiline 
Character 
Strings """

long_string = """Multiline 
Character 
Strings """

# -- Accessing Elements of Character Arrays
name
name[0]
name[3]

# -- Slice in Character String

name[0:2]
long_string[0:10]

# -- Querying Character in String
long_string

"line" in long_string

"Line" in long_string

# -- String Methods
dir(str)

# --- len:

name = "George"
type(name)
type(len)

len(name)

len("alberteinstein")


# --- upper() & lower():

"George".upper()
"George".lower()

# --- replace():

hi = "Hello Ai"

hi.replace("e", "a")

# --- split():

hi.split()

# --- strip():

" Hallo Ai ".strip()
"ofofof".strip("o")

# --- capitalize():

"foo".capitalize()

# ---startswith():

"foo".startswith("f")
"foo".startswith("o")


# - List

my_list = [1, 2, 3, 4]
type(my_list)

my_str_list = ["a", "c", "d"]

str_int_list = [1, 2, 3, 4, "a", "c", "d", True, [1, 3, 5]]

str_int_list[0]

str_int_list[5]

str_int_list[8][0]

type(str_int_list[0])

type(str_int_list[8][0])

my_list[3] = 99

my_list


str_int_list[0:6]

# -- List Methods

dir(my_list)

# --- len:

len(my_list)
len(str_int_list)

# --- append:

my_list.append(111)
my_list

# --- pop:

my_list.pop(4)
my_list

# --- insert:

my_list.insert(1, "#1")
my_list


# - Dictionary

    # key-value

dictionary = {"ML": "Machine Learning",
              "DS": "Data Science",
              "DE": "Data Engineering"}

dictionary["ML"]

dictionary = {"ML": ["Machine Learning", 10],
              "DS": 20,
              "DE": "Data Engineering"}


dictionary["ML"][1]


"DS" in dictionary

"AS" in dictionary


dictionary.get("DE")

dictionary["ML"] = ["ML ML", 0]

dictionary.keys()

dictionary.values()

dictionary.items()

dictionary.update({"ML": 11})

dictionary.update(({"DA": "Data Analysis"}))



# - Tuple

tpl = ("George", "marry", 1, 51.3)
type(tpl)

tpl[0]

tpl[0:3]

tpl[0] = 99

# - Set

set1 = set([1, 2, 3])
set2 = set([1, 2, 5])

type(set1)

# -- difference of two sets
set1.difference(set2) #in set1 and not in set2
set1 - set2

set2.difference(set1)
set2 - set1

# -- in either of the sets and not in their intersection
set1.symmetric_difference(set2)
set2.symmetric_difference(set1)

# -- intersection of two sets
set1.intersection(set2)
set2.intersection(set1)

# -- union of two sets
set1.union(set2)
set2.union(set1)

# -- Is the intersection of two sets empty?
set1.isdisjoint(set2)

# -- Is one set a subset of another set?
set1.issubset(set2)
set2.issubset(set1)

# -- Does one set include the other set?
set1.issuperset(set2)
set2.issuperset(set1)
