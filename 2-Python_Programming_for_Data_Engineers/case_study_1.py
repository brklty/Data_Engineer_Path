# Task 1: Analyse the data structures of the given values

x = 8
y = 3.2
z = 8j + 18
a = "Hello World"
b = True
c = 23 < 22
l = [1, 2, 3, 4]
d = {"Name": "Jake", "Age": 27, "Address": "Downtown"}
t = ("Machine Learning", "Data Science")
s = {"python", "Machine Learning", "Data Science"}

# Solution 1

var_list = [x, y, z, a, b, c, l, d, t, s]

for i in var_list:
    print(type(i))

# Task 2: Capitalise all letters of the given string expression. Replace comma and period with space,
# word by word.


text = "The goal is to turn data Into information, and information into insight."

# Solution 2

text.upper().replace(",", " ").replace(".", " ").split()

# Task 3: Apply the following steps to the given list.


lst = ['D', 'A', 'T', 'A', 'S', 'C', 'I', 'E', 'N', 'C', 'E']

# Step 1: Check the number of elements of the given list.

print(len(lst))

# Step 2: Call the elements at index zero and ten.

print(lst[0], lst[10])

# Step 3: Create a list ["D", "A", "T", "A"] from the given list.

n_lst = lst[0:4]
print(n_lst)

# Step 4: Delete the element at the eighth index.
lst.pop(8)
print(lst)

# Step 5: Add a new element.
lst.append("1")
print(lst)

# Step 6: Add the element "N" to the eighth index again.
lst.insert(8, "N")
print(lst)

# Task 4: Apply the following steps to the given dictionary.

dictionary = {"Christian": ["America", 18],
              "Daisy": ["England", 12],
              "Antonio": ["Spain", 22],
              "Dante": ["Italy", 25]}

# Step 1: Access the key values.

dictionary.keys()

# Step 2: Access the values.

dictionary.values()

# Step 3: Update the value 12 of the Daisy key to 13.
dictionary["Daisy"][1] = 13
dictionary["Daisy"]

# Step 4: Add a new value to the key value Ahmet with the value [Turkey,24].
dictionary["Ahmet"] = ["Turkey", 24]
dictionary

# Step 5: Delete Antonio from the dictionary.
dictionary.pop("Antonio")
dictionary


# Task 5: Write a function that takes a list as argument,
# assigns the odd and even numbers in the list to separate lists and returns these lists.


def list_splitter(lst=list):
    odd_list = []
    even_list = []

    for number in lst:

        if number % 2 == 0:
            even_list.append(number)
        else:
            odd_list.append(number)

    return odd_list, even_list


l = [2, 12, 18, 93, 22]

even_list, odd_list = list_splitter(l)
print(even_list, odd_list)

# Task 6: The following list contains the names of the students ranked in the
# engineering and medical faculties. The first three students represent the
# ranking of the engineering faculty and the last three students represent the
# ranking of the medical faculty. Use Enumarate to enter student ranks by faculty.


students = ["John", "Anna", "Mark", "Kareem", "Julia", "Emily"]

for index, student in enumerate(students):
    if index < 3:
        print(f"Mühendislik Fakültesi {index + 1} . student: {student}")
    else:
        print(f"Tip Fakültesi {index + 1} . student: {student}")

## Task 7: 3 lists are given below. The lists contain the code, credit and quota information
## of a course respectively. Print the course information using Zip.

course_code = ["CMP1005", "PSY1001", "HUK1005", "SEN2204"]
credit = [3, 4, 2, 4]
quota = [30, 75, 150, 25]

for course_code, credit, quota in (zip(course_code, credit, quota)):
    print(f"The quota for the course coded {course_code} with {credit} credits is {quota} students.")


